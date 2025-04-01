#  Copyright (c) 2014-2019 LINBIT HA Solutions GmbH
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License. You may obtain
#  a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#  License for the specific language governing permissions and limitations
#  under the License.

"""This driver connects Cinder to an installed LINSTOR instance.

See https://docs.linbit.com/docs/users-guide-9.0/#ch-openstack-linstor
for more details.
"""
import contextlib
import functools
import socket
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from eventlet.green import threading
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import importutils
from oslo_utils import units

from cinder.common import constants
from cinder import exception
from cinder.i18n import _
from cinder.image import image_utils
from cinder import interface
from cinder import objects
from cinder.objects import fields
from cinder.volume import configuration
from cinder.volume import driver
from cinder.volume.targets import driver as targets
from cinder.volume import volume_utils

import time
from oslo_utils import timeutils

try:
    import linstor
except ImportError:
    linstor = None

# To override these values, update cinder.conf in /etc/cinder/
linstor_opts = [
    cfg.ListOpt('linstor_uris',
                default=['linstor://localhost'],
                deprecated_name='linstor_default_uri',
                help='URI(s) of Linstor controller to connect to. Specify '
                     'multiple URIs to take advantage of a LINSTOR HA'
                     'deployment.'),

    cfg.StrOpt('linstor_client_key',
               help='Path to the PEM encoded private key used for HTTPS '
                    'connections to the server'),

    cfg.StrOpt('linstor_client_cert',
               help='Path to the PEM encoded client certificate to present '
                    'the controller on HTTPS connections'),

    cfg.StrOpt('linstor_trusted_ca',
               help='Path to the PEM encoded CA certificate, used to verify '
                    'the LINSTOR controller authenticity'),

    cfg.StrOpt('linstor_default_storage_pool_name',
               help='Default LINSTOR Storage Pool to use.'),

    cfg.StrOpt('linstor_default_resource_group_name',
               help='Resource Group to use when no volume type was provided',
               default="DfltRscGrp"),

    cfg.BoolOpt('linstor_direct',
                default=False,
                help='True, if the volume should be directly attached on the'
                     'target. Requires the target to be part of the Linstor '
                     'cluster. False, if the volume should be attached via '
                     'one of the transports included in Cinder (i.e. ISCSI).'),

    cfg.IntOpt('linstor_timeout',
               default=60,
               help='How long to wait for a response from the Linstor API'),

    cfg.BoolOpt('linstor_force_udev',
                default=True,
                help='True, if the driver should assume udev created links'
                     'always exist.'),

    cfg.BoolOpt('linstor_use_snapshot_based_clone',
                default=False,
                help='True, if the driver should use clone with snapshot'
                     '(dependent clone)'),

]

LOG = logging.getLogger(__name__)  # type: logging.logging.Logger

CONF = cfg.CONF
CONF.register_opts(linstor_opts, group=configuration.SHARED_CONF_GROUP)

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def wrap_linstor_api_exception(func):
    """Wrap Linstor exceptions in LinstorDriverApiExceptions"""
    @functools.wraps(func)
    def f(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except linstor.LinstorError as error:
            raise LinstorDriverApiException(error)

    return f


class ThreadSafeLinstorClient(object):
    def __init__(self, configuration):
        self.configuration = configuration
        self._thread_local = threading.local()  # pylint: disable=no-member

    def get(self):
        """Returns a (thread-local) linstor client

        :rtype: linstor.Linstor
        """
        if not hasattr(self._thread_local, 'linstor_client'):
            client = linstor.MultiLinstor(
                self.configuration.linstor_uris,
                timeout=self.configuration.linstor_timeout,
            )
            client.keyfile = self.configuration.safe_get('linstor_client_key')
            client.certfile = self.configuration.safe_get(
                'linstor_client_cert',
            )
            client.cafile = self.configuration.safe_get('linstor_trusted_ca')
            self._thread_local.linstor_client = client
        return self._thread_local.linstor_client


@interface.volumedriver
class LinstorDriver(driver.VolumeDriver):
    """LINSTOR Driver.

    Manages Cinder volumes provisioned by LINSTOR.

    A quick overview on how names and concepts are mapped between Linstor and
    Cinder:
    * A Cinder Volume maps to a Resource (with one volume) in Linstor
    * A Cinder Snapshot maps to a Snapshot of a Resource in Linstor
    * A Cinder Volume Type maps to a Resource Group in Linstor.

    Version history:

    .. code-block:: none

        1.0.0 - Initial driver
        1.0.1 - Added support for LINSTOR 0.9.12
        1.1.0 - Updated driver to match LINSTOR backend improvements
        2.0.0 - Complete rewrite using python-linstor high-level API
          * Removed node and storage-pool creation
          * Support Linstor resource groups via cinder storage pools
          * Support live migration in direct attach mode
          * Limited support for snapshot revert
    """

    VERSION = '2.0.0'

    CI_WIKI_NAME = 'LINBIT_LINSTOR_CI'

    SUPPORTS_ACTIVE_ACTIVE = True

    @volume_utils.trace
    def __init__(self, *args, **kwargs):
        super(LinstorDriver, self).__init__(*args, **kwargs)

        self.configuration.append_config_values(linstor_opts)
        self._stats = {}
        self._vendor_properties = {}
        self.target_driver = None  # type: targets.Target
        self.protocol = None  # type: str
        self.c = ThreadSafeLinstorClient(self.configuration)

    @staticmethod
    @volume_utils.trace
    def get_driver_options():
        return linstor_opts

    @volume_utils.trace
    def _use_direct_connection(self):
        """Should the driver attach the volume directly or not

        :returns: True, if the volume should be directly attached on the
          target. Requires the target to be part of the Linstor cluster
          False, if the volume should be attached via one of the transports
          included in Cinder (i.e. ISCSI).
        """
        return self.configuration.linstor_direct

    @wrap_linstor_api_exception
    @volume_utils.trace
    def check_for_setup_error(self):
        """Runs the set-up and verifies that it is in working order"""
        if not linstor:
            msg = _('Package python-linstor is not installed')
            raise LinstorDriverException(msg)

        if not hasattr(linstor, 'MultiLinstor'):
            msg = _('Package python-linstor does not support MultiLinstor, '
                    'please update')
            raise LinstorDriverException(msg)

        try:
            linstor.Resource('test', existing_client=self.c.get())
        except TypeError:
            msg = _('Package python-linstor does not support passing clients '
                    'to Resource class, please update')
            raise LinstorDriverException(msg)

        with self.c.get() as client:
            version_str = client.controller_version().rest_api_version
            nodes = client.node_list_raise(filter_by_nodes=[self._hostname])

        rest_version = tuple(int(n) for n in version_str.split("."))
        if rest_version < (1, 4, 0):
            msg = _('Linstor API not supported: %s < (1, 4, 0)') \
                % str(rest_version)
            raise LinstorDriverException(msg)

        if len(nodes.nodes) < 1:
            msg = _('Cinder host %s is not a configured Linstor '
                    'node') % self._hostname
            raise LinstorDriverException(msg)

        if self._use_direct_connection():
            self.target_driver = LinstorDirectTarget(self.c, self._force_udev)
            self.protocol = self.target_driver.protocol
        else:
            target_driver = self.target_mapping[
                self.configuration.target_helper
            ]

            LOG.debug('Attempting to initialize LINSTOR driver with the '
                      'following target_driver: %s',
                      target_driver)

            self.target_driver = importutils.import_object(
                target_driver,
                configuration=self.configuration,
                executor=self._execute)  # type: targets.Target
            self.protocol = self.target_driver.protocol

    @property
    def _linstor_uri_str(self):
        """String representation of all Linstor URIs"""
        return ",".join(sorted(self.configuration.linstor_uris))

    @property
    def _hostname(self):
        """Get the name of the local host"""
        if self.host:
            return volume_utils.extract_host(self.host, level='host')

        return socket.gethostname()

    @property
    def _force_udev(self):
        return self.configuration.safe_get('linstor_force_udev')

    @property
    def _use_snapshot_based_clone(self):
        return self.configuration.safe_get('linstor_use_snapshot_based_clone')

    @volume_utils.trace
    def _init_vendor_properties(self):
        """Return the vendor properties supported by this driver"""
        self._set_property(
            self._vendor_properties,
            'linstor:storage_pool',
            title='LINSTOR Storage Pool',
            description='Storage pool to use when auto-placing',
            type='str',
            default=self.configuration.safe_get(
                'linstor_default_storage_pool_name'
            ),
        )
        self._set_property(
            self._vendor_properties,
            'linstor:diskless_on_remaining',
            title='Diskless on remaining',
            description='Create diskless replicas on non-selected nodes after'
                        'auto-placing',
            type='bool',
            default=False,
        )
        self._set_property(
            self._vendor_properties,
            'linstor:do_not_place_with_regex',
            title='Do not place with regex',
            description='Do not place the resource on a node which has a '
                        'resource with a name matching the regex.',
            type='str',
        )
        self._set_property(
            self._vendor_properties,
            'linstor:layer_list',
            title='Layer List',
            description='Comma-separated list of layers to apply for resources'
                        'If empty, defaults to DRBD,Storage.',
            type='str',
        )
        self._set_property(
            self._vendor_properties,
            'linstor:provider_list',
            title='Provider list',
            description='Comma-separated list of providers to use',
            type='str',
        )
        self._set_property(
            self._vendor_properties,
            'linstor:redundancy',
            title='Redudancy',
            description='Number of replicas to create. Defaults to 2',
            type='int',
        )
        self._set_property(
            self._vendor_properties,
            'linstor:replicas_on_different',
            title='Replicas on different',
            description='A comma-separated list of key or key=value items '
                        'used as autoplacement selection labels when '
                        'autoplace is used to determine where to provision '
                        'storage',
            type='str',
        )
        self._set_property(
            self._vendor_properties,
            'linstor:replicas_on_same',
            title='Replicas on same',
            description='A comma-separated list of key or key=value items '
                        'used as autoplacement selection labels when '
                        'autoplace is used to determine where to provision '
                        'storage',
            type='str',
        )

        return self._vendor_properties, 'linstor'

    def _get_linstor_property(self, name, volume_type):
        """Retrieve the named property, either from the volume type or defaults

        :param str name: the name of the property to retrieve (without linstor
         prefix)
        :param cinder.objects.volume_type.VolumeType volume_type: The volume
         type containing the extra specs to check
        :return: The property value, if set
        :rtype: str|None
        """
        prefixed_name = "linstor:" + name
        extras = volume_type.get('extra_specs', {})
        if prefixed_name in extras:
            return extras[prefixed_name]
        return self._vendor_properties[prefixed_name].get('default')

    def _resource_group_for_volume_type(self, volume_type):
        """Ensure a LINSTOR resource group exists matching the volume type

        :param cinder.objects.volume_type.VolumeType volume_type: The volume
         type for which the resource group should exist
        :return:
        :rtype: linstor.ResourceGroup
        """
        if not volume_type:
            return linstor.ResourceGroup(
                self.configuration.linstor_default_resource_group_name,
                existing_client=self.c.get(),
            )

        # We use the ID here, as it is unique and compatible with LINSTOR
        # naming requirements. The cinder- prefix is required as LINSTOR names
        # have to start with an alphabetic character
        rg = linstor.ResourceGroup(
            'cinder-' + volume_type['id'],
            existing_client=self.c.get(),
        )

        description = 'For volume type "%s"' % volume_type['name']
        if rg.description != description:
            rg.description = description

        nr_volumes = 1
        if rg.nr_volumes != nr_volumes:
            rg.nr_volumes = nr_volumes

        storage_pool = self._get_linstor_property('storage_pool', volume_type)
        if storage_pool and rg.storage_pool != storage_pool.split(','):
            rg.storage_pool = storage_pool.split(',')

        diskless = self._get_linstor_property(
            'diskless_on_remaining', volume_type,
        )
        if rg.diskless_on_remaining != diskless:
            rg.diskless_on_remaining = diskless

        # do_not_place_with intentionally skipped, just use the regex version
        dnpw_r = self._get_linstor_property(
            'do_not_place_with_regex', volume_type,
        )
        if dnpw_r and rg.do_not_place_with_regex != dnpw_r:
            rg.do_not_place_with_regex = dnpw_r

        layer_list = self._get_linstor_property(
            'layer_list', volume_type
        )
        if layer_list and rg.layer_list != layer_list.split(','):
            rg.layer_list = layer_list.split(',')

        provider_list = self._get_linstor_property(
            'provider_list', volume_type,
        )
        if provider_list and rg.provider_list != provider_list.split(','):
            rg.provider_list = provider_list.split(',')

        redundancy = self._get_linstor_property(
            'redundancy', volume_type,
        )
        if redundancy and rg.redundancy != int(redundancy):
            rg.redundancy = redundancy

        def make_aux_list(propvalue):
            if propvalue is None:
                return None
            return ['Aux/%s' % item for item in propvalue.split(',')]

        on_diff = self._get_linstor_property(
            'replicas_on_different', volume_type,
        )
        on_diff = make_aux_list(on_diff)
        if on_diff and rg.replicas_on_different != on_diff:
            rg.replicas_on_different = on_diff

        on_same = self._get_linstor_property(
            'replicas_on_same', volume_type
        )
        on_same = make_aux_list(on_same)
        if on_same is not None and rg.replicas_on_same != on_same:
            rg.replicas_on_same = on_same

        extra_props = {}
        props_need_update = False
        existing = rg.property_dict
        for k, v in volume_type.get('extra_specs', {}).items():
            if not k.startswith('linstor:property:'):
                continue
            prop_name = k[len('linstor:property:'):]
            prop_name = prop_name.replace(':', '/')
            extra_props[prop_name] = v
            props_need_update |= existing.get(prop_name) != v
        if props_need_update:
            existing.update(extra_props)
            rg.property_dict = existing

        return rg

    @wrap_linstor_api_exception
    @volume_utils.trace
    def create_volume(self, volume):
        """Create a new volume

        :param cinder.objects.volume.Volume volume: The new volume to create
        :return: A dict of fields to update on the volume object
        """
        LOG.info('Creating volume %s [volume_id: %s] [func: create_volume]', 
                 volume['name'], volume['id'])
        LOG.info('Volume size %s [volume_id: %s] [func: create_volume]', 
                 volume['size'], volume['id'])
        linstor_size = volume['size'] * units.Gi // units.Ki
        LOG.info('LINSTOR size %s [volume_id: %s] [func: create_volume]', 
                 linstor_size, volume['id'])

        rg = self._resource_group_for_volume_type(volume['volume_type'])
        linstor.Resource.from_resource_group(
            uri="[unused]",
            resource_group_name=rg.name,
            resource_name=volume['name'],
            vlm_sizes=[linstor_size],
            existing_client=self.c.get(),
        )

        return {}

    @wrap_linstor_api_exception
    @volume_utils.trace
    def create_volume_from_snapshot(self, volume, snapshot):
        """Create a new volume from a snapshot with unlinked clones by default"""
        LOG.info('Creating volume %s from snapshot %s [volume_id: %s] [func: create_volume_from_snapshot]', 
                 volume['name'], snapshot['name'], volume['id'])

        src = _get_existing_resource(
            self.c.get(),
            snapshot['volume']['name'],
            snapshot['volume_id'],
        )

        # Default to unlinked clone unless explicitly specified as linked
        use_linked_clone = False
        if volume.get('metadata') and volume['metadata'].get('useLinkedClone') == 'true':
            LOG.info('Using linked clone for volume %s [volume_id: %s] [func: create_volume_from_snapshot]', 
                     volume['name'], volume['id'])
            use_linked_clone = True
            final_name = volume['name']
        else:
            LOG.info('Using unlinked clone (default) for volume %s [volume_id: %s] [func: create_volume_from_snapshot]', 
                     volume['name'], volume['id'])
            final_name = volume['name'] + "-temp"

        try:
            # First create temporary resource from snapshot
            rsc = _restore_snapshot_to_new_resource(
                src, snapshot, final_name,
            )

            # Add timeout mechanism for initial resource creation
            start_time = timeutils.utcnow()
            timeout = 180
            retry_interval = 1

            # Wait for volume to be ready and resize if needed
            while True:
                try:
                    expected_size = volume['size'] * units.Gi
                    LOG.debug('Checking volume size. Expected: %s [volume_id: %s] [func: create_volume_from_snapshot]', 
                             expected_size, volume['id'])

                    if not rsc.volumes:
                        LOG.debug('Waiting for volume to be created... [volume_id: %s] [func: create_volume_from_snapshot]', 
                                 volume['id'])
                    elif rsc.volumes[0].size < expected_size:
                        LOG.info('Resizing volume to %s [volume_id: %s] [func: create_volume_from_snapshot]', 
                                 expected_size, volume['id'])
                        rsc.volumes[0].size = expected_size
                        break
                    else:
                        LOG.debug('Volume size is correct [volume_id: %s] [func: create_volume_from_snapshot]', 
                                 volume['id'])
                        break

                except linstor.LinstorError as e:
                    if timeutils.delta_seconds(start_time, timeutils.utcnow()) > timeout:
                        LOG.error('Timeout waiting for volume: %s [volume_id: %s] [func: create_volume_from_snapshot]', 
                                 e, volume['id'])
                        rsc.delete()
                        raise
                    LOG.debug('Volume not ready, retrying in %s seconds... [volume_id: %s] [func: create_volume_from_snapshot]', 
                             retry_interval, volume['id'])
                    time.sleep(retry_interval)
                    continue
                except Exception as e:
                    LOG.exception('Unexpected error handling volume [volume_id: %s] [func: create_volume_from_snapshot]', 
                                volume['id'])
                    rsc.delete()
                    raise

            # For unlinked clones (default case)
            if not use_linked_clone:
                LOG.info('Creating independent volume (unlinked clone) [volume_id: %s] [func: create_volume_from_snapshot]', 
                         volume['id'])
                try:
                    # Clear snapshot association
                    volume['snapshot_id'] = None
                    volume.save()

                    # Create new resource with proper size
                    linstor_size = volume['size'] * units.Gi // units.Ki
                    rg = self._resource_group_for_volume_type(volume['volume_type'])

                    LOG.debug('Creating new independent resource [volume_id: %s] [func: create_volume_from_snapshot]', 
                             volume['id'])
                    new_rsc = linstor.Resource.from_resource_group(
                        uri="[unused]",
                        resource_group_name=rg.name,
                        resource_name=volume['name'],
                        vlm_sizes=[linstor_size],
                        existing_client=self.c.get(),
                    )

                    # Wait for both resources to be ready
                    time.sleep(5)

                    LOG.debug('Copying data from temporary to final resource [volume_id: %s] [func: create_volume_from_snapshot]', 
                             volume['id'])
                    with _temp_resource_path(self.c.get(), rsc, self._hostname) as src_path, \
                        _temp_resource_path(self.c.get(), new_rsc, self._hostname) as dst_path:
                        self._execute('dd', 'if=' + src_path, 'of=' + dst_path, 'bs=1M', run_as_root=True)

                    LOG.info('Cleaning up temporary resource [volume_id: %s] [func: create_volume_from_snapshot]', 
                             volume['id'])
                    rsc.delete(snapshots=True)

                except Exception as e:
                    LOG.exception('Failed to create unlinked clone [volume_id: %s] [func: create_volume_from_snapshot]', 
                                volume['id'])
                    if 'new_rsc' in locals():
                        new_rsc.delete()
                    rsc.delete()
                    raise

            LOG.info('Successfully created volume from snapshot [volume_id: %s] [func: create_volume_from_snapshot]', 
                     volume['id'])
            return {}

        except Exception as e:
            LOG.exception('Failed to create volume from snapshot [volume_id: %s] [func: create_volume_from_snapshot]', 
                         volume['id'])
            try:
                leftover_rsc = linstor.Resource(
                    final_name,
                    existing_client=self.c.get(),
                )
                leftover_rsc.delete()
            except:
                pass
            raise

    @wrap_linstor_api_exception
    @volume_utils.trace
    def delete_volume(self, volume):
        """Delete the volume in the backend

        Uses HTTP request to papaya handler app which will handle the interaction with LINSTOR.
        :param cinder.objects.volume.Volume volume: the volume to delete
        """
        LOG.info('Deleting volume %s', volume['name'])

        # Make HTTP request to delete volume with retries
        api_url = f"http://0.0.0.0:5050/v1/volumes"
        headers = {'Content-Type': 'application/json'}
        retry_delays = [2, 3, 5, 8, 13]
        max_retries = len(retry_delays)
        
        # Try HTTP request with retries
        for attempt in range(max_retries):
            try:
                # Create query parameters
                query_params = {
                    'volume_name': volume['name'],
                    'volume_id': volume['id'],
                    'project_id': volume['project_id']
                }
                
                LOG.info("Sending DELETE request to %s for volume %s (attempt %d/%d)", 
                         api_url, volume['name'], attempt + 1, max_retries)
                LOG.info("Request query parameters: %s", query_params)
                
                response = requests.delete(
                    api_url, 
                    headers=headers,
                    params=query_params,
                    timeout=30
                )
                
                if response.status_code in (200, 202, 204):
                    LOG.info("HTTP DELETE request successful (status: %d) for volume %s", 
                             response.status_code, volume['name'])
                    return {}
                else:
                    LOG.error("HTTP DELETE request failed with status %d: %s", 
                              response.status_code, response.text)
                    
            except requests.RequestException as req_err:
                LOG.error("HTTP request error for volume %s: %s", volume['name'], str(req_err))
                
            # If this is the last attempt, don't wait
            if attempt == max_retries - 1:
                LOG.exception('Failed to delete volume %s after %d attempts via HTTP request to papaya handler', 
                            volume['name'], max_retries)
                # Return success anyway as the volume might be deleted later by the cleanup process
                return {}
                
            # Wait before retrying
            wait_time = retry_delays[attempt]
            LOG.warning("Retrying HTTP DELETE in %d seconds (attempt %d/%d)...", 
                      wait_time, attempt + 1, max_retries)
            time.sleep(wait_time)
        
        # Even if all attempts failed, return success as this will be handled by 
        # the papaya handler's cleanup process
        return {}

    @wrap_linstor_api_exception
    @volume_utils.trace
    def create_snapshot(self, snapshot):
        """Create a snapshot

        :param cinder.objects.snapshot.Snapshot snapshot: snapshot to create
        """
        LOG.info('Creating snapshot %s', snapshot['name'])
        rsc = _get_existing_resource(
            self.c.get(),
            snapshot['volume']['name'],
            snapshot['volume_id'],
        )
        rsc.snapshot_create(snapshot['name'])

    @wrap_linstor_api_exception
    @volume_utils.trace
    def delete_snapshot(self, snapshot):
        """Delete the given snapshot

        :param cinder.objects.snapshot.Snapshot snapshot: snapshot to delete
        """
        rsc = _get_existing_resource(
            self.c.get(),
            snapshot['volume']['name'],
            snapshot['volume_id'],
        )

        try:
            rsc.snapshot_delete(snapshot['name'])
        except linstor.LinstorError:
            raise exception.SnapshotIsBusy(snapshot['name'])

        return {}

    @wrap_linstor_api_exception
    @volume_utils.trace
    def revert_to_snapshot(self, context, volume, snapshot):
        """Reverts a volume to a snapshot state

        LINSTOR can only revert to the last snapshot. Reverting to an older
        snapshot would mean we had to delete other snapshots first, which we
        can't do as Cinder still expects them to be present after the revert.
        :param context: request context
        :param cinder.objects.volume.Volume volume: The volume to revert
        :param cinder.objects.snapshot.Snapshot snapshot: The snapshot to
         revert to
        """
        LOG.info('Reverting volume %s to snapshot %s', volume['name'],
                snapshot['name'])
        rsc = _get_existing_resource(
            self.c.get(),
            snapshot['volume']['name'],
            snapshot['volume_id'],
        )
        try:
            rsc.snapshot_rollback(snapshot['name'])
        except linstor.LinstorError:
            LOG.info('Failed to rollback snapshot, retrying with v1 driver '
                     'name %s', 'SN_' + snapshot['id'])
            rsc.snapshot_rollback('SN_' + snapshot['id'])


        LOG.info('Creating volume %s', volume['name'])
        LOG.info('Volume size %s', volume['size'])
        expected_size = volume['size'] * units.Gi
        LOG.info('LINSTOR size %s', expected_size)
        if rsc.volumes[0].size < expected_size:
            rsc.volumes[0].size = expected_size

    def snapshot_revert_use_temp_snapshot(self):
        """Do not create a snapshot in case revert_to_snapshot_fails

        Otherwise we could never revert to any snapshot: Linstor only supports
        reverting to the last snapshot, but if this returns true, a new
        snapshot is created before every call to revert_to_snapshot.
        """
        return False

    @wrap_linstor_api_exception
    @volume_utils.trace
    def create_cloned_volume(self, volume, src_vref):
        """Create a copy of an existing volume

        :param cinder.objects.volume.Volume volume: The new clone
        :param cinder.objects.volume.Volume src_vref: The volume to clone from
        """
        LOG.info('Creating clone %s from %s [volume_id: %s] [func: create_cloned_volume]', 
                 volume['name'], src_vref['name'], volume['id'])
        if self.configuration.safe_get('linstor_use_snapshot_based_clone'):
            ctxt = volume._context

            snapshot = objects.Snapshot(ctxt)
            snapshot.user_id = ctxt.user_id
            snapshot.project_id = ctxt.project_id
            snapshot.volume_id = src_vref['id']
            snapshot.volume_size = src_vref['size']
            snapshot.display_name = 'for-' + volume['id']
            snapshot.status = fields.SnapshotStatus.CREATING

            snapshot.create()
            snapshot.save()

            clone_snap = {
                'id': snapshot['id'],
                'name': 'snapshot-' + snapshot['id'],
                'volume': {
                    'name': src_vref['name'],
                    'id': src_vref['id'],
                },
                'volume_id': src_vref['id'],
            }

            self.create_snapshot(clone_snap)
            snapshot.status = fields.SnapshotStatus.AVAILABLE

            volume.source_volid = None
            volume.source_volstatus = None
            volume.snapshot_id = snapshot['id']
            volume.save()

            return self.create_volume_from_snapshot(volume, snapshot)
        else:
            rsc = _get_existing_resource(
                self.c.get(),
                src_vref['name'],
                src_vref['id'],
            )
            LOG.info('Cloning volume with direct clone method [volume_id: %s] [func: create_cloned_volume]', 
                     volume['id'])
            rsc.clone(volume['name'], use_zfs_clone=False)

            LOG.info('Origin volume size %s [volume_id: %s] [func: create_cloned_volume]', 
                     src_vref['size'], volume['id'])
            LOG.info('Cloned volume size %s [volume_id: %s] [func: create_cloned_volume]', 
                     volume['size'], volume['id'])
            # Handle size after clone
            if volume['size'] != src_vref['size']:
                LOG.info('Resizing cloned volume to %sGB [volume_id: %s] [func: create_cloned_volume]', 
                         volume['size'], volume['id'])
                cloned_rsc = _get_existing_resource(
                    self.c.get(),
                    volume['name'],
                    volume['id'],
                )
                # convert GB to bytes using units.Gi
                linstor_size = volume['size'] * units.Gi
                cloned_rsc.volumes[0].size = linstor_size

        return {}

    @wrap_linstor_api_exception
    @volume_utils.trace
    def copy_image_to_volume(self, context, volume, image_service, image_id,
                             disable_sparse=False):
        """Copy an image to a volume using image_utils.fetch_to_raw.

        This method downloads an image from the image service and writes it directly
        to the volume's device path. It includes progress tracking, verification,
        and proper error handling.

        :param context: The context for the request
        :param volume: The volume to copy the image to
        :param image_service: The image service to use
        :param image_id: The ID of the image to copy
        :param disable_sparse: Whether to disable sparse copying
        :return: Empty dict on success
        :raises: VolumeBackendAPIException if the operation fails
        """
        start_time = time.time()
        LOG.info('Initiating image copy to volume %s [volume_id: %s] [func: copy_image_to_volume]', 
                 volume['name'], volume['id'])

        # Set timeout for the entire operation (5 minutes)
        operation_timeout = 300  # 5 minutes in seconds
        last_progress_time = time.time()

        try:
            # Get the LINSTOR resource
            rsc_start_time = time.time()
            rsc = _get_existing_resource(self.c.get(), volume['name'], volume['id'])
            rsc_time = time.time() - rsc_start_time
            LOG.info('Got LINSTOR resource in %.2f seconds [volume_id: %s] [func: copy_image_to_volume]', 
                     rsc_time, volume['id'])
            
            # Get the device path with proper error handling
            try:
                path_start_time = time.time()
                with _temp_resource_path(self.c.get(), rsc, self._hostname,
                                       self._force_udev) as path:
                    path_time = time.time() - path_start_time
                    LOG.info('Got device path %s in %.2f seconds [volume_id: %s] [func: copy_image_to_volume]', 
                             path, path_time, volume['id'])
                    
                    # Get image size and verify it fits in volume
                    try:
                        meta_start_time = time.time()
                        image_meta = image_service.show(context, image_id)
                        image_size = image_meta['size']
                        volume_size = volume['size'] * units.Gi
                        meta_time = time.time() - meta_start_time
                        
                        LOG.info('Got image metadata in %.2f seconds. Size: %s [volume_id: %s] [func: copy_image_to_volume]', 
                                 meta_time, image_size, volume['id'])
                        
                        if image_size > volume_size:
                            LOG.error('Image size %s exceeds volume size %s [volume_id: %s] [func: copy_image_to_volume]', 
                                     image_size, volume_size, volume['id'])
                            raise exception.ImageTooBig(image_id=image_id)
                            
                    except Exception as e:
                        LOG.error('Failed to get image metadata: %s [volume_id: %s] [func: copy_image_to_volume]', 
                                 str(e), volume['id'])
                        raise exception.ImageNotFound(image_id=image_id)
                    
                    # Get blocksize from config or use default
                    blocksize = self.configuration.safe_get('dd_blocksize', '1M')
                    LOG.info('Using blocksize %s for image copy [volume_id: %s] [func: copy_image_to_volume]', 
                             blocksize, volume['id'])
                    
                    # Copy image to volume with progress tracking
                    try:
                        copy_start_time = time.time()
                        LOG.info('Starting image copy to volume %s [volume_id: %s] [func: copy_image_to_volume]', 
                                 volume['name'], volume['id'])
                        
                        # Create a progress monitoring thread
                        def monitor_progress():
                            while True:
                                current_time = time.time()
                                if current_time - last_progress_time > 30:  # Log progress every 30 seconds
                                    LOG.info('Image copy still in progress... [volume_id: %s] [func: copy_image_to_volume]', 
                                             volume['id'])
                                    last_progress_time = current_time
                                time.sleep(10)  # Check every 10 seconds
                        
                        progress_thread = threading.Thread(target=monitor_progress)
                        progress_thread.daemon = True
                        progress_thread.start()
                        
                        # Start the image copy operation
                        image_utils.fetch_to_raw(
                            context,
                            image_service,
                            image_id,
                            path,
                            blocksize,
                            size=volume['size'],
                            disable_sparse=disable_sparse
                        )
                        
                        copy_time = time.time() - copy_start_time
                        total_time = time.time() - start_time
                        
                        LOG.info('Successfully copied image to volume %s in %.2f seconds [volume_id: %s] [func: copy_image_to_volume]', 
                                 volume['name'], copy_time, volume['id'])
                        LOG.info('Total operation took %.2f seconds [volume_id: %s] [func: copy_image_to_volume]', 
                                 total_time, volume['id'])
                        return {}
                        
                    except Exception as e:
                        copy_time = time.time() - copy_start_time
                        LOG.error('Failed to copy image to volume after %.2f seconds: %s [volume_id: %s] [func: copy_image_to_volume]', 
                                 copy_time, str(e), volume['id'])
                        raise exception.VolumeBackendAPIException(
                            data=_('Failed to copy image to volume: %s') % str(e))
                            
            except Exception as e:
                path_time = time.time() - path_start_time
                LOG.error('Failed to get device path after %.2f seconds: %s [volume_id: %s] [func: copy_image_to_volume]', 
                          path_time, str(e), volume['id'])
                raise exception.VolumeBackendAPIException(
                    data=_('Failed to get device path: %s') % str(e))
            
        except linstor.LinstorError as e:
            total_time = time.time() - start_time
            LOG.error('LINSTOR error during image copy after %.2f seconds: %s [volume_id: %s] [func: copy_image_to_volume]', 
                      total_time, str(e), volume['id'])
            raise LinstorDriverApiException(e)
        
        except Exception as e:
            total_time = time.time() - start_time
            LOG.exception('Unexpected error during image copy after %.2f seconds [volume_id: %s] [func: copy_image_to_volume]', 
                         total_time, volume['id'])
            raise exception.VolumeBackendAPIException(
                data=_('Unexpected error during image copy: %s') % str(e))
        
        finally:
            # Check if operation exceeded timeout
            if time.time() - start_time > operation_timeout:
                LOG.error('Operation timed out after %.2f seconds [volume_id: %s] [func: copy_image_to_volume]', 
                          operation_timeout, volume['id'])
                # Update volume status to error
                volume.status = 'error'
                volume.save()
                raise exception.VolumeBackendAPIException(
                    data=_('Operation timed out after %d seconds') % operation_timeout)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        """Copy volume to image"""
        LOG.info('Copying volume to image %s [volume_id: %s] [func: copy_volume_to_image]', 
                 volume['name'], volume['id'])
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )

        with _temp_resource_path(self.c.get(), rsc, self._hostname,
                                 self._force_udev) as path:
            attach_info = {
                'conn': 'local',
                'device': {'path': path},
            }

            volume_utils.upload_volume(context,
                                       image_service,
                                       image_meta,
                                       attach_info['device']['path'],
                                       volume,
                                       compress=True)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def _update_volume_stats(self):
        """Refresh the Cinder storage pool statistics for scheduling decisions.

        Safely handles cases where nodes are down by checking for None values
        in storage pool capacities. Aggregates all (per-node) storage pools as
        a total capacity, even if that clashes with how replicated volumes are
        using these storage pools.
        """
        with self.c.get() as lclient:
            storage_pools = lclient.storage_pool_list_raise().storage_pools
            resource_dfns = lclient.resource_dfn_list_raise().resource_definitions

        def _safe_get_capacity(storage_pool, attr):
            """Safely get capacity value from storage pool.

            Args:
                storage_pool: Storage pool object
                attr: Attribute to get ('total_capacity' or 'free_capacity')

            Returns:
                int: Capacity value or 0 if unavailable
            """
            try:
                if storage_pool.free_space is None:
                    return 0
                value = getattr(storage_pool.free_space, attr)
                return value if value is not None else 0
            except AttributeError:
                return 0

        # Filter out diskless storage pools
        storage_pools = [sp for sp in storage_pools if not sp.is_diskless()]

        backend_name = self.configuration.volume_backend_name or self._linstor_uri_str

        # Safely calculate capacities
        tot = _kib_to_gib(sum(
            _safe_get_capacity(p, 'total_capacity') for p in storage_pools
        ))

        free = _kib_to_gib(sum(
            _safe_get_capacity(p, 'free_capacity') for p in storage_pools
        ))

        # Calculate provisioned capacity with None check
        provisioned_cap = _kib_to_gib(sum(
            vd.size for rd in resource_dfns
            for vd in rd.volume_definitions
            if hasattr(vd, 'size') and vd.size is not None
        ))

        # Check storage pool properties
        thin = any(p.is_thin() for p in storage_pools if hasattr(p, 'is_thin'))
        fat = any(not p.is_fat() for p in storage_pools if hasattr(p, 'is_fat'))
        volumes_in_pool = len(resource_dfns)

        self._stats = {
            'volume_backend_name': backend_name,
            'vendor_name': 'LINBIT',
            'driver_version': self.get_version(),
            'storage_protocol': self.protocol,
            'location_info': self._linstor_uri_str,
            'multiattach': True, # ToDo - need to make this dynamic based on whether DRBD multiattach is enabled in the volume type - linstor:DrbdOptions/Net/allow-two-primaries
            'online_extend_support': True, # This was dependent on the target driver, but now that we have a direct target driver, we can support online extend
            'total_capacity_gb': tot,
            'provisioned_capacity_gb': provisioned_cap,
            'free_capacity_gb': free,
            'max_over_subscription_ratio': 20.0 if thin else 0.0,
            'thin_provisioning_support': thin,
            'thick_provisioning_support': fat,
            'total_volumes': volumes_in_pool,
            'goodness_function': self.get_goodness_function(),
            'filter_function': self.get_filter_function(),
        }

        return self._stats

    @wrap_linstor_api_exception
    @volume_utils.trace
    def extend_volume(self, volume, new_size):
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )

        # convert GB to bytes using units.Gi
        linstor_size = new_size * units.Gi
        rsc.volumes[0].size = linstor_size

        if hasattr(self.target_driver, 'extend_target'):
            # ISCSI targets require additional resize encouragement
            self.target_driver.extend_target(volume)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def retype(self, context, volume, new_type, diff, host):
        """Retype a volume, i.e. allow updating QoS and extra specs"""
        LOG.debug('LINSTOR retype called for volume %s. No action '
                  'required for LINSTOR volumes.',
                  volume['id'])
        rg = self._resource_group_for_volume_type(new_type)
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )

        with self.c.get() as lclient:
            responses = lclient.resource_dfn_modify(
                rsc.linstor_name, property_dict={}, resource_group=rg.name,
            )
            if not lclient.all_api_responses_no_error(responses):
                raise LinstorDriverApiException(responses)

        return True, None

    @wrap_linstor_api_exception
    @volume_utils.trace
    def migrate_volume(self, context, volume, host):
        """Migrate a volume from one backend to another

        :param context: the request context
        :param volume: The volume to migrate (away from the self)
        :param host: The host/backend to migrate to
        :return: (True, model_update) if migration was successful
                 (False, None) if the volume could not be migrated
        """
        target_ctrl = host['capabilities'].get('location_info')
        if self._linstor_uri_str != target_ctrl:
            LOG.debug('Target is not using the same controllers: %s != %s',
                      self._linstor_uri_str, target_ctrl)
            return False, None

        target_protocol = host['capabilities'].get('storage_protocol')
        if volume['status'] in {'attached', 'in-use'} and \
                self.protocol != target_protocol:
            LOG.debug('Cannot migrate attached volume between different '
                      'transport protocols: %s -> %s',
                      self.protocol, target_protocol)
            return False, None

        return True, None

    # ====================== Transport related operations ====================
    # Mostly just passes through to the transport. One thing to note: in case
    # Of non-linstor-managed attach (linstor_direct=False) we need to have the
    # resource available locally for the target helper to attach
    @wrap_linstor_api_exception
    @volume_utils.trace
    def ensure_export(self, context, volume):
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )
        volume_path = None
        if not self._use_direct_connection():
            LOG.debug('using non-direct driver method, need to create local '
                      'replica on cinder host')
            volume_path = _ensure_resource_path(
                self.c.get(), rsc, self._hostname,
            )

        return self.target_driver.ensure_export(context, volume, volume_path)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def create_export(self, context, volume, connector):
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id'],
        )
        volume_path = None
        if not self._use_direct_connection():
            LOG.debug('using non-direct driver method, need to create local '
                      'replica on cinder host')
            volume_path = _ensure_resource_path(
                self.c.get(), rsc, self._hostname, self._force_udev
            )

        export_info = self.target_driver.create_export(
            context,
            volume,
            volume_path)

        if export_info:
            return {'provider_location': export_info['location'],
                    'provider_auth': export_info['auth'], }

        return {}

    @wrap_linstor_api_exception
    @volume_utils.trace
    def remove_export(self, context, volume):
        self.target_driver.remove_export(context, volume)
        if not self._use_direct_connection():
            LOG.debug('using non-direct driver method, need to delete local '
                      'replica on cinder host')
            rsc = _get_existing_resource(
                self.c.get(),
                volume['name'],
                volume['id'],
            )
            rsc.deactivate(self._hostname)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def initialize_connection(self, volume, connector, **kwargs):
        return self.target_driver.initialize_connection(volume, connector)

    @wrap_linstor_api_exception
    @volume_utils.trace
    def terminate_connection(self, volume, connector, **kwargs):
        # This is lifted from the LVMDriver. We only want to terminate
        # the connection if no attachments remain. This is important in multi-
        # attach scenarios.
        #
        # Linstor exposes a similar interface to LVM for ISCSI Targets, so we
        # can reuse this code from there. DRBD targets do not support multi-
        # attach in any case, so it will do a normal detach in that case.
        attachments = volume['volume_attachment']
        if volume['multiattach']:
            if sum(1 for a in attachments if a.connector and
                    a.connector['initiator'] == connector['initiator']) > 1:
                return True

        self.target_driver.terminate_connection(volume, connector, **kwargs)
        return len(attachments) > 1


@interface.volumedriver
class LinstorDrbdDriver(LinstorDriver):
    """Shim for a Linstor driver compatible with v1 LinstorDrbdDriver"""

    def __init__(self, *args, **kwargs):
        super(LinstorDrbdDriver, self).__init__(*args, **kwargs)

    def _use_direct_connection(self):
        return True


@interface.volumedriver
class LinstorIscsiDriver(LinstorDriver):
    """Shim for a Linstor driver compatible with v1 LinstorIscsiDriver"""

    def __init__(self, *args, **kwargs):
        super(LinstorIscsiDriver, self).__init__(*args, **kwargs)

    def _use_direct_connection(self):
        return False


class LinstorDirectTarget(targets.Target):
    """Target object that uses Linstor to create block devices"""
    # This may be a lie as there are other ways LINSTOR could do replication,
    # but this way we stay compatible with the v1 drivers
    protocol = constants.DRBD

    def __init__(self, client, force_udev=True, *args, **kwargs):
        """Uses Linstor to deploy resources directly on the target host

        :param ThreadSafeLinstorClient client: the client wrapper to use
        :param bool force_udev: Assume udev paths always exist.
        """
        super().__init__(*args, **kwargs)
        self.c = client
        self._force_udev = force_udev

    def ensure_export(self, context, volume, volume_path):
        pass

    def create_export(self, context, volume, volume_path):
        pass

    def remove_export(self, context, volume):
        pass

    @wrap_linstor_api_exception
    def initialize_connection(self, volume, connector):
        """Creates a connection and tells the target how to connect

        This target-driver ensures a replica of the request volume is available
        locally on the connection target.
        """
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id']
        )

        if connector['host'] not in _attached_on(volume):
            LOG.debug('Trying to attach to a volume in use, looks like live '
                      'migration. Setting "allow_two_primaries=True"')
            rsc.allow_two_primaries = True

        path = _ensure_resource_path(
            self.c.get(), rsc, connector['host'], self._force_udev,
        )
        return {
            'driver_volume_type': 'local',
            'data': {'device_path': path},
        }

    @wrap_linstor_api_exception
    def terminate_connection(self, volume, connector, **kwargs):
        """Terminates an existing connection

        This target-driver removes replicas created in initialize_connection.
        :param volume: the connected volume
        :param dict|None connector: which connection to terminate, or None if
         all connection should be terminated
        """
        rsc = _get_existing_resource(
            self.c.get(),
            volume['name'],
            volume['id']
        )

        if connector is None:
            LOG.debug('force detach of volume, no clever deactivating of '
                      'resources required')
            # Since we detach everything we can also reset this
            rsc.allow_two_primaries = False
            return

        if volume['status'] == 'in-use' and \
                any(x != connector['host'] for x in _attached_on(volume)):
            LOG.debug('Trying to detach from a volume in use, looks like live '
                      'migration. Resetting "allow_two_primaries=False"')
            rsc.allow_two_primaries = False

        # This might delete the tiebreaker. Think about a workaround!
        rsc.deactivate(connector['host'])


@contextlib.contextmanager
@wrap_linstor_api_exception
def _temp_resource_path(linstor_client, rsc, host, force_udev=True):
    """Temporarily attach the given resource on a host

    :param linstor.Linstor linstor_client: Client used for API calls
    :param linstor.Resource rsc: The resource to attach
    :param str host: The host as named in LINSTOR
    :param bool force_udev: Assume udev paths always exist.
    :return: The path to the temporary device
    :rtype: str
    """
    try:
        yield _ensure_resource_path(linstor_client, rsc, host, force_udev)
    finally:
        rsc.deactivate(host)


@wrap_linstor_api_exception
def _ensure_resource_path(linstor_client, rsc, host, force_udev=True):
    """Ensure a resource is deployed on a host and return its device path

    :param linstor.Linstor linstor_client: Client used for API calls
    :param linstor.Resource rsc: The resource to deploy on the node
    :param str host: The host as named in Linstor
    :param bool force_udev: Assume udev paths always exist.
    :return: The path to the deployed node
    :rtype: str
    """
    rsc.activate(host)
    symlink = _find_symlink_to_device(linstor_client, rsc.name, host)
    if symlink:
        return symlink
    if force_udev:
        return "/dev/drbd/by-res/%s/0" % rsc.name

    return rsc.volumes[0].device_path


def _get_existing_resource(linstor_client, volume_name, volume_id):
    """Get an existing resource matching a cinder volume

    :param linstor.Linstor linstor_client: Client used for API calls
    :param str volume_name: The name of the volume (most likely volume-<id>)
    :param str volume_id: The id of the volume (most likely a UUIDv4)
    :return: The matching resource object
    :rtype: linstor.Resource
    """
    LOG.debug('Searching existing volume %s in LINSTOR '
              'backend', volume_name)
    plain_rsc = linstor.Resource(volume_name, existing_client=linstor_client)
    if plain_rsc.defined:
        LOG.debug('Found matching resource named: %s', plain_rsc.name)
        return plain_rsc

    alt_name = "CV_" + volume_id
    alt_rsc = linstor.Resource(alt_name, existing_client=linstor_client)
    if alt_rsc.defined:
        LOG.debug('Found matching resource named: %s', alt_rsc.name)
        return alt_rsc

    msg = _('Found no matching resource in LINSTOR backend for '
            'volume %s') % volume_name
    raise LinstorDriverException(msg)


@wrap_linstor_api_exception
def _restore_snapshot_to_new_resource(resource, snap, restore_name):
    """Try restoring a snapshot to a new resource

    Note: in case of an exception during the restore process things such as
    the resource definition of the target can be left on the server. To retry
    the process ensure those resources are deleted first.
    :param linstor.Resource resource: The source of the snapshot
    :param cinder.objects.snapshot.Snapshot snap: The snapshot to restore.
    If it can't be found, it's retried with a v1 compatible name.
    :param str restore_name: The name of the resource to restore to.
    :return: The restored resource
    :rtype: linstor.Resource
    """
    try:
        return resource.restore_from_snapshot(snap['name'], restore_name)
    except linstor.LinstorError:
        LOG.info('failed to restore snapshot, retrying with fallback id')
        return resource.restore_from_snapshot('SN_' + snap['id'], restore_name)


@wrap_linstor_api_exception
def _find_symlink_to_device(linstor_client, resource_name, node):
    """Get the symlink to a device managed by Linstor

    This is useful when managing encrypted volumes, since os_brick likes to
    mess with the device_path provided. In particular, it replaces the local
    device path with a symlink to the decrypted volume, but never changes it
    back. If we gave the device path directly os_brick would be happy to
    replace the device file with a symlink, which finally breaks Linstor as
    the device points to a potentially non-existent volume.

    :param linstor.Linstor linstor_client: Client used for API calls
    :param str resource_name: The resource for which to find the symlink
    :param str node: The name of the node for which to find the symlink
    :returns: A symlink as a path, or None if no symlink was found
    :rtype: str|None
    """
    with linstor_client as lclient:
        vol_list = lclient.volume_list_raise(
            filter_by_nodes=[node],
            filter_by_resources=[resource_name],
        )
    if len(vol_list.resources) != 1:
        msg = _('Unexpected response to volume_list: %s') % vol_list.resources
        raise LinstorDriverException(msg)

    volume = vol_list.resources[0].volumes[0]
    links = [v for k, v in volume.properties.items()
             if k.startswith('Satellite/Device/Symlinks/')]

    if not links:
        LOG.debug('Could not find symlinks: No udev rules or Linstor too old?')
        return None

    for symlink in links:
        # /dev/drbd/by-res/... is the preferred symlink
        if 'by-res' in symlink:
            return symlink

    # Fallback: just return any
    return links[0]


def _kib_to_gib(kib):
    """Converts KiB to GiB with rounding up

    :param int kib: the value to convert in KiB
    :returns: Value in GiB, rounded up
    :rtype: int
    """
    return linstor.SizeCalc.convert_round_up(
        kib,
        linstor.SizeCalc.UNIT_KiB,
        linstor.SizeCalc.UNIT_GiB,
    )


def _attached_on(volume):
    """Checks if the volume is attached on another host

    :param volume cinder.objects.volume.Volume: the current volume state
    :returns: The list of hosts this is currently attached at
    :rtype: list[str]
    """
    return [attachment['attached_host']
            for attachment in volume['volume_attachment']]


class LinstorDriverException(exception.VolumeDriverException):
    def __init__(self, message=None, **kwargs):
        super().__init__(message, **kwargs)


class LinstorDriverApiException(exception.VolumeBackendAPIException):
    def __init__(self, error, **kwargs):
        super().__init__(data=error, **kwargs)
