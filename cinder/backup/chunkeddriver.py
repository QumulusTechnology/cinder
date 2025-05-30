# Copyright (C) 2012 Hewlett-Packard Development Company, L.P.
# Copyright (c) 2014 TrilioData, Inc
# Copyright (c) 2015 EMC Corporation
# Copyright (C) 2015 Kevin Fox <kevin@efox.cc>
# Copyright (C) 2015 Tom Barron <tpb@dyncloud.net>
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Generic base class to implement metadata, compression and chunked data
   operations
"""

import abc
import hashlib
import json
import os
import sys

import eventlet
from oslo_config import cfg
from oslo_log import log as logging
from oslo_service import loopingcall
from oslo_utils import excutils
from oslo_utils import secretutils
from oslo_utils import units

from cinder.backup import driver
from cinder import exception
from cinder.i18n import _
from cinder import objects
from cinder.objects import fields
from cinder.volume import volume_utils

if sys.platform == 'win32':
    from os_win import utilsfactory as os_win_utilsfactory
else:
    os_win_utilsfactory = None

LOG = logging.getLogger(__name__)

backup_opts = [
    cfg.StrOpt('backup_compression_algorithm',
               default='zlib',
               ignore_case=True,
               choices=[('none', 'Do not use compression'),
                        ('off', "Same as 'none'"),
                        ('no', "Same as 'none'"),
                        ('zlib', 'Use the Deflate compression algorithm'),
                        ('gzip', "Same as 'zlib'"),
                        ('bz2', 'Use Burrows-Wheeler transform compression'),
                        ('bzip2', "Same as 'bz2'"),
                        ('zstd', 'Use the Zstandard compression algorithm')],
               help="Compression algorithm for backups ('none' to disable)"),
]

CONF = cfg.CONF
CONF.register_opts(backup_opts)


def _write_nonzero(volume_file, volume_offset, content):
    """Write non-zero parts of `content` into `volume_file`."""
    chunk_length = 1024 * 1024
    content = memoryview(content)
    for chunk_offset in range(0, len(content), chunk_length):
        chunk_end = chunk_offset + chunk_length
        chunk = content[chunk_offset:chunk_end]
        # The len(chunk) may be smaller than chunk_length. It's okay.
        if not volume_utils.is_all_zero(chunk):
            volume_file.seek(volume_offset + chunk_offset)
            volume_file.write(chunk.tobytes())


def _write_volume(volume_is_new, volume_file, volume_offset, content):
    if volume_is_new:
        _write_nonzero(volume_file, volume_offset, content)
    else:
        volume_file.seek(volume_offset)
        volume_file.write(content)


# Object writer and reader returned by inheriting classes must not have any
# logging calls, as well as the compression libraries, as eventlet has a bug
# (https://github.com/eventlet/eventlet/issues/432) that would result in
# failures.

class ChunkedBackupDriver(driver.BackupDriver, metaclass=abc.ABCMeta):
    """Abstract chunked backup driver.

       Implements common functionality for backup drivers that store volume
       data in multiple "chunks" in a backup repository when the size of
       the backed up cinder volume exceeds the size of a backup repository
       "chunk."

       Provides abstract methods to be implemented in concrete chunking
       drivers.
    """

    DRIVER_VERSION = '1.0.0'
    DRIVER_VERSION_MAPPING = {'1.0.0': '_restore_v1'}

    def _get_compressor(self, algorithm):
        try:
            if algorithm.lower() in ('none', 'off', 'no'):
                return None
            if algorithm.lower() in ('zlib', 'gzip'):
                import zlib as compressor
                result = compressor
            elif algorithm.lower() in ('bz2', 'bzip2'):
                import bz2 as compressor
                result = compressor
            elif algorithm.lower() == 'zstd':
                import zstd as compressor
                result = compressor
            else:
                result = None
            if result:
                # NOTE(geguileo): Compression/Decompression starves
                # greenthreads so we use a native thread instead.
                return eventlet.tpool.Proxy(result)
        except ImportError:
            pass

        err = _('unsupported compression algorithm: %s') % algorithm
        raise ValueError(err)

    def __init__(
        self, context, chunk_size_bytes, sha_block_size_bytes,
        backup_default_container, enable_progress_timer,
    ):
        super(ChunkedBackupDriver, self).__init__(context)
        self.chunk_size_bytes = chunk_size_bytes
        self.sha_block_size_bytes = sha_block_size_bytes
        self.backup_default_container = backup_default_container
        self.enable_progress_timer = enable_progress_timer

        self.backup_timer_interval = CONF.backup_timer_interval
        self.data_block_num = CONF.backup_object_number_per_notification
        self.az = CONF.storage_availability_zone
        self.backup_compression_algorithm = CONF.backup_compression_algorithm
        self.compressor = \
            self._get_compressor(CONF.backup_compression_algorithm)
        self.support_force_delete = True

        if sys.platform == 'win32' and self.chunk_size_bytes % 4096:
            # The chunk size must be a multiple of the sector size. In order
            # to fail out early and avoid attaching the disks, we'll just
            # enforce the chunk size to be a multiple of 4096.
            err = _("Invalid chunk size. It must be a multiple of 4096.")
            raise exception.InvalidConfigurationValue(message=err)

    def _get_object_writer(self, container, object_name, extra_metadata=None):
        """Return writer proxy-wrapped to execute methods in native thread."""
        writer = self.get_object_writer(container, object_name, extra_metadata)
        return eventlet.tpool.Proxy(writer)

    def _get_object_reader(self, container, object_name, extra_metadata=None):
        """Return reader proxy-wrapped to execute methods in native thread."""
        reader = self.get_object_reader(container, object_name, extra_metadata)
        return eventlet.tpool.Proxy(reader)

    # To create your own "chunked" backup driver, implement the following
    # abstract methods.

    @abc.abstractmethod
    def put_container(self, container):
        """Create the container if needed. No failure if it pre-exists."""
        return

    @abc.abstractmethod
    def get_container_entries(self, container, prefix):
        """Get container entry names."""
        return

    @abc.abstractmethod
    def get_object_writer(self, container, object_name, extra_metadata=None):
        """Returns a writer object which stores the chunk data.

        The object returned should be a context handler that can be used in a
        "with" context.

        The object writer methods must not have any logging calls, as eventlet
        has a bug (https://github.com/eventlet/eventlet/issues/432) that would
        result in failures.
        """
        return

    @abc.abstractmethod
    def get_object_reader(self, container, object_name, extra_metadata=None):
        """Returns a reader object for the backed up chunk.

        The object reader methods must not have any logging calls, as eventlet
        has a bug (https://github.com/eventlet/eventlet/issues/432) that would
        result in failures.
        """
        return

    @abc.abstractmethod
    def delete_object(self, container, object_name):
        """Delete object from container."""
        return

    @abc.abstractmethod
    def _generate_object_name_prefix(self, backup):
        return

    @abc.abstractmethod
    def update_container_name(self, backup, container):
        """Allow sub-classes to override container name.

        This method exists so that sub-classes can override the container name
        as it comes in to the driver in the backup object. Implementations
        should return None if no change to the container name is desired.
        """
        return

    @abc.abstractmethod
    def get_extra_metadata(self, backup, volume):
        """Return extra metadata to use in prepare_backup.

        This method allows for collection of extra metadata in prepare_backup()
        which will be passed to get_object_reader() and get_object_writer().
        Subclass extensions can use this extra information to optimize
        data transfers. Return a json serializable object.
        """
        return

    def _create_container(self, backup):
        # Container's name will be decided by the driver (returned by method
        # update_container_name), if no change is required by the driver then
        # we'll use the one the backup object already has, but if it doesn't
        # have one backup_default_container will be used.
        new_container = self.update_container_name(backup, backup.container)
        if new_container:
            # If the driver is not really changing the name we don't want to
            # dirty the field in the object and save it to the DB with the same
            # value.
            if new_container != backup.container:
                backup.container = new_container
        elif backup.container is None:
            backup.container = self.backup_default_container

        LOG.debug('_create_container started, container: %(container)s,'
                  'backup: %(backup_id)s.',
                  {'container': backup.container, 'backup_id': backup.id})

        backup.save()
        self.put_container(backup.container)
        return backup.container

    def _generate_object_names(self, backup):
        prefix = backup['service_metadata']
        object_names = self.get_container_entries(backup['container'], prefix)
        LOG.debug('generated object list: %s.', object_names)
        return object_names

    def _metadata_filename(self, backup):
        object_name = backup['service_metadata']
        filename = '%s_metadata' % object_name
        return filename

    def _sha256_filename(self, backup):
        object_name = backup['service_metadata']
        filename = '%s_sha256file' % object_name
        return filename

    def _write_metadata(self, backup, volume_id, container, object_list,
                        volume_meta, extra_metadata=None):
        filename = self._metadata_filename(backup)
        LOG.debug('_write_metadata started, container name: %(container)s,'
                  ' metadata filename: %(filename)s.',
                  {'container': container, 'filename': filename})
        metadata = {}
        metadata['version'] = self.DRIVER_VERSION
        metadata['backup_id'] = backup['id']
        metadata['volume_id'] = volume_id
        metadata['backup_name'] = backup['display_name']
        metadata['backup_description'] = backup['display_description']
        metadata['created_at'] = str(backup['created_at'])
        metadata['objects'] = object_list
        metadata['parent_id'] = backup['parent_id']
        metadata['volume_meta'] = volume_meta
        if extra_metadata:
            metadata['extra_metadata'] = extra_metadata
        metadata_json = json.dumps(metadata, sort_keys=True, indent=2)
        metadata_json = metadata_json.encode('utf-8')
        with self._get_object_writer(container, filename) as writer:
            writer.write(metadata_json)
        LOG.debug('_write_metadata finished. Metadata: %s.', metadata_json)

    def _write_sha256file(self, backup, volume_id, container, sha256_list):
        filename = self._sha256_filename(backup)
        LOG.debug('_write_sha256file started, container name: %(container)s,'
                  ' sha256file filename: %(filename)s.',
                  {'container': container, 'filename': filename})
        sha256file = {}
        sha256file['version'] = self.DRIVER_VERSION
        sha256file['backup_id'] = backup['id']
        sha256file['volume_id'] = volume_id
        sha256file['backup_name'] = backup['display_name']
        sha256file['backup_description'] = backup['display_description']
        sha256file['created_at'] = str(backup['created_at'])
        sha256file['chunk_size'] = self.sha_block_size_bytes
        sha256file['sha256s'] = sha256_list
        sha256file_json = json.dumps(sha256file, sort_keys=True, indent=2)
        sha256file_json = sha256file_json.encode('utf-8')
        with self._get_object_writer(container, filename) as writer:
            writer.write(sha256file_json)
        LOG.debug('_write_sha256file finished.')

    def _read_metadata(self, backup):
        container = backup['container']
        filename = self._metadata_filename(backup)
        LOG.debug('_read_metadata started, container name: %(container)s, '
                  'metadata filename: %(filename)s.',
                  {'container': container, 'filename': filename})
        with self._get_object_reader(container, filename) as reader:
            metadata_json = reader.read()
        metadata_json = metadata_json.decode('utf-8')
        metadata = json.loads(metadata_json)
        LOG.debug('_read_metadata finished. Metadata: %s.', metadata_json)
        return metadata

    def _read_sha256file(self, backup):
        container = backup['container']
        filename = self._sha256_filename(backup)
        LOG.debug('_read_sha256file started, container name: %(container)s, '
                  'sha256 filename: %(filename)s.',
                  {'container': container, 'filename': filename})
        with self._get_object_reader(container, filename) as reader:
            sha256file_json = reader.read()
        sha256file_json = sha256file_json.decode('utf-8')
        sha256file = json.loads(sha256file_json)
        LOG.debug('_read_sha256file finished.')
        return sha256file

    def _prepare_backup(self, backup):
        """Prepare the backup process and return the backup metadata."""
        volume = self.db.volume_get(self.context, backup.volume_id)

        if volume['size'] <= 0:
            err = _('volume size %d is invalid.') % volume['size']
            raise exception.InvalidVolume(reason=err)

        container = self._create_container(backup)

        object_prefix = self._generate_object_name_prefix(backup)
        backup.service_metadata = object_prefix
        backup.save()

        volume_size_bytes = volume['size'] * units.Gi
        availability_zone = self.az
        LOG.debug('starting backup of volume: %(volume_id)s,'
                  ' volume size: %(volume_size_bytes)d, object names'
                  ' prefix %(object_prefix)s, availability zone:'
                  ' %(availability_zone)s',
                  {
                      'volume_id': backup.volume_id,
                      'volume_size_bytes': volume_size_bytes,
                      'object_prefix': object_prefix,
                      'availability_zone': availability_zone,
                  })
        object_meta = {'id': 1, 'list': [], 'prefix': object_prefix,
                       'volume_meta': None}
        object_sha256 = {'id': 1, 'sha256s': [], 'prefix': object_prefix}
        extra_metadata = self.get_extra_metadata(backup, volume)
        if extra_metadata is not None:
            object_meta['extra_metadata'] = extra_metadata

        return (object_meta, object_sha256, extra_metadata, container,
                volume_size_bytes)

    def _backup_chunk(self, backup, container, data, data_offset,
                      object_meta, extra_metadata):
        """Backup data chunk based on the object metadata and offset."""
        object_prefix = object_meta['prefix']
        object_list = object_meta['list']

        object_id = object_meta['id']
        object_name = '%s-%05d' % (object_prefix, object_id)
        obj = {}
        obj[object_name] = {}
        obj[object_name]['offset'] = data_offset
        obj[object_name]['length'] = len(data)
        LOG.debug('Backing up chunk of data from volume.')
        algorithm, output_data = self._prepare_output_data(data)
        obj[object_name]['compression'] = algorithm
        LOG.debug('About to put_object')
        with self._get_object_writer(
                container, object_name, extra_metadata=extra_metadata
        ) as writer:
            writer.write(output_data)
        md5 = eventlet.tpool.execute(
            secretutils.md5, data, usedforsecurity=False).hexdigest()
        obj[object_name]['md5'] = md5
        LOG.debug('backup MD5 for %(object_name)s: %(md5)s',
                  {'object_name': object_name, 'md5': md5})
        object_list.append(obj)
        object_id += 1
        object_meta['list'] = object_list
        object_meta['id'] = object_id

        LOG.debug('Calling eventlet.sleep(0)')
        eventlet.sleep(0)

    def _prepare_output_data(self, data):
        if self.compressor is None:
            return 'none', data
        data_size_bytes = len(data)
        # Execute compression in native thread so it doesn't prevent
        # cooperative greenthread switching.
        compressed_data = self.compressor.compress(data)
        comp_size_bytes = len(compressed_data)
        algorithm = CONF.backup_compression_algorithm.lower()
        if comp_size_bytes >= data_size_bytes:
            LOG.debug('Compression of this chunk was ineffective: '
                      'original length: %(data_size_bytes)d, '
                      'compressed length: %(compressed_size_bytes)d. '
                      'Using original data for this chunk.',
                      {'data_size_bytes': data_size_bytes,
                       'compressed_size_bytes': comp_size_bytes,
                       })
            return 'none', data
        LOG.debug('Compressed %(data_size_bytes)d bytes of data '
                  'to %(comp_size_bytes)d bytes using %(algorithm)s.',
                  {'data_size_bytes': data_size_bytes,
                   'comp_size_bytes': comp_size_bytes,
                   'algorithm': algorithm,
                   })
        return algorithm, compressed_data

    def _finalize_backup(self, backup, container, object_meta, object_sha256):
        """Write the backup's metadata to the backup repository."""
        object_list = object_meta['list']
        object_id = object_meta['id']
        volume_meta = object_meta['volume_meta']
        sha256_list = object_sha256['sha256s']
        extra_metadata = object_meta.get('extra_metadata')
        self._write_sha256file(backup,
                               backup.volume_id,
                               container,
                               sha256_list)
        self._write_metadata(backup,
                             backup.volume_id,
                             container,
                             object_list,
                             volume_meta,
                             extra_metadata)
        # NOTE(whoami-rajat) : The object_id variable is used to name
        # the backup objects and hence differs from the object_count
        # variable, therefore the increment of object_id value in the last
        # iteration of _backup_chunk() method shouldn't be reflected in the
        # object_count variable.
        backup.object_count = object_id - 1
        backup.save()
        LOG.debug('backup %s finished.', backup['id'])

    def _backup_metadata(self, backup, object_meta):
        """Backup volume metadata.

        NOTE(dosaboy): the metadata we are backing up is obtained from a
                       versioned api so we should not alter it in any way here.
                       We must also be sure that the service that will perform
                       the restore is compatible with version used.
        """
        json_meta = self.get_metadata(backup['volume_id'])
        if not json_meta:
            LOG.debug("No volume metadata to backup.")
            return

        object_meta["volume_meta"] = json_meta

    def _send_progress_end(self, context, backup, object_meta):
        object_meta['backup_percent'] = 100
        volume_utils.notify_about_backup_usage(context,
                                               backup,
                                               "createprogress",
                                               extra_usage_info=
                                               object_meta)

    def _send_progress_notification(self, context, backup, object_meta,
                                    total_block_sent_num, total_volume_size):
        backup_percent = total_block_sent_num * 100 / total_volume_size
        object_meta['backup_percent'] = backup_percent
        volume_utils.notify_about_backup_usage(context,
                                               backup,
                                               "createprogress",
                                               extra_usage_info=
                                               object_meta)

    def _get_win32_phys_disk_size(self, disk_path):
        win32_diskutils = os_win_utilsfactory.get_diskutils()
        disk_number = win32_diskutils.get_device_number_from_device_name(
            disk_path)
        return win32_diskutils.get_disk_size(disk_number)

    def _calculate_sha(self, data):
        """Calculate SHA256 of a data chunk.

        This method cannot log anything as it is called on a native thread.
        """
        # NOTE(geguileo): Using memoryview to avoid data copying when slicing
        # for the sha256 call.
        chunk = memoryview(data)
        shalist = []
        off = 0
        datalen = len(chunk)
        while off < datalen:
            chunk_end = min(datalen, off + self.sha_block_size_bytes)
            block = chunk[off:chunk_end]
            sha = hashlib.sha256(block).hexdigest()
            shalist.append(sha)
            off += self.sha_block_size_bytes
        return shalist

    def backup(self, backup, volume_file, backup_metadata=True):
        """Backup the given volume.

           If backup['parent_id'] is given, then an incremental backup
           is performed.
        """
        if self.chunk_size_bytes % self.sha_block_size_bytes:
            err = _('Chunk size is not multiple of '
                    'block size for creating hash.')
            raise exception.InvalidBackup(reason=err)

        # Read the shafile of the parent backup if backup['parent_id']
        # is given.
        parent_backup_shafile = None
        parent_backup = None
        if backup.parent_id:
            parent_backup = objects.Backup.get_by_id(self.context,
                                                     backup.parent_id)
            parent_backup_shafile = self._read_sha256file(parent_backup)
            parent_backup_shalist = parent_backup_shafile['sha256s']
            if (parent_backup_shafile['chunk_size'] !=
                    self.sha_block_size_bytes):
                err = (_('Hash block size has changed since the last '
                         'backup. New hash block size: %(new)s. Old hash '
                         'block size: %(old)s. Do a full backup.')
                       % {'old': parent_backup_shafile['chunk_size'],
                          'new': self.sha_block_size_bytes})
                raise exception.InvalidBackup(reason=err)
            # If the volume size increased since the last backup, fail
            # the incremental backup and ask user to do a full backup.
            if backup.size > parent_backup.size:
                err = _('Volume size increased since the last '
                        'backup. Do a full backup.')
                raise exception.InvalidBackup(reason=err)

        win32_disk_size = None
        if sys.platform == 'win32':
            # When dealing with Windows physical disks, we need the exact
            # size of the disk. Attempting to read passed this boundary will
            # lead to an IOError exception. At the same time, we cannot
            # seek to the end of file.
            win32_disk_size = self._get_win32_phys_disk_size(volume_file.name)

        (object_meta, object_sha256, extra_metadata, container,
         volume_size_bytes) = self._prepare_backup(backup)

        counter = 0
        total_block_sent_num = 0

        # There are two mechanisms to send the progress notification.
        # 1. The notifications are periodically sent in a certain interval.
        # 2. The notifications are sent after a certain number of chunks.
        # Both of them are working simultaneously during the volume backup,
        # when "chunked" backup drivers are deployed.
        def _notify_progress():
            self._send_progress_notification(self.context, backup,
                                             object_meta,
                                             total_block_sent_num,
                                             volume_size_bytes)
        timer = loopingcall.FixedIntervalLoopingCall(
            _notify_progress)
        if self.enable_progress_timer:
            timer.start(interval=self.backup_timer_interval)

        sha256_list = object_sha256['sha256s']
        shaindex = 0
        is_backup_canceled = False
        while True:
            # First of all, we check the status of this backup. If it
            # has been changed to delete or has been deleted, we cancel the
            # backup process to do forcing delete.
            with backup.as_read_deleted():
                backup.refresh()
            if backup.status in (fields.BackupStatus.DELETING,
                                 fields.BackupStatus.DELETED):
                is_backup_canceled = True
                # To avoid the chunk left when deletion complete, need to
                # clean up the object of chunk again.
                self.delete_backup(backup)
                LOG.debug('Cancel the backup process of %s.', backup.id)
                break
            data_offset = volume_file.tell()

            if win32_disk_size is not None:
                read_bytes = min(self.chunk_size_bytes,
                                 win32_disk_size - data_offset)
            else:
                read_bytes = self.chunk_size_bytes
            data = volume_file.read(read_bytes)

            if data == b'':
                break

            # Calculate new shas with the datablock.
            shalist = eventlet.tpool.execute(self._calculate_sha, data)
            sha256_list.extend(shalist)

            # If parent_backup is not None, that means an incremental
            # backup will be performed.
            if parent_backup:
                # Find the extent that needs to be backed up.
                extent_off = -1
                for idx, sha in enumerate(shalist):
                    if sha != parent_backup_shalist[shaindex]:
                        if extent_off == -1:
                            # Start of new extent.
                            extent_off = idx * self.sha_block_size_bytes
                    else:
                        if extent_off != -1:
                            # We've reached the end of extent.
                            extent_end = idx * self.sha_block_size_bytes
                            segment = data[extent_off:extent_end]
                            self._backup_chunk(backup, container, segment,
                                               data_offset + extent_off,
                                               object_meta,
                                               extra_metadata)
                            extent_off = -1
                    shaindex += 1

                # The last extent extends to the end of data buffer.
                if extent_off != -1:
                    extent_end = len(data)
                    segment = data[extent_off:extent_end]
                    self._backup_chunk(backup, container, segment,
                                       data_offset + extent_off,
                                       object_meta, extra_metadata)
                    extent_off = -1
            else:  # Do a full backup.
                self._backup_chunk(backup, container, data, data_offset,
                                   object_meta, extra_metadata)

            # Notifications
            total_block_sent_num += self.data_block_num
            counter += 1
            if counter == self.data_block_num:
                # Send the notification to Ceilometer when the chunk
                # number reaches the data_block_num.  The backup percentage
                # is put in the metadata as the extra information.
                self._send_progress_notification(self.context, backup,
                                                 object_meta,
                                                 total_block_sent_num,
                                                 volume_size_bytes)
                # Reset the counter
                counter = 0

        # Stop the timer.
        timer.stop()
        # If backup has been cancelled we have nothing more to do
        # but timer.stop().
        if is_backup_canceled:
            return
        # All the data have been sent, the backup_percent reaches 100.
        self._send_progress_end(self.context, backup, object_meta)

        object_sha256['sha256s'] = sha256_list
        if backup_metadata:
            try:
                self._backup_metadata(backup, object_meta)
            # Whatever goes wrong, we want to log, cleanup, and re-raise.
            except Exception:
                with excutils.save_and_reraise_exception():
                    LOG.exception("Backup volume metadata failed.")
                    self.delete_backup(backup)

        self._finalize_backup(backup, container, object_meta, object_sha256)

    def _restore_v1(self, backup, volume_id, metadata, volume_file,
                    volume_is_new, requested_backup):
        """Restore a v1 volume backup.

        Raises BackupRestoreCancel on any requested_backup status change, we
        ignore the backup parameter for this check since that's only the
        current data source from the list of backup sources.
        """
        backup_id = backup['id']
        LOG.debug('v1 volume backup restore of %s started.', backup_id)
        extra_metadata = metadata.get('extra_metadata')
        container = backup['container']
        metadata_objects = metadata['objects']
        metadata_object_names = []
        for obj in metadata_objects:
            metadata_object_names.extend(obj.keys())
        LOG.debug('metadata_object_names = %s.', metadata_object_names)
        prune_list = [self._metadata_filename(backup),
                      self._sha256_filename(backup)]
        object_names = [object_name for object_name in
                        self._generate_object_names(backup)
                        if object_name not in prune_list]
        if sorted(object_names) != sorted(metadata_object_names):
            err = _('restore_backup aborted, actual object list '
                    'does not match object list stored in metadata.')
            raise exception.InvalidBackup(reason=err)

        for metadata_object in metadata_objects:
            # Abort when status changes to error, available, or anything else
            with requested_backup.as_read_deleted():
                requested_backup.refresh()
            if requested_backup.status != fields.BackupStatus.RESTORING:
                raise exception.BackupRestoreCancel(back_id=backup.id,
                                                    vol_id=volume_id)

            object_name, obj = list(metadata_object.items())[0]
            LOG.debug('restoring object. backup: %(backup_id)s, '
                      'container: %(container)s, object name: '
                      '%(object_name)s, volume: %(volume_id)s.',
                      {
                          'backup_id': backup_id,
                          'container': container,
                          'object_name': object_name,
                          'volume_id': volume_id,
                      })

            with self._get_object_reader(
                    container, object_name,
                    extra_metadata=extra_metadata) as reader:
                body = reader.read()
            compression_algorithm = metadata_object[object_name]['compression']
            decompressor = self._get_compressor(compression_algorithm)
            if decompressor is not None:
                LOG.debug('decompressing data using %s algorithm',
                          compression_algorithm)
                decompressed = decompressor.decompress(body)
                body = None  # Allow Python to free it
                _write_volume(volume_is_new,
                              volume_file, obj['offset'], decompressed)
                decompressed = None  # Allow Python to free it
            else:
                _write_volume(volume_is_new,
                              volume_file, obj['offset'], body)
                body = None  # Allow Python to free it

            # force flush every write to avoid long blocking write on close
            volume_file.flush()

            # Be tolerant to IO implementations that do not support fileno()
            try:
                fileno = volume_file.fileno()
            except IOError:
                LOG.debug("volume_file does not support fileno() so skipping "
                          "fsync()")
            else:
                os.fsync(fileno)

            # Restoring a backup to a volume can take some time. Yield so other
            # threads can run, allowing for among other things the service
            # status to be updated
            eventlet.sleep(0)
        LOG.debug('v1 volume backup restore of %s finished.',
                  backup_id)

    def restore(self, backup, volume_id, volume_file, volume_is_new):
        """Restore the given volume backup from backup repository.

        Raises BackupRestoreCancel on any backup status change.
        """
        backup_id = backup['id']
        container = backup['container']
        object_prefix = backup['service_metadata']
        LOG.debug('starting restore of backup %(object_prefix)s '
                  'container: %(container)s, '
                  'to %(new)s volume %(volume_id)s, '
                  'backup: %(backup_id)s.',
                  {
                      'object_prefix': object_prefix,
                      'container': container,
                      'volume_id': volume_id,
                      'backup_id': backup_id,
                      'new': 'new' if volume_is_new else 'existing',
                  })
        metadata = self._read_metadata(backup)
        metadata_version = metadata['version']
        LOG.debug('Restoring backup version %s', metadata_version)
        try:
            restore_func = getattr(self, self.DRIVER_VERSION_MAPPING.get(
                metadata_version))
        except TypeError:
            err = (_('No support to restore backup version %s')
                   % metadata_version)
            raise exception.InvalidBackup(reason=err)

        # Build a list of backups based on parent_id. A full backup
        # will be the last one in the list.
        backup_list = []
        backup_list.append(backup)
        current_backup = backup
        while current_backup.parent_id:
            prev_backup = objects.Backup.get_by_id(self.context,
                                                   current_backup.parent_id)
            backup_list.append(prev_backup)
            current_backup = prev_backup

        # Do a full restore first, then layer the incremental backups
        # on top of it in order.
        index = len(backup_list) - 1
        while index >= 0:
            backup1 = backup_list[index]
            index = index - 1
            metadata = self._read_metadata(backup1)
            restore_func(backup1, volume_id, metadata, volume_file,
                         volume_is_new, backup)

            volume_meta = metadata.get('volume_meta', None)
            try:
                if volume_meta:
                    self.put_metadata(volume_id, volume_meta)
                else:
                    LOG.debug("No volume metadata in this backup.")
            except exception.BackupMetadataUnsupportedVersion:
                msg = _("Metadata restore failed due to incompatible version.")
                LOG.error(msg)
                raise exception.BackupOperationError(msg)

        LOG.debug('restore %(backup_id)s to %(volume_id)s finished.',
                  {'backup_id': backup_id, 'volume_id': volume_id})

    def delete_backup(self, backup):
        """Delete the given backup."""
        container = backup['container']
        object_prefix = backup['service_metadata']
        LOG.debug('delete started, backup: %(id)s, container: %(cont)s, '
                  'prefix: %(pre)s.',
                  {'id': backup['id'],
                   'cont': container,
                   'pre': object_prefix})

        if container is not None and object_prefix is not None:
            object_names = []
            try:
                object_names = self._generate_object_names(backup)
            except Exception:
                LOG.warning('Error while listing objects, continuing'
                            ' with delete.')

            for object_name in object_names:
                self.delete_object(container, object_name)
                LOG.debug('deleted object: %(object_name)s'
                          ' in container: %(container)s.',
                          {
                              'object_name': object_name,
                              'container': container
                          })
                # Deleting a backup's objects can take some time.
                # Yield so other threads can run
                eventlet.sleep(0)

        LOG.debug('delete %s finished.', backup['id'])
