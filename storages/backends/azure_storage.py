from __future__ import unicode_literals
from datetime import datetime, timedelta
from tempfile import SpooledTemporaryFile
import os.path
import mimetypes
import re

from azure.storage.common import CloudStorageAccount
from azure.common import AzureMissingResourceHttpError
from azure.storage.blob import ContentSettings, BlobPermissions
from storages.utils import setting

from django.core.files.storage import Storage
from django.utils.deconstruct import deconstructible
from django.core.files.base import File
from django.utils.encoding import force_bytes
from django.utils import timezone
from django.utils.encoding import force_text


@deconstructible
class AzureStorageFile(File):

    def __init__(self, name, mode, storage):
        self.name = storage.get_valid_name(name)
        self._mode = mode
        self._storage = storage
        self._is_dirty = False
        self._file = None
        if 'w' in self._mode:
            self._storage.connection._put_blob(
                self._storage.azure_container, self.name, None)
        self._write_counter = 0
        self._block_list = list()
        self._last_commit_pos = 0

    @property
    def file(self):
        if self._file is not None:
            return self._file

        self._file = SpooledTemporaryFile(
            max_size=self._storage.max_memory_size,
            suffix=".AzureStorageFile",
            dir=setting("FILE_UPLOAD_TEMP_DIR", None))
        if 'r' in self._mode:
            # I set max connection to 1 since spooledtempfile is not seekable which is required if we use
            # max_conection > 1
            self._storage.connection.get_blob_to_stream(
                container_name=self._storage.azure_container,
                blob_name=self.name,
                stream=self._file,
                max_connections=1)
            self._file.seek(0)
        return self._file

    def read(self, *args, **kwargs):
        if 'r' not in self._mode:
            raise AttributeError("File was not opened in read mode.")
        return super(AzureStorageFile, self).read(*args, **kwargs)

    def write(self, content):
        if len(content) > 100*1024*1024:
            raise ValueError("Max chunk size is 100MB")
        if 'w' not in self._mode:
            raise AttributeError("File was not opened in write mode.")
        self._is_dirty = True
        return super(AzureStorageFile, self).write(force_bytes(content))

    def close(self):
        if self._file is None:
            return
        if self._is_dirty:
            self._storage.save(self.name, self._file)
            self._is_dirty = False
        self._file.close()
        self._file = None


def _content_type(name, content):
    try:
        return content.file.content_type
    except AttributeError:
        pass
    try:
        return content.content_type
    except AttributeError:
        pass
    return mimetypes.guess_type(name)[0]


def _get_valid_filename(s):
    # A blob name:
    #   * must not end with dot or slash
    #   * can contain any character
    #   * must escape URL reserved characters
    # We allow a subset of this to avoid
    # illegal file names. We must ensure it is idempotent.
    s = force_text(s)
    s = s.replace('\\', '/').replace(' ', '_')
    s = re.sub(r'(?u)[^-_\w./]', '', s)
    s = os.path.normpath(s)
    return s.strip(' ./')


# Max len according to azure's docs
_AZURE_NAME_MAX_LEN = 1024


@deconstructible
class AzureStorage(Storage):

    account_name = setting("AZURE_ACCOUNT_NAME")
    account_key = setting("AZURE_ACCOUNT_KEY")
    azure_container = setting("AZURE_CONTAINER")
    azure_ssl = setting("AZURE_SSL")
    max_memory_size = setting('AZURE_BLOB_MAX_MEMORY_SIZE', 0)
    buffer_size = setting('AZURE_FILE_BUFFER_SIZE', 4194304)
    expiration_secs = setting('AZURE_URL_EXPIRATION_SECS')
    overwrite_files = setting('AZURE_OVERWRITE_FILES', True)

    def __init__(self):
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            account = CloudStorageAccount(self.account_name, self.account_key)
            self._connection = account.create_block_blob_service()
        return self._connection

    @property
    def azure_protocol(self):
        if self.azure_ssl:
            return 'https'
        return 'http' if self.azure_ssl is not None else None

    def _open(self, name, mode="rb"):
        return AzureStorageFile(name, mode, self)

    def get_valid_name(self, name):
        """
        Returns a filename, based on the provided filename, that's suitable for
        use in the target storage system.
        """
        # XXX allow up to 256 `/` slashes
        name = _get_valid_filename(name)
        if len(name) > _AZURE_NAME_MAX_LEN:
            raise ValueError("Blob name max len is %d" % _AZURE_NAME_MAX_LEN)
        if not len(name):
            raise ValueError(
                "A file name of one or more"
                "printable characters is required")
        return name

    def get_available_name(self, name, max_length=_AZURE_NAME_MAX_LEN):
        """
        Returns a filename that's free on the target storage system, and
        available for new content to be written to.
        """
        name = self.get_valid_name(name)
        return super(AzureStorage, self).get_available_name(name, max_length)

    def exists(self, name):
        return self.connection.exists(self.azure_container, name)

    def delete(self, name):
        try:
            self.connection.delete_blob(
                container_name=self.azure_container,
                blob_name=name)
        except AzureMissingResourceHttpError:
            pass

    def size(self, name):
        properties = self.connection.get_blob_properties(
            self.azure_container, name).properties
        return properties.content_length

    def save(self, name, content, content_type=None):
        if self.overwrite_files:
            name = self.get_valid_name(name)
        else:
            name = self.get_available_name(name)

        if content_type is None:
            content_type = _content_type(name, content)

        content_settings = ContentSettings(content_type=content_type)
        self.connection.create_blob_from_stream(
            container_name=self.azure_container,
            blob_name=name,
            stream=content,
            content_settings=content_settings)
        return name

    def _expire_at(self, expire):
        # azure expects time in UTC
        return datetime.utcnow() + timedelta(seconds=expire)

    def url(self, name, expire=None):
        name = self.get_valid_name(name)

        if expire is None:
            expire = self.expiration_secs

        make_blob_url_kwargs = {}
        if expire:
            sas_token = self.connection.generate_blob_shared_access_signature(
                self.azure_container, name, BlobPermissions.READ, expiry=self._expire_at(expire))
            make_blob_url_kwargs['sas_token'] = sas_token

        if self.azure_protocol:
            make_blob_url_kwargs['protocol'] = self.azure_protocol
        return self.connection.make_blob_url(
            container_name=self.azure_container,
            blob_name=name,
            **make_blob_url_kwargs)

    def get_modified_time(self, name):
        """
        Returns an (aware) datetime object containing the last modified time if
        USE_TZ is True, otherwise returns a naive datetime in the local timezone.
        """
        properties = self.connection.get_blob_properties(
            self.azure_container, name).properties
        if setting('USE_TZ'):
            # `last_modified` is in UTC time_zone, we
            # must convert it to settings time_zone
            tz = timezone.get_current_timezone()
            return properties.last_modified.astimezone(tz)
        else:
            return timezone.make_naive(properties.last_modified)

    def modified_time(self, name):
        """Returns a naive datetime object containing the last modified time."""
        mtime = self.get_modified_time(name)
        if timezone.is_naive(mtime):
            return mtime
        return timezone.make_naive(mtime)

    def list_all(self, path=''):
        """Return all files for a given path"""
        return [
            blob.name
            for blob in self.connection.list_blobs(
                self.azure_container, prefix=path)]

    def listdir(self, path=''):
        """
        Return directories and files for a given path.
        Leave the path empty to list the root.
        Order of dirs and files is undefined.
        """
        path = path

        if path and not path.endswith('/'):
            path += '/'
        files = []
        dirs = set()
        for name in self.list_all(path):
            n = name[len(path):]
            if '/' in n:
                dirs.add(n.split('/', 1)[0])
            else:
                files.append(n)
        return list(dirs), files
