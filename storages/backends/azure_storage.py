import mimetypes
import os.path
import time
from datetime import datetime, timedelta
from time import mktime

from django.core.exceptions import ImproperlyConfigured
from django.core.files.storage import Storage
from django.utils.deconstruct import deconstructible

try:
    import azure  # noqa
except ImportError:
    raise ImproperlyConfigured(
        "Could not load Azure bindings. "
        "See https://github.com/WindowsAzure/azure-sdk-for-python")

try:
    # azure-storage 0.20.0
    from azure.storage.blob.blobservice import BlobService
    from azure.common import AzureMissingResourceHttpError
except ImportError:
    from azure.storage import BlobService
    from azure import WindowsAzureMissingResourceError as AzureMissingResourceHttpError
from azure.storage import AccessPolicy
from azure.storage import CloudStorageAccount
from storages.utils import setting
from tempfile import SpooledTemporaryFile
from django.core.files.base import File


def clean_name(name):
    return os.path.normpath(name).replace("\\", "/")


@deconstructible
class AzureStorageFile(File):

    def __init__(self, name, mode, storage):
        self._name = name
        self._mode = mode
        self._storage = storage
        self._is_dirty = False
        self._file = None

    def _get_file(self):
        if self._file is None:
            self._file = SpooledTemporaryFile(
                max_size=self._storage.max_memory_size,
                suffix=".AzureBoto3StorageFile",
                dir=setting("FILE_UPLOAD_TEMP_DIR", None)
            )
            if 'r' in self._mode:
                self._is_dirty = False
                self._storage.connection.get_blob_to_stream(container_name=self._storage.azure_container,
                                                            blob_name=self._name, stream=self._file,
                                                            max_connections=setting("AZURE_MAX_CONNECTIONS", 2))
                self._file.seek(0)
        return self._file

    file = property(_get_file)

    def read(self, *args, **kwargs):
        if 'r' not in self._mode:
            raise AttributeError("File was not opened in read mode.")
        return super(AzureStorageFile, self).read(*args, **kwargs)


@deconstructible
class AzureStorage(Storage):
    account_name = setting("AZURE_ACCOUNT_NAME")
    account_key = setting("AZURE_ACCOUNT_KEY")
    azure_container = setting("AZURE_CONTAINER")
    azure_ssl = setting("AZURE_SSL")
    max_memory_size = setting('AZURE_BLOB_MAX_MEMORY_SIZE', 0)

    def __init__(self, *args, **kwargs):
        super(AzureStorage, self).__init__(*args, **kwargs)
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

    def __get_blob_properties(self, name):
        try:
            return self.connection.get_blob_properties(
                self.azure_container,
                name
            )
        except AzureMissingResourceHttpError:
            return None

    def _open(self, name, mode="rb"):
        return AzureStorageFile(name, mode, self)

    def exists(self, name):
        return self.connection.exists(name)

    def delete(self, name):
        try:
            self.connection.delete_blob(self.azure_container, name)
        except AzureMissingResourceHttpError:
            pass

    def size(self, name):
        properties = self.connection.get_blob_properties(
            self.azure_container, name)
        return properties["content-length"]

    def _save(self, name, content):
        if hasattr(content.file, 'content_type'):
            content_type = content.file.content_type
        else:
            content_type = mimetypes.guess_type(name)[0]

        if hasattr(content, 'chunks'):
            content_data = b''.join(chunk for chunk in content.chunks())
        else:
            content_data = content.read()

        self.connection.put_blob(self.azure_container, name,
                                 content_data, "BlockBlob",
                                 x_ms_blob_content_type=content_type)
        return name

    def url(self, name, expire=None, mode='r'):
        if hasattr(self.connection, 'make_blob_url'):
            sas_token = None

            if expire:
                today = datetime.utcnow()
                today_plus_delta = today + timedelta(seconds=expire)
                today_plus_delta = today_plus_delta.replace(microsecond=0).isoformat() + 'Z'
                sas_token = self.connection.generate_shared_access_signature(self.azure_container, name,
                                                                             SharedAccessPolicy(
                                                                                AccessPolicy(permission=mode,
                                                                                             expiry=today_plus_delta),
                                                                                None))
            return self.connection.make_blob_url(
                container_name=self.azure_container,
                blob_name=name,
                protocol=self.azure_protocol,
                sas_token=sas_token
            )
        else:
            return "{}{}/{}".format(setting('MEDIA_URL'), self.azure_container, name)

    def modified_time(self, name):
        try:
            modified = self.__get_blob_properties(name)['last-modified']
        except (TypeError, KeyError):
            return super(AzureStorage, self).modified_time(name)

        modified = time.strptime(modified, '%a, %d %b %Y %H:%M:%S %Z')
        modified = datetime.fromtimestamp(mktime(modified))

        return modified
