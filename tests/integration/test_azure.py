try:
    from unittest import mock
except ImportError:  # Python 3.2 and below
    import mock

import io
import datetime
from datetime import timezone, timedelta
from gzip import GzipFile

from azure.storage.blob import BlobPermissions
from azure.storage.blob import BlobProperties, Blob

from django.test import TestCase, override_settings
from django.core.files.base import ContentFile

from storages.backends import azure_storage


class AzureStorageTest(TestCase):

    def setUp(self, *args):
        self.storage = azure_storage.AzureStorage()
        self.storage.is_emulated = True
        self.storage.account_name = "XXX"
        self.storage.account_key = "KXXX"
        self.storage.azure_container = "test"
        self.storage.connection.delete_container(
            self.storage.azure_container, fail_not_exist=False)
        self.storage.connection.create_container(
            self.storage.azure_container, public_access=False, fail_on_exist=False)

    def test_save(self):
        stream = io.BytesIO(b'Im a stream')
        name = self.storage.save('some blob Ϊ.txt', stream)
        self.assertEqual(name, "some_blob_Ϊ.txt")
        self.assertTrue(self.storage.exists(name))
