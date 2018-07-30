import io

from django.test import TestCase, override_settings
from django.utils import timezone

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
        expected_name = "some_blob_Ϊ.txt"
        self.assertFalse(self.storage.exists(expected_name))
        stream = io.BytesIO(b'Im a stream')
        name = self.storage.save('some blob Ϊ.txt', stream)
        self.assertEqual(name, expected_name)
        self.assertTrue(self.storage.exists(expected_name))

    def test_delete(self):
        self.storage.location = 'path'
        expected_name = "some_blob_Ϊ.txt"
        self.assertFalse(self.storage.exists(expected_name))
        stream = io.BytesIO(b'Im a stream')
        name = self.storage.save('some blob Ϊ.txt', stream)
        self.assertEqual(name, expected_name)
        self.assertTrue(self.storage.exists(expected_name))
        self.storage.delete(expected_name)
        self.assertFalse(self.storage.exists(expected_name))

    def test_size(self):
        self.storage.location = 'path'
        expected_name = "some_path/some_blob_Ϊ.txt"
        self.assertFalse(self.storage.exists(expected_name))
        stream = io.BytesIO(b'Im a stream')
        name = self.storage.save('some path/some blob Ϊ.txt', stream)
        self.assertEqual(name, expected_name)
        self.assertTrue(self.storage.exists(expected_name))
        self.assertEqual(self.storage.size(expected_name), len(b'Im a stream'))

    def test_url(self):
        self.assertTrue(
            self.storage.url("my_file.txt").endswith("/test/my_file.txt"))
        self.storage.expiration_secs = 360
        # has some query-string
        self.assertTrue("/test/my_file.txt?" in self.storage.url("my_file.txt"))

    @override_settings(USE_TZ=True)
    def test_get_modified_time_tz(self):
        stream = io.BytesIO(b'Im a stream')
        name = self.storage.save('some path/some blob Ϊ.txt', stream)
        self.assertTrue(timezone.is_aware(self.storage.get_modified_time(name)))

    @override_settings(USE_TZ=False)
    def test_get_modified_time_no_tz(self):
        stream = io.BytesIO(b'Im a stream')
        name = self.storage.save('some path/some blob Ϊ.txt', stream)
        self.assertTrue(timezone.is_naive(self.storage.get_modified_time(name)))

    @override_settings(USE_TZ=True)
    def test_modified_time_tz(self):
        stream = io.BytesIO(b'Im a stream')
        name = self.storage.save('some path/some blob Ϊ.txt', stream)
        self.assertTrue(timezone.is_naive(self.storage.modified_time(name)))

    @override_settings(USE_TZ=False)
    def test_modified_time_no_tz(self):
        stream = io.BytesIO(b'Im a stream')
        name = self.storage.save('some path/some blob Ϊ.txt', stream)
        self.assertTrue(timezone.is_naive(self.storage.modified_time(name)))
