
try:
    from unittest import mock
except ImportError:  # Python 3.2 and below
    import mock
import datetime
from datetime import timezone, timedelta
from gzip import GzipFile

from azure.storage.blob import ContentSettings, BlobPermissions
from azure.storage.blob import BlobProperties, Blob, BlobBlock


from django.test import TestCase
from django.core.files.base import ContentFile
from django.utils.encoding import force_bytes

from storages.backends import azure_storage


class AzureStorageTest(TestCase):

    def setUp(self, *args):
        self.storage = azure_storage.AzureStorage()
        self.storage._connection = mock.MagicMock()
        self.container_name = 'test'
        self.storage.azure_container = self.container_name

    def test_get_valid_path(self):
        self.assertEqual(
            self.storage._get_valid_path("path/to/somewhere"),
            "path/to/somewhere")
        self.assertEqual(
            self.storage._get_valid_path("path/to/../somewhere"),
            "path/somewhere")
        self.assertEqual(
            self.storage._get_valid_path("path/to/../"), "path")
        self.assertEqual(
            self.storage._get_valid_path("path\\to\\..\\"), "path")
        self.assertEqual(
            self.storage._get_valid_path("path/name/"), "path/name")
        self.assertEqual(
            self.storage._get_valid_path("path\\to\\somewhere"),
            "path/to/somewhere")
        self.assertEqual(
            self.storage._get_valid_path("some/$/path"), "some/path")
        self.assertEqual(
            self.storage._get_valid_path("/$/path"), "path")
        self.assertEqual(
            self.storage._get_valid_path("path/$/"), "path")
        self.assertEqual(
            self.storage._get_valid_path("some///path"), "some/path")
        self.assertEqual(
            self.storage._get_valid_path("some//path"), "some/path")
        self.assertEqual(
            self.storage._get_valid_path("some\\\\path"), "some/path")
        self.assertEqual(
            self.storage._get_valid_path("a" * 1024), "a" * 1024)
        self.assertEqual(
            self.storage._get_valid_path("a/a" * 256), "a/a" * 256)
        self.assertRaises(ValueError, self.storage._get_valid_path, "")
        self.assertRaises(ValueError, self.storage._get_valid_path, "/")
        self.assertRaises(ValueError, self.storage._get_valid_path, "/../")
        self.assertRaises(ValueError, self.storage._get_valid_path, "..")
        self.assertRaises(ValueError, self.storage._get_valid_path, "///")
        self.assertRaises(ValueError, self.storage._get_valid_path, "!!!")
        self.assertRaises(ValueError, self.storage._get_valid_path, "a" * 1025)
        self.assertRaises(ValueError, self.storage._get_valid_path, "a/a" * 257)

    def test_get_valid_path_idempotency(self):
        self.assertEqual(
            self.storage._get_valid_path("//$//a//$//"), "a")
        self.assertEqual(
            self.storage._get_valid_path(
                self.storage._get_valid_path("//$//a//$//")),
            self.storage._get_valid_path("//$//a//$//"))
        self.assertEqual(
            self.storage._get_valid_path("some path/some long name & then some.txt"),
            "some_path/some_long_name__then_some.txt")
        self.assertEqual(
            self.storage._get_valid_path(
                self.storage._get_valid_path("some path/some long name & then some.txt")),
            self.storage._get_valid_path("some path/some long name & then some.txt"))

    def test_get_available_name(self):
        self.storage.overwrite_files = False
        self.storage._connection.exists.side_effect = [True, False]
        name = self.storage.get_available_name('foo.txt')
        self.assertTrue(name.startswith('foo_'))
        self.assertTrue(name.endswith('.txt'))
        self.assertTrue(len(name) > len('foo.txt'))
        self.assertEqual(self.storage._connection.exists.call_count, 2)

    def test_get_available_name_first(self):
        self.storage.overwrite_files = False
        self.storage._connection.exists.return_value = False
        self.assertEqual(
            self.storage.get_available_name('foo bar baz.txt'),
            'foo_bar_baz.txt')
        self.assertEqual(self.storage._connection.exists.call_count, 1)

    def test_get_available_name_max_len(self):
        self.storage.overwrite_files = False
        # if you wonder why this is, file-system
        # storage will raise when file name is too long as well,
        # the form should validate this
        self.assertRaises(ValueError, self.storage.get_available_name, 'a' * 1025)
        self.storage._connection.exists.side_effect = [True, False]
        name = self.storage.get_available_name('a' * 1000, max_length=100)  # max_len == 1024
        self.assertEqual(len(name), 100)
        self.assertTrue('_' in name)
        self.assertEqual(self.storage._connection.exists.call_count, 2)

    def test_get_available_invalid(self):
        self.storage.overwrite_files = False
        self.storage._connection.exists.return_value = False
        self.assertRaises(ValueError, self.storage.get_available_name, "")
        self.assertRaises(ValueError, self.storage.get_available_name, "$$")

    def test_url(self):
        self.storage._connection.make_blob_url.return_value = 'ret_foo'
        self.assertEqual(self.storage.url('some blob'), 'ret_foo')
        self.storage._connection.make_blob_url.assert_called_once_with(
            container_name=self.container_name,
            blob_name='some_blob')

    def test_url_expire(self):
        fixed_time = datetime.datetime(2016, 11, 6, 4, tzinfo=timezone.utc)
        self.storage._connection.generate_blob_shared_access_signature.return_value = 'foo_token'
        self.storage._connection.make_blob_url.return_value = 'ret_foo'
        with mock.patch('storages.backends.azure_storage.datetime') as d_mocked:
            d_mocked.utcnow.return_value = fixed_time
            self.assertEqual(self.storage.url('some blob', 100), 'ret_foo')
            self.storage._connection.generate_blob_shared_access_signature.assert_called_once_with(
                self.container_name,
                'some_blob',
                BlobPermissions.READ,
                expiry=fixed_time + timedelta(seconds=100))
            self.storage._connection.make_blob_url.assert_called_once_with(
                container_name=self.container_name,
                blob_name='some_blob',
                sas_token='foo_token')

    # From boto3

    def test_storage_save(self):
        """
        Test saving a file
        """
        name = 'test storage save.txt'
        content = ContentFile('new content')
        with mock.patch('storages.backends.azure_storage.ContentSettings') as c_mocked:
            c_mocked.return_value = 'content_settings_foo'
            self.assertEqual(self.storage.save(name, content), 'test_storage_save.txt')
            self.storage._connection.create_blob_from_stream.assert_called_once_with(
                container_name=self.container_name,
                blob_name='test_storage_save.txt',
                stream=content.file,
                content_settings='content_settings_foo')
            c_mocked.assert_called_once_with(
                content_type='text/plain',
                content_encoding=None)

    def test_storage_save_gzipped(self):
        """
        Test saving a gzipped file
        """
        name = 'test_storage_save.gz'
        content = ContentFile("I am gzip'd")
        self.storage.gzip = False
        with mock.patch('storages.backends.azure_storage.ContentSettings') as c_mocked:
            c_mocked.return_value = 'content_settings_foo'
            self.assertEqual(self.storage.save(name, content), 'test_storage_save.gz')
            self.storage._connection.create_blob_from_stream.assert_called_once_with(
                container_name=self.container_name,
                blob_name='test_storage_save.gz',
                stream=content.file,
                content_settings='content_settings_foo')
            c_mocked.assert_called_once_with(
                content_type='application/octet-stream',
                content_encoding='gzip')

    def test_storage_save_gzipped_enaled(self):
        """
        Test saving a gzipped file
        """
        name = 'test_storage_save.css'
        content = ContentFile("I should be gzip'd")
        self.storage.gzip = True
        with mock.patch('storages.backends.azure_storage.ContentSettings') as c_mocked:
            c_mocked.return_value = 'content_settings_foo'
            self.assertEqual(self.storage.save(name, content), name)
            self.storage._connection.create_blob_from_stream.assert_called_once_with(
                container_name=self.container_name,
                blob_name=name,
                stream=mock.ANY,
                content_settings='content_settings_foo')
            c_mocked.assert_called_once_with(
                content_type='application/octet-stream',
                content_encoding='gzip')
        _args, kwargs = self.storage._connection.create_blob_from_stream.call_args
        zfile = GzipFile(mode='rb', fileobj=kwargs['stream'])
        self.assertEqual(zfile.read(), b"I should be gzip'd")

    def test_compress_content_len(self):
        """
        Test that file returned by _compress_content() is readable.
        """
        self.storage.gzip = True
        content = ContentFile("I should be gzip'd")
        content = self.storage._compress_content(content)
        self.assertTrue(len(content.read()) > 0)

    def test_storage_open_write(self):
        """
        Test opening a file in write mode
        """
        name = 'test_open_for_writïng.txt'
        content = 'new content'

        file = self.storage.open(name, 'w')
        self.storage._connection._put_blob.assert_called_once_with(
            self.container_name, name, None)

        file.write(content)
        written_file = file.file
        file.close()
        self.storage._connection.create_blob_from_stream.assert_called_once_with(
            container_name=self.container_name,
            blob_name=name,
            stream=written_file,
            content_settings=mock.ANY)

    def test_storage_exists(self):
        self.storage._connection.exists.return_value = True
        blob_name = "blob"
        self.assertTrue(self.storage.exists(blob_name))
        self.storage._connection.exists.assert_called_once_with(
            self.container_name, blob_name)

    def test_delete_blob(self):
        self.storage.delete("name")
        self.storage._connection.delete_blob.assert_called_once_with(
            container_name=self.container_name,
            blob_name="name")

    def test_storage_listdir_base(self):
        file_names = ["some/path/1.txt", "2.txt", "other/path/3.txt", "4.txt"]

        result = []
        for p in file_names:
            obj = mock.MagicMock()
            obj.name = p
            result.append(obj)
        self.storage._connection.list_blobs.return_value = iter(result)

        dirs, files = self.storage.listdir("")
        self.storage._connection.list_blobs.assert_called_with(
            self.container_name, prefix="")

        self.assertEqual(len(dirs), 2)
        for directory in ["some", "other"]:
            self.assertTrue(
                directory in dirs,
                """ "%s" not in directory list "%s".""" % (directory, dirs))

        self.assertEqual(len(files), 2)
        for filename in ["2.txt", "4.txt"]:
            self.assertTrue(
                filename in files,
                """ "%s" not in file list "%s".""" % (filename, files))

    def test_storage_listdir_subdir(self):
        file_names = ["some/path/1.txt", "some/2.txt"]

        result = []
        for p in file_names:
            obj = mock.MagicMock()
            obj.name = p
            result.append(obj)
        self.storage._connection.list_blobs.return_value = iter(result)

        dirs, files = self.storage.listdir("some/")
        self.storage._connection.list_blobs.assert_called_with(
            self.container_name, prefix="some/")

        self.assertEqual(len(dirs), 1)
        self.assertTrue(
            'path' in dirs,
            """ "path" not in directory list "%s".""" % (dirs,))

        self.assertEqual(len(files), 1)
        self.assertTrue(
            '2.txt' in files,
            """ "2.txt" not in files list "%s".""" % (files,))

"""
    def test_blob_open_read(self):
        mocked_binary = b"mocked test"
        blob_name = "blob_name"
        sent_kwargs = {}

        def mocked_stream(*args, **kwargs):
            stream = kwargs['stream']
            stream.write(mocked_binary)
            sent_kwargs.update(kwargs)
            assert kwargs['max_connections'] == 1

        self.storage.connection.get_blob_to_stream.side_effect = mocked_stream
        with self.storage.open(blob_name, "rb") as f:
            content = f.read()
        self.assertEqual(mocked_binary, content)
        # I am doing this trick here to validate that the method was called, I couldn't use it with
        # the known parameter since a stream is an internal object that I don't have access to
        self.storage.connection.get_blob_to_stream.assert_called_once_with(**sent_kwargs)


    def test_blob_open_text_write(self):
        mocked_text = "written text"

        with self.storage.open("name", "w") as f:
            f.write(mocked_text)
        self.storage.connection._put_blob.assert_called_once_with(self.container_name, "name", None)
        self.storage.connection.put_block.assert_called_once_with(self.container_name,
                                                                  "name", force_bytes(mocked_text),
                                                                  'MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwbmFtZTE%3D')
        put_block_list_call_list = self.storage.connection.put_block_list.call_args_list
        self.assertEqual(1, len(put_block_list_call_list))
        put_block_args = put_block_list_call_list[0]
        self.assertEqual(self.container_name, put_block_args[0][0])
        self.assertEqual("name", put_block_args[0][1])
        self.assertEqual(1, len(put_block_args[0][2]))
        self.assertIsInstance(put_block_args[0][2][0], BlobBlock)

    def test_blob_open_text_write_3_times(self):
        content1 = "content1"
        content2 = "content2"
        content3 = "content3"

        with self.storage.open("name", "w") as f:
            f.write(content1)
            f.write(content2)
            f.write(content3)
        self.storage.connection._put_blob.assert_called_once_with(self.container_name, "name", None)
        self.storage.connection.put_block.assert_called_once_with(self.container_name,
                                                                  "name", force_bytes(content1+content2+content3),
                                                                  'MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwbmFtZTE%3D')
        put_block_list_call_list = self.storage.connection.put_block_list.call_args_list
        self.assertEqual(1, len(put_block_list_call_list))
        put_block_args = put_block_list_call_list[0]
        self.assertEqual(self.container_name, put_block_args[0][0])
        self.assertEqual("name", put_block_args[0][1])
        self.assertEqual(1, len(put_block_args[0][2]))
        self.assertIsInstance(put_block_args[0][2][0], BlobBlock)

    def test_blob_open_text_write_3_times_small_buffer_size(self):
        contents = ["cont", "content2", "content3"]
        self.storage.buffer_size = 2

        with self.storage.open("name", "w") as f:
            for content in contents:
                f.write(content)
        self.storage.connection._put_blob.assert_called_once_with(self.container_name, "name", None)
        put_block_args_list = self.storage.connection.put_block.call_args_list
        self.assertEqual(11, len(put_block_args_list))
        actual_content = bytes()
        for idx, args in enumerate(put_block_args_list):
            self.assertEqual(self.container_name, args[0][0])
            self.assertEqual("name", args[0][1])
            self.assertLessEqual(len(args[0][2]), self.storage.buffer_size)
            actual_content = actual_content + (args[0][2])
        self.assertEqual(force_bytes("".join(contents)), actual_content)
        put_block_list_call_list = self.storage.connection.put_block_list.call_args_list
        self.assertEqual(1, len(put_block_list_call_list))
        put_block_args = put_block_list_call_list[0]
        self.assertEqual(self.container_name, put_block_args[0][0])
        self.assertEqual("name", put_block_args[0][1])
        self.assertEqual(11, len(put_block_args[0][2]))
        for blob_block in put_block_args[0][2]:
            self.assertIsInstance(blob_block, BlobBlock)



    def test_size_of_file(self):
        props = BlobProperties()
        props.content_length = 12
        self.storage.connection.get_blob_properties.return_value = Blob(props=props)
        size = self.storage.size("name")
        self.assertEqual(12, size)

    def test_last_modfied_of_file(self):
        props = BlobProperties()
        accepted_time = datetime.datetime(2017, 5, 11, 8, 52, 4,)
        props.last_modified = accepted_time
        self.storage.connection.get_blob_properties.return_value = Blob(props=props)
        time = self.storage.modified_time("name")
        self.assertEqual(accepted_time, time)

    def test_url_blob(self):
        sas_token = "token"
        url = "url"
        blob = "blob"
        self.storage.connection.generate_blob_shared_access_signature.return_value = sas_token
        self.storage.connection.make_blob_url.return_value = url
        actual_url = self.storage.url(blob)
        self.assertEqual(url, actual_url)
        self.storage.connection.generate_blob_shared_access_signature.assert_not_called()
        self.storage.connection.make_blob_url.assert_called_once_with(blob_name=blob,
                                                                      container_name=self.container_name)

    def test_url_blob_with_expiry(self):
        sas_token = "token"
        url = "url"
        blob = "blob"
        self.storage.connection.generate_blob_shared_access_signature.return_value = sas_token
        self.storage.connection.make_blob_url.return_value = url
        self.storage._expire_at = mock.MagicMock(return_value=("now", 'expires_at'))
        actual_url = self.storage.url(blob, expire=30)
        self.assertEqual(url, actual_url)
        self.storage.connection.generate_blob_shared_access_signature.assert_called_once_with(self.container_name,
                                                                                              blob,
                                                                                              'r',
                                                                                              expiry='expires_at')
        self.storage.connection.make_blob_url.assert_called_once_with(blob_name=blob,
                                                                      container_name=self.container_name,
                                                                      sas_token=sas_token)

    def test_expires_at(self):
        expected_now = datetime.datetime.utcnow()
        now, now_plus_delta = self.storage._expire_at(expire=30)
        expected_now_plus_delta = now + datetime.timedelta(seconds=30)
        expected_now_plus_delta = expected_now_plus_delta.replace(microsecond=0).isoformat() + 'Z'
        self.assertEqual(expected_now_plus_delta, now_plus_delta)
        self.assertLess(now - expected_now, datetime.timedelta(seconds=1))

    def test_save(self):
        sent_kwargs = {}
        f = ContentFile("content")

        def validate_create_blob_from_stream(*args, **kwargs):
            sent_kwargs.update(kwargs)
            content_settings = kwargs['content_settings']
            assert content_settings.content_type == 'text/plain'
            content = kwargs['stream']
            assert content == f

        self.storage.connection.create_blob_from_stream.side_effect = validate_create_blob_from_stream
        self.storage._save("bla.txt", f)
        self.storage.connection.create_blob_from_stream.assert_called_once_with(**sent_kwargs)
"""
