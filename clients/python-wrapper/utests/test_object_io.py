import requests
from contextlib import contextmanager
import lakefs_sdk.api

from utests.test_client import lakectl_test_config_context


class ObjectTestKWargs:
    def __init__(self) -> None:
        self.repository = "test_repo"
        self.reference = "test_reference"
        self.path = "test_path"


class StorageTestConfig(lakefs_sdk.StorageConfig):

    def __init__(self) -> None:
        super().__init__(blockstore_type="s3",
                         blockstore_namespace_example="",
                         blockstore_namespace_ValidityRegex="",
                         pre_sign_support=True,
                         pre_sign_support_ui=False,
                         import_support=False,
                         import_validity_regex="")


class ObjectTestStats(lakefs_sdk.ObjectStats):
    def __init__(self) -> None:
        super().__init__(path="",
                         path_type="object",
                         physical_address="",
                         checksum="",
                         mtime=0)


class StagingTestLocation(lakefs_sdk.StagingLocation):
    def __init__(self) -> None:
        super().__init__(physical_address="physical_address")


@contextmanager
def readable_object_context(monkey, tmp_path, **kwargs):
    with lakectl_test_config_context(monkey, tmp_path) as client:
        from lakefs.object_io import ReadableObject
        conf = lakefs_sdk.Config(version_config=lakefs_sdk.VersionConfig(), storage_config=StorageTestConfig())
        monkey.setattr(client.DefaultClient, "_server_conf", conf)
        read_obj = ReadableObject(client=client.DefaultClient, **kwargs)
        yield read_obj


@contextmanager
def writeable_object_context(monkey, tmp_path, **kwargs):
    with lakectl_test_config_context(monkey, tmp_path) as client:
        monkey.setattr(lakefs_sdk.api.BranchesApi, "get_branch", lambda *args: None)
        from lakefs.object_io import WriteableObject
        conf = lakefs_sdk.Config(version_config=lakefs_sdk.VersionConfig(), storage_config=StorageTestConfig())
        monkey.setattr(client.DefaultClient, "_server_conf", conf)
        obj = WriteableObject(client=client.DefaultClient, **kwargs)
        yield obj


class TestReadableObject:
    def test_seek(self, monkeypatch, tmp_path):
        test_kwargs = ObjectTestKWargs()
        with readable_object_context(monkeypatch, tmp_path, **test_kwargs.__dict__) as obj:
            assert obj.pos == 0
            obj.seek(30)
            assert obj.pos == 30
            try:
                obj.seek(-1)
                assert 0, "expected ValueError exception"
            except ValueError:
                pass

    def test_read(self, monkeypatch, tmp_path):
        test_kwargs = ObjectTestKWargs()
        with readable_object_context(monkeypatch, tmp_path, **test_kwargs.__dict__) as obj:
            object_stats = ObjectTestStats()
            object_stats.path = test_kwargs.path
            object_stats.size_bytes = 3000
            monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "stat_object", lambda *args: object_stats)

            # read negative
            try:
                obj.read(-1)
                assert 0, "Expected ValueError"
            except ValueError:
                pass

            # Read whole file
            start_pos = 0
            end_pos = object_stats.size_bytes - 1

            def monkey_get_object(_, repository, ref, path, range, presign, **kwargs):  # pylint: disable=W0622
                assert repository == test_kwargs.repository
                assert ref == test_kwargs.reference
                assert path == test_kwargs.path
                assert range == f"bytes={start_pos}-{end_pos}"
                assert presign
                return b"test"

            monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "get_object", monkey_get_object)
            obj.read()
            assert obj.pos == start_pos + object_stats.size_bytes

            # Test reading from middle
            start_pos = 132
            obj.seek(start_pos)
            read_size = 456
            end_pos = start_pos + read_size - 1
            obj.read(read_size)
            assert obj.pos == start_pos + read_size

            # Read more than file size
            start_pos = obj.pos
            read_size = 2 * object_stats.size_bytes
            end_pos = object_stats.size_bytes - 1
            obj.read(read_size)
            assert obj.pos == object_stats.size_bytes

            # Read again and expect EOF
            try:
                obj.read()
                assert 0, "Expected EOF error"
            except EOFError:
                pass

    def test_exists(self, monkeypatch, tmp_path):
        test_kwargs = ObjectTestKWargs()
        with readable_object_context(monkeypatch, tmp_path, **test_kwargs.__dict__) as obj:
            # Object exists
            monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "head_object", lambda *args: None)
            assert obj.exists()
            # Object doesn't exist
            monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "head_object",
                                lambda *args: (_ for _ in ()).throw(lakefs_sdk.exceptions.NotFoundException))
            assert not obj.exists()

            # Other exception
            monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "head_object",
                                lambda *args: 1 / 0)
            try:
                obj.exists()
                assert 0, "Exception not raised"
            except ZeroDivisionError:
                pass


class TestWriteableObject:
    def test_create(self, monkeypatch, tmp_path):
        test_kwargs = ObjectTestKWargs()
        with writeable_object_context(monkeypatch, tmp_path, **test_kwargs.__dict__) as obj:
            staging_location = StagingTestLocation()
            monkeypatch.setattr(lakefs_sdk.api.StagingApi, "get_physical_address", lambda *args: staging_location)
            resp = requests.Response()
            resp.status_code = requests.codes.OK
            monkeypatch.setattr(requests.Session, "put", lambda *args, **kwargs: resp)

            def monkey_link_physical_address(*args, staging_metadata: lakefs_sdk.StagingMetadata, **kwargs):
                assert staging_metadata.size_bytes == len(data)
                assert staging_metadata.staging == staging_location

            monkeypatch.setattr(lakefs_sdk.api.StagingApi, "link_physical_address", monkey_link_physical_address)
            # Test string
            data = "test_data"
            obj.create(data=data)
