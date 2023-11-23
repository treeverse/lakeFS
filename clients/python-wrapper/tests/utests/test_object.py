import http
from contextlib import contextmanager
from typing import get_args
import urllib3

import pytest

from tests.utests.common import get_test_client
import lakefs_sdk.api
from lakefs_sdk.rest import RESTResponse

from lakefs.object import OpenModes


class ObjectTestKWArgs:
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
def readable_object_context(monkey, **kwargs):
    with monkey.context():
        from lakefs.object import StoredObject
        clt = get_test_client()
        conf = lakefs_sdk.Config(version_config=lakefs_sdk.VersionConfig(), storage_config=StorageTestConfig())
        monkey.setattr(clt, "_server_conf", conf)
        read_obj = StoredObject(client=clt, **kwargs)
        yield read_obj


@contextmanager
def writeable_object_context(monkey, **kwargs):
    with monkey.context():
        monkey.setattr(lakefs_sdk.api.BranchesApi, "get_branch", lambda *args: None)
        from lakefs.object import WriteableObject
        conf = lakefs_sdk.Config(version_config=lakefs_sdk.VersionConfig(), storage_config=StorageTestConfig())
        clt = get_test_client()
        monkey.setattr(clt, "_server_conf", conf)
        obj = WriteableObject(client=clt, **kwargs)
        yield obj


class TestStoredObject:
    def test_exists(self, monkeypatch, tmp_path):
        test_kwargs = ObjectTestKWArgs()
        with readable_object_context(monkeypatch, **test_kwargs.__dict__) as obj:
            # Object exists
            monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "head_object", lambda *args: None)
            assert obj.exists()
            # Object doesn't exist
            monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "head_object",
                                lambda *args: (_ for _ in ()).throw(lakefs_sdk.exceptions.NotFoundException(
                                    status=http.HTTPStatus.NOT_FOUND)))
            assert not obj.exists()

            # Other exception
            monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "head_object",
                                lambda *args: 1 / 0)
            try:
                obj.exists()
                assert False, "Exception not raised"
            except ZeroDivisionError:
                pass


class TestObjectReader:
    def test_seek(self, monkeypatch, tmp_path):
        test_kwargs = ObjectTestKWArgs()
        with readable_object_context(monkeypatch, **test_kwargs.__dict__) as obj:
            with obj.open() as fd:
                assert fd.tell() == 0
                fd.seek(30)
                assert fd.tell() == 30
                try:
                    fd.seek(-1)
                    assert False, "expected ValueError exception"
                except OSError:
                    pass

            # Create another reader
            with obj.open() as fd:
                assert fd.tell() == 0

    def test_read(self, monkeypatch, tmp_path):
        test_kwargs = ObjectTestKWArgs()
        with readable_object_context(monkeypatch, **test_kwargs.__dict__) as obj:
            data = b"test \xcf\x84o\xcf\x81\xce\xbdo\xcf\x82\n" * 100
            with obj.open(mode="rb") as fd:
                object_stats = ObjectTestStats()
                object_stats.path = test_kwargs.path
                object_stats.size_bytes = len(data)
                monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "stat_object", lambda *args: object_stats)

                # read negative
                try:
                    fd.read(-1)
                    assert False, "Expected ValueError"
                except OSError:
                    pass

                # Read whole file
                start_pos = 0
                end_pos = ""

                def monkey_get_object(_, repository, ref, path, range, presign, **__):  # pylint: disable=W0622
                    assert repository == test_kwargs.repository
                    assert ref == test_kwargs.reference
                    assert path == test_kwargs.path
                    assert presign

                    if isinstance(end_pos, int):
                        return data[start_pos:end_pos]
                    return data[start_pos:]

                monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "get_object", monkey_get_object)
                assert fd.read() == data
                assert fd.tell() == object_stats.size_bytes

                # Test reading from middle
                start_pos = 132
                fd.seek(start_pos)
                read_size = 456
                end_pos = start_pos + read_size - 1
                fd.read(read_size)
                assert fd.tell() == start_pos + read_size - 1

                # Read more than file size
                start_pos = fd.tell()
                read_size = 2 * object_stats.size_bytes
                end_pos = start_pos + 2 * object_stats.size_bytes - 1
                fd.read(read_size)
                assert fd.tell() == object_stats.size_bytes

    @pytest.mark.parametrize("mode", [*get_args(OpenModes)])
    def test_read_modes(self, monkeypatch, tmp_path, mode):
        test_kwargs = ObjectTestKWArgs()
        data = b"test \xcf\x84o\xcf\x81\xce\xbdo\xcf\x82"
        with readable_object_context(monkeypatch, **test_kwargs.__dict__) as obj:
            with obj.open(mode=mode) as fd:
                object_stats = ObjectTestStats()
                object_stats.path = test_kwargs.path
                object_stats.size_bytes = len(data)
                monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "stat_object", lambda *args: object_stats)

                # Read whole file
                start_pos = 0

        def monkey_get_object(_, repository, ref, path, range, presign, **__):  # pylint: disable=W0622
            assert repository == test_kwargs.repository
            assert ref == test_kwargs.reference
            assert path == test_kwargs.path
            assert range is None
            assert presign
            return b"test \xcf\x84o\xcf\x81\xce\xbdo\xcf\x82"

        monkeypatch.setattr(lakefs_sdk.api.ObjectsApi, "get_object", monkey_get_object)
        res = fd.read()
        if 'b' not in mode:
            assert res == data.decode('utf-8')
        else:
            assert res == data

        assert fd.tell() == start_pos + object_stats.size_bytes

    def test_read_invalid_mode(self, monkeypatch, tmp_path):
        test_kwargs = ObjectTestKWArgs()
        with readable_object_context(monkeypatch, **test_kwargs.__dict__) as obj:
            try:
                with obj.open(mode="invalid"):
                    assert False, "Exception expected"
            except ValueError:
                pass


class TestWriteableObject:
    def test_create(self, monkeypatch, tmp_path):
        test_kwargs = ObjectTestKWArgs()
        with writeable_object_context(monkeypatch, **test_kwargs.__dict__) as obj:
            staging_location = StagingTestLocation()
            monkeypatch.setattr(lakefs_sdk.api.StagingApi, "get_physical_address", lambda *args: staging_location)
            monkeypatch.setattr(obj._client.sdk_client.objects_api.api_client, "request",
                                lambda *args, **kwargs: RESTResponse(urllib3.response.HTTPResponse()))

            def monkey_link_physical_address(*_, staging_metadata: lakefs_sdk.StagingMetadata, **__):
                assert staging_metadata.size_bytes == len(data)
                assert staging_metadata.staging == staging_location
                return lakefs_sdk.ObjectStats(path=obj.path,
                                              path_type="object",
                                              physical_address=staging_location.physical_address,
                                              checksum="",
                                              mtime=12345)

            monkeypatch.setattr(lakefs_sdk.api.StagingApi, "link_physical_address", monkey_link_physical_address)
            # Test string
            data = "test_data"
            obj.create(data=data)

    def test_create_invalid_mode(self, monkeypatch, tmp_path):
        test_kwargs = ObjectTestKWArgs()
        with writeable_object_context(monkeypatch, **test_kwargs.__dict__) as obj:
            try:
                obj.create(data="", mode="invalid")
                assert False, "Exception expected"
            except ValueError:
                pass
