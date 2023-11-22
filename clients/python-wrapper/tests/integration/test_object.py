from typing import get_args

import pytest

from tests.integration.conftest import expect_exception_context
from lakefs.exceptions import ObjectExistsException, InvalidRangeException
from lakefs.object import WriteableObject, WriteModes, OpenModes


def test_object_read_seek(setup_repo):
    clt, repo = setup_repo
    data = "test_data"
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).create(
        data=data)

    with obj.open() as fd:
        assert fd.read(2 * len(data)) == data
        fd.seek(2)

        assert fd.read(5) == data[2:7]

        assert fd.read() == data[7:]

        # This should raise an exception
        with expect_exception_context(InvalidRangeException):
            fd.read(1)

        fd.seek(0)
        for c in data:
            assert fd.read(1) == c
        # This should raise an exception
        with expect_exception_context(InvalidRangeException):
            fd.read(1)


def test_object_create_exists(setup_repo):
    clt, repo = setup_repo
    data = "test_data"
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).create(
        data=data)
    with expect_exception_context(ObjectExistsException):
        obj.create(data="some_other_data", mode='xb')

    with obj.open() as fd:
        assert fd.read() == data

    # Create - overwrite
    new_data = "new_data"
    obj2 = obj.create(data=new_data, mode='w')

    with obj.open() as fd:
        assert fd.read() == new_data

    assert obj2 == obj


@pytest.mark.parametrize("w_mode", get_args(WriteModes))
@pytest.mark.parametrize("pre_sign", (True, False))
@pytest.mark.parametrize("r_mode", get_args(OpenModes))
def test_object_create_read_different_params(setup_repo, w_mode, pre_sign, r_mode):
    clt, repo = setup_repo
    data = b'test \xcf\x84o\xcf\x81\xce\xbdo\xcf\x82'
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).create(
        data=data, mode=w_mode, pre_sign=pre_sign)

    with obj.open(mode=r_mode) as fd:
        res = fd.read()
        if 'b' in w_mode and 'b' in r_mode:
            assert res == data
        elif 'b' in w_mode and 'b' not in r_mode:
            assert res == data.decode('utf-8')
        elif 'b' not in r_mode:
            assert res.encode('utf-8') == data
        else:
            assert res == data


def test_object_copy(setup_repo):
    clt, repo = setup_repo
    data = "test_data"
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).create(
        data=data, metadata={"foo": "bar"})

    copy_name = "copy_obj"
    copy = obj.copy("main", copy_name)
    obj_stat = obj.stat()
    copy_stat = copy.stat()

    assert copy != obj
    assert copy_stat.metadata == obj_stat.metadata
    assert copy_stat.path != obj_stat.path
    assert copy_stat.content_type == obj_stat.content_type
    assert copy_stat.physical_address != obj_stat.physical_address
    assert copy_stat.mtime >= obj_stat.mtime
    assert copy_stat.size_bytes == obj_stat.size_bytes
    assert copy_stat.checksum == obj_stat.checksum
