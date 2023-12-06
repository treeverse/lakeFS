import io
from typing import get_args

import pytest

from tests.utests.common import expect_exception_context
from lakefs.exceptions import ObjectExistsException, InvalidRangeException, NotFoundException
from lakefs.object import WriteableObject, WriteModes, ReadModes


@pytest.mark.parametrize("pre_sign", (True, False), indirect=True)
def test_object_read_seek(setup_repo, pre_sign):
    clt, repo = setup_repo
    data = b"test_data"
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).upload(
        data=data, pre_sign=pre_sign)

    with obj.reader() as fd:
        assert fd.read(2 * len(data)) == data
        fd.seek(2)

        assert fd.read(5) == data[2:7]

        assert fd.read() == data[7:]

        # This should raise an exception
        with expect_exception_context(InvalidRangeException):
            fd.read(1)

        fd.seek(0)
        for c in data:
            assert ord(fd.read(1)) == c
        # This should raise an exception
        with expect_exception_context(InvalidRangeException):
            fd.read(1)


def test_object_upload_exists(setup_repo):
    clt, repo = setup_repo
    data = b"test_data"
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).upload(
        data=data)
    with expect_exception_context(ObjectExistsException):
        obj.upload(data="some_other_data", mode='xb')

    with obj.reader() as fd:
        assert fd.read() == data

    # Create - overwrite
    new_data = b"new_data"
    obj2 = obj.upload(data=new_data, mode='w')

    with obj.reader() as fd:
        assert fd.read() == new_data

    assert obj2 == obj


@pytest.mark.parametrize("w_mode", get_args(WriteModes))
@pytest.mark.parametrize("r_mode", get_args(ReadModes))
@pytest.mark.parametrize("pre_sign", (True, False), indirect=True)
def test_object_upload_read_different_params(setup_repo, w_mode, r_mode, pre_sign):
    clt, repo = setup_repo

    # urllib3 encodes TextIOBase to ISO-8859-1, data should contain symbols that can be encoded as such
    data = b'test \x48\x65\x6c\x6c\x6f\x20\x57\x6f\x72\x6c\x64\x21'
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).upload(
        data=data, mode=w_mode, pre_sign=pre_sign)

    with obj.reader(mode=r_mode) as fd:
        res = fd.read()
        if 'b' in r_mode:
            assert res == data
        else:
            assert res.encode('utf-8') == data


def test_object_copy(setup_repo):
    clt, repo = setup_repo
    data = "test_data"
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).upload(
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


def test_writer(setup_repo):
    _, repo = setup_repo
    obj = repo.branch("main").object("test_object")
    with obj.writer() as writer:
        with expect_exception_context(io.UnsupportedOperation):
            writer.seek(10)

        assert not writer.readable()
        assert writer.writable()

        writer.write("Hello")
        writer.write(" World!")

        # Check that the object does not exist in lakeFS before we close the writer
        with expect_exception_context(NotFoundException):
            obj.stat()

    assert obj.reader().read() == b"Hello World!"


@pytest.mark.parametrize("w_mode", get_args(WriteModes))
@pytest.mark.parametrize("r_mode", get_args(ReadModes))
@pytest.mark.parametrize("pre_sign", (True, False), indirect=True)
def test_writer_different_params(setup_repo, w_mode, r_mode, pre_sign):
    _, repo = setup_repo
    obj = repo.branch("main").object("test_object")
    writer = obj.writer(mode=w_mode)

    data = [
        "The quick brown fox jumps over the lazy dog"
        "Pack my box with five dozen liquor jugs"
        "Sphinx of black quartz, judge my vow" * 1000
    ]

    pos = 0
    for part in data:
        pos += len(part)
        writer.write(part)
        assert writer.tell() == pos

    # Check that the object does not exist in lakeFS before we close the writer
    with expect_exception_context(NotFoundException):
        obj.stat()

    writer.content_type = "text/plain"
    writer.metadata = {"foo": "bar"}
    writer.pre_sign = pre_sign

    writer.close()

    stats = obj.stat()
    assert stats.metadata == {"foo": "bar"}
    assert stats.content_type == "text/plain"
    assert stats.path == "test_object"
    assert stats.size_bytes == pos

    expected = "".join(data)
    res = obj.reader(mode=r_mode).read()

    if 'b' in r_mode:
        assert res.decode('utf-8') == expected
    else:
        assert res == expected
