import io
import csv
import json
import math
import os
from xml.etree import ElementTree
from typing import get_args

import pandas as pd
import yaml
import pytest

from tests.integration.conftest import TEST_DATA
from tests.utests.common import expect_exception_context
from lakefs.exceptions import ObjectExistsException, NotFoundException
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

        # This should return an empty string (simulates behavior of builtin open())
        assert len(fd.read(1)) == 0

        fd.seek(0)
        for c in data:
            assert ord(fd.read(1)) == c
        # This should return an empty string (simulates behavior of builtin open())
        assert len(fd.read(1)) == 0

        fd.seek(-4, os.SEEK_END)
        assert fd.read() == data[len(data) - 4:]

        with expect_exception_context(OSError):
            fd.seek(-len(data) - 1, os.SEEK_CUR)

        assert fd.tell() == len(data)

        fd.seek(-4, os.SEEK_CUR)
        assert fd.read() == data[len(data) - 4:]

        with expect_exception_context(io.UnsupportedOperation):
            fd.seek(0, 10)


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


def _upload_file(repo, test_file):
    obj = repo.branch("main").object("test_obj")

    with open(test_file, "rb") as fd, obj.writer() as writer:
        writer.write(fd.read())

    return obj


def test_read_byte_by_byte(setup_repo):
    clt, repo = setup_repo

    data = b'test_data'
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).upload(
        data=data, pre_sign=False)
    res = b""
    reader = obj.reader()
    while True:
        byte = reader.read(1)
        if not byte:
            break
        res += byte

    assert res == data


@TEST_DATA
def test_read_all(setup_repo, datafiles):
    _, repo = setup_repo
    test_file = datafiles / "data.csv"
    obj = _upload_file(repo, test_file)

    with open(test_file, "r", encoding="utf-8") as fd:
        data = fd.read()
        read_data = obj.reader("r").read()
        assert read_data == data


@TEST_DATA
def test_read_csv(setup_repo, datafiles):
    _, repo = setup_repo
    test_file = datafiles / "data.csv"
    obj = _upload_file(repo, test_file)

    uploaded = csv.reader(obj.reader('r'))

    with open(test_file, "r", encoding="utf-8") as fd:
        source = csv.reader(fd)
        for uploaded_row, source_row in zip(uploaded, source):
            assert uploaded_row == source_row


@TEST_DATA
def test_read_json(setup_repo, datafiles):
    _, repo = setup_repo
    test_file = datafiles / "data.json"
    obj = _upload_file(repo, test_file)

    with open(test_file, "r", encoding="utf-8") as fd:
        source = json.load(fd)
        uploaded = json.load(obj.reader(mode="r"))

        assert uploaded == source


@TEST_DATA
def test_read_yaml(setup_repo, datafiles):
    _, repo = setup_repo
    test_file = datafiles / "data.yaml"
    obj = _upload_file(repo, test_file)

    with open(test_file, "r", encoding="utf-8") as fd:
        source = yaml.load(fd, Loader=yaml.Loader)
        uploaded = yaml.load(obj.reader(mode="r"), Loader=yaml.Loader)
        assert uploaded == source


@TEST_DATA
def test_read_parquet(setup_repo, datafiles):
    _, repo = setup_repo
    test_file = datafiles / "data.parquet"
    obj = _upload_file(repo, test_file)

    uploaded = pd.read_parquet(obj.reader('rb'))
    print(uploaded)
    with open(test_file, "rb") as fd:
        source = pd.read_parquet(fd)
        assert source.equals(uploaded)


@TEST_DATA
def test_read_xml(setup_repo, datafiles):
    _, repo = setup_repo
    test_file = datafiles / "data.xml"
    obj = _upload_file(repo, test_file)

    uploaded = ElementTree.parse(obj.reader(mode="r"))
    source = ElementTree.parse(test_file)
    uploaded_root = uploaded.getroot()
    source_root = source.getroot()
    assert uploaded_root.tag == source_root.tag
    assert uploaded_root.attrib == source_root.attrib
    assert uploaded_root.text == source_root.text
    assert uploaded_root.tail == source_root.tail

    for uploaded_child, source_child in zip(uploaded_root, source_root):
        for uploaded_child_child, source_child_child in zip(uploaded_child, source_child):
            assert uploaded_child_child.tag == source_child_child.tag
            assert uploaded_child_child.attrib == source_child_child.attrib
            assert uploaded_child_child.text == source_child_child.text
            assert uploaded_child_child.tail == source_child_child.tail


def test_readline_no_newline(setup_repo):
    clt, repo = setup_repo
    data = b'test_data'
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).upload(
        data=data, pre_sign=False)

    assert obj.reader().readline() == data

    assert obj.reader().readline() == data


def test_readline_partial_line_buffer(setup_repo):
    clt, repo = setup_repo
    data = "a" * 15 + "\n" + "b" * 25 + "\n" + "That is all folks! "
    obj = WriteableObject(repository=repo.properties.id, reference="main", path="test_obj", client=clt).upload(
        data=data, pre_sign=False)

    with obj.reader(mode="r") as reader:
        reader.seek(5)
        assert reader.readline() == "a" * 10 + "\n"
        assert reader.readline() == "b" * 25 + "\n"
        assert reader.readline() == "That is all folks! "
        assert reader.readline() == ""
        reader.seek(0)
        assert reader.readline() == "a" * 15 + "\n"
        assert reader.read() == data[16:]

    # Read with limit
    with obj.reader(mode="r") as reader:
        for _ in range(math.ceil(len(data) / 10)):
            end = 10
            index = data.find("\n")
            if -1 < index < 10:
                end = index + 1

            expected = data[:end]
            data = data[len(expected):]
            read = reader.readline(10)
            assert read == expected

        assert reader.read() == ""


def test_write_read_csv(setup_repo):
    _, repo = setup_repo
    columns = ["ID", "Name", "Email"]
    sample_data = [
        ['1', "Alice", "alice@example.com"],
        ['2', "Bob", "bob@example.com"],
        ['3', "Carol", "carol@example.com"],
    ]
    obj = repo.branch("main").object(path="csv/sample_data.csv")

    with obj.writer(mode='w', pre_sign=False, content_type="text/csv") as fd:
        writer = csv.writer(fd)
        writer.writerow(columns)
        for row in sample_data:
            writer.writerow(row)

    for i, row in enumerate(csv.reader(obj.reader(mode='r'))):
        if i == 0:
            assert row == columns
        else:
            assert row == sample_data[i - 1]
