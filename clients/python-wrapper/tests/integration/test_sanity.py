import ssl
import time
import pytest

from tests.utests.common import expect_exception_context
from lakefs.exceptions import NotFoundException, ConflictException, ObjectNotFoundException
import lakefs


def test_repository_sanity(storage_namespace, setup_repo):
    _, repo = setup_repo
    repo = lakefs.repository(repo.properties.id)  # test the lakefs.repository function works properly
    default_branch = "main"
    expected_properties = lakefs.RepositoryProperties(id=repo.properties.id,
                                                      default_branch=default_branch,
                                                      storage_namespace=storage_namespace,
                                                      creation_date=repo.properties.creation_date)
    assert repo.properties == expected_properties

    # Create with allow exists
    new_data = repo.create(storage_namespace, default_branch, True, exist_ok=True)
    assert new_data.properties == repo.properties

    # Try to create twice and expect conflict
    with expect_exception_context(ConflictException):
        repo.create(storage_namespace, default_branch, True)

    # Get metadata
    _ = repo.metadata

    # List branches
    branches = list(repo.branches())
    assert len(branches) == 1

    # Delete repository
    repo.delete()

    # Delete non existent
    with expect_exception_context(NotFoundException):
        repo.delete()


def test_branch_sanity(setup_repo):
    _, repo = setup_repo
    branch_name = "test_branch"

    main_branch = repo.branch("main")
    new_branch = repo.branch(branch_name).create("main")
    assert new_branch.repo_id == repo.properties.id
    assert new_branch.id == branch_name
    assert new_branch.head.id == main_branch.head.id

    new_branch.delete()
    with expect_exception_context(NotFoundException):
        new_branch.head  # pylint: disable=pointless-statement


def test_ref_sanity(setup_repo):
    _, repo = setup_repo
    ref_id = "main"
    ref = repo.ref(ref_id)
    assert ref.repo_id == repo.properties.id
    assert ref.id == ref_id
    assert ref.get_commit().metadata == {}
    assert ref.get_commit().message == "Repository created"


def test_tag_sanity(setup_repo):
    _, repo = setup_repo
    tag_name = "test_tag"
    tag = repo.tag(tag_name)

    # expect not found
    with expect_exception_context(NotFoundException):
        tag.get_commit()

    branch = repo.branch("main")
    commit = branch.get_commit()
    res = tag.create(commit)
    assert res == tag
    assert tag.id == tag_name
    assert tag.get_commit().metadata == commit.metadata
    assert tag.get_commit().message == commit.message

    # Create again
    with expect_exception_context(ConflictException):
        tag.create(commit.id)

    # Create again with exist_ok
    tag2 = tag.create(tag_name, True)
    assert tag2 == tag

    # Delete tag
    tag.delete()

    # expect not found
    with expect_exception_context(NotFoundException):
        tag.get_commit()

    # Delete twice
    with expect_exception_context(NotFoundException):
        tag.delete()

    # Create again
    tag.create(commit.id)


def test_object_sanity(setup_repo):
    clt, repo = setup_repo
    data = b"test_data"
    path = "test_obj"
    metadata = {"foo": "bar"}
    obj = lakefs.WriteableObject(repository_id=repo.properties.id, reference_id="main", path=path, client=clt).upload(
        data=data, metadata=metadata)
    with obj.reader() as fd:
        assert fd.read() == data

    stats = obj.stat()
    assert stats.path == path == obj.path
    assert stats.mtime <= time.time()
    assert stats.size_bytes == len(data)
    assert stats.metadata == metadata
    assert stats.content_type == "application/octet-stream"

    obj.delete()
    with expect_exception_context(ObjectNotFoundException):
        obj.stat()


@pytest.mark.parametrize("ssl_enabled", (True, False, None))
def test_ssl_configuration(ssl_enabled):
    expected_ssl = ssl_enabled is not False
    expected_cert_req = ssl.CERT_REQUIRED if expected_ssl else ssl.CERT_NONE
    proxy = "http://someproxy:1234"
    cert = "somecert"
    clt = lakefs.client.Client(verify_ssl=ssl_enabled, proxy=proxy, ssl_ca_cert=cert)
    assert clt.config.verify_ssl is expected_ssl
    assert clt.config.proxy == proxy
    assert clt.config.ssl_ca_cert == cert
    assert clt.sdk_client._api.rest_client.pool_manager.connection_pool_kw.get("cert_reqs") is expected_cert_req
    assert str(clt.sdk_client._api.rest_client.pool_manager.proxy) == proxy
    assert clt.sdk_client._api.rest_client.pool_manager.connection_pool_kw.get("ca_certs") is cert
