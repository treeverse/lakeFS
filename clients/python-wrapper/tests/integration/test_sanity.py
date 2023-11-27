import time

from tests.utests.common import expect_exception_context
from lakefs.exceptions import NotFoundException, ConflictException, ObjectNotFoundException
from lakefs import RepositoryProperties, WriteableObject


def test_repository_sanity(storage_namespace, setup_repo):
    _, repo = setup_repo
    default_branch = "main"
    expected_properties = RepositoryProperties(id=repo.properties.id,
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

    # Delete repository
    repo.delete()

    # Delete non existent
    with expect_exception_context(NotFoundException):
        repo.delete()


def test_branch_sanity(storage_namespace, setup_repo):
    _, repo = setup_repo
    branch_name = "test_branch"

    main_branch = repo.branch("main")
    new_branch = repo.branch(branch_name).create("main")
    assert new_branch.repo_id == repo.properties.id
    assert new_branch.id == branch_name
    assert new_branch.head().id == main_branch.head().id

    initial_content = b"test_content"
    new_branch.object("test_object").upload(initial_content)
    new_branch.commit("test_commit", {"test_key": "test_value"})

    override_content = b"override_test_content"
    obj = new_branch.object("test_object").upload(override_content)
    new_branch.commit("override_data")
    with obj.reader() as fd:
        assert fd.read() == override_content

    new_branch.revert(new_branch.head().id)

    with obj.reader() as fd:
        assert fd.read() == initial_content

    new_branch.delete()

    with expect_exception_context(NotFoundException):
        new_branch.head()


def test_ref_sanity(setup_repo):
    _, repo = setup_repo
    ref_id = "main"
    ref = repo.ref(ref_id)
    assert ref.repo_id == repo.properties.id
    assert ref.id == ref_id
    assert ref.metadata() == {}
    assert ref.commit_message() == "Repository created"


def test_tag_sanity(setup_repo):
    _, repo = setup_repo
    tag_name = "test_tag"
    tag = repo.tag(tag_name)

    # expect not found
    with expect_exception_context(NotFoundException):
        tag.commit_message()

    commit = repo.commit("main")
    res = tag.create(commit.id)
    assert res == tag
    assert tag.id == tag_name
    assert tag.metadata() == commit.metadata()
    assert tag.commit_message() == commit.commit_message()

    # Create again
    with expect_exception_context(ConflictException):
        tag.create(tag_name)

    # Create again with exist_ok
    tag2 = tag.create(tag_name, True)
    assert tag2 == tag

    # Delete tag
    tag.delete()

    # expect not found
    with expect_exception_context(NotFoundException):
        tag.metadata()

    # Delete twice
    with expect_exception_context(NotFoundException):
        tag.delete()


def test_object_sanity(setup_repo):
    clt, repo = setup_repo
    data = b"test_data"
    path = "test_obj"
    metadata = {"foo": "bar"}
    obj = WriteableObject(repository=repo.properties.id, reference="main", path=path, client=clt).upload(
        data=data, metadata=metadata)
    with obj.reader() as fd:
        assert fd.read() == data

    stats = obj.stat()
    assert stats.path == path == obj.path
    assert stats.path_type == "object"
    assert stats.mtime <= time.time()
    assert stats.size_bytes == len(data)
    assert stats.metadata == metadata
    assert stats.content_type == "application/octet-stream"

    obj.delete()
    with expect_exception_context(ObjectNotFoundException):
        obj.stat()
