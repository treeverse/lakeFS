from lakefs.exceptions import NotFoundException, ConflictException
from lakefs.repository import RepositoryProperties
from tests.integration.conftest import expect_exception_context


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

    initial_content = "test_content"
    new_branch.object("test_object").create(initial_content, metadata={
        "test_key": "test_value"})  # TODO(Guys): remove metadata once bug is fixed
    new_branch.commit("test_commit", {"test_key": "test_value"})

    override_content = "override_test_content"
    new_branch.object("test_object").create(override_content, metadata={
        "test_key": "test_value"})  # TODO: remove metadata once bug is fixed
    new_branch.commit("override_data")
    assert new_branch.object("test_object").read().decode() == override_content
    new_branch.revert(new_branch.head().id)

    assert new_branch.object("test_object").read().decode() == initial_content

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
    assert ref.commit_message() == "Add sample data"


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
