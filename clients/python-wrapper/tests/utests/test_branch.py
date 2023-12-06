import http

import lakefs_sdk

from tests.utests.common import get_test_client, expect_exception_context
from lakefs.repository import Repository
from lakefs.exceptions import ConflictException


def get_test_branch():
    client = get_test_client()
    repo = Repository(repository_id="test_repo", client=client)
    return repo.branch("test_branch")


def test_branch_creation():
    """
    Ensure branches can only be created in repo context
    """
    client = get_test_client()
    repo = Repository(repository_id="test_repo", client=client)
    branch = repo.branch("test_branch")
    assert branch.repo_id == "test_repo"
    assert branch.id == "test_branch"


def test_branch_create(monkeypatch):
    branch = get_test_branch()
    source = "main"
    with monkeypatch.context():
        def monkey_create_branch(_self, repo_name, branch_creation, *_):
            assert repo_name == branch.repo_id
            assert branch_creation.name == branch.id
            assert branch_creation.source == source
            return lakefs_sdk.BranchCreation(name=branch.id, source=source)

        monkeypatch.setattr(lakefs_sdk.BranchesApi, "create_branch", monkey_create_branch)
        branch.create(source)


def test_branch_create_already_exists(monkeypatch):
    branch = get_test_branch()
    source = "main"
    ex = lakefs_sdk.exceptions.ApiException(status=http.HTTPStatus.CONFLICT.value)

    with monkeypatch.context():
        def monkey_create_branch(_self, repo_name, branch_creation, *_):
            raise ex

        monkeypatch.setattr(lakefs_sdk.BranchesApi, "create_branch", monkey_create_branch)

        # Expect success when exist_ok = True
        res = branch.create(source, exist_ok=True)
        assert res.id == branch.id
        assert res.repo_id == branch.repo_id

        # Expect fail on exists
        with expect_exception_context(ConflictException):
            branch.create(source)


def test_branch_head(monkeypatch):
    branch = get_test_branch()
    commit_id = "1234"
    with monkeypatch.context():
        def monkey_get_branch(repo_name, branch_name, *_):
            assert repo_name == branch.repo_id
            assert branch_name == branch.id
            return lakefs_sdk.Ref(commit_id=commit_id, id=branch.id)

        monkeypatch.setattr(branch._client.sdk_client.branches_api, "get_branch", monkey_get_branch)
        res = branch.head()
        assert res.id == commit_id
        assert res.repo_id == branch.repo_id


def test_branch_commit(monkeypatch):
    branch = get_test_branch()
    md = {"key": "value"}
    commit_id = "1234"
    commit_message = "test message"
    with monkeypatch.context():
        def monkey_commit(repo_name, branch_name, commits_creation, *_):
            assert repo_name == branch.repo_id
            assert branch_name == branch.id
            assert commits_creation.message == commit_message
            assert commits_creation.metadata == md
            return lakefs_sdk.Commit(
                id=commit_id,
                parents=[""],
                committer="Committer",
                message=commit_message,
                creation_date=123,
                meta_range_id="",
            )

        monkeypatch.setattr(branch._client.sdk_client.commits_api, "commit", monkey_commit)
        res = branch.commit(commit_message, metadata=md)
        assert res.id == commit_id


def test_branch_delete(monkeypatch):
    branch = get_test_branch()
    with monkeypatch.context():
        def monkey_delete_branch(repo_name, branch_name, *_):
            assert repo_name == branch.repo_id
            assert branch_name == branch.id

        monkeypatch.setattr(branch._client.sdk_client.branches_api, "delete_branch", monkey_delete_branch)
        branch.delete()


def test_branch_revert(monkeypatch):
    branch = get_test_branch()
    ref_id = "ab1234"
    expected_parent = 0
    with monkeypatch.context():
        def monkey_revert_branch(repo_name, branch_name, revert_branch_creation, *_):
            assert repo_name == branch.repo_id
            assert branch_name == branch.id
            assert revert_branch_creation.ref == ref_id
            assert revert_branch_creation.parent_number == expected_parent  # default value

        # Test default parent number
        monkeypatch.setattr(branch._client.sdk_client.branches_api, "revert_branch", monkey_revert_branch)
        branch.revert(ref_id)
        expected_parent = 2
        # Test set parent number
        branch.revert(ref_id, 2)

        # Test set invalid parent number
        with expect_exception_context(ValueError):
            branch.revert(ref_id, 0)
