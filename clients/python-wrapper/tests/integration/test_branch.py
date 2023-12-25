from pydantic import ValidationError

import lakefs
from lakefs.exceptions import NotFoundException, TransactionException
from tests.utests.common import expect_exception_context


def test_revert(setup_repo):
    _, repo = setup_repo
    test_branch = repo.branch("main")
    initial_content = "test_content"
    test_branch.object("test_object").upload(initial_content)
    test_branch.commit("test_commit", {"test_key": "test_value"})

    override_content = "override_test_content"
    obj = test_branch.object("test_object").upload(override_content)
    test_branch.commit("override_data")

    with obj.reader(mode='r') as fd:
        assert fd.read() == override_content

    test_branch.revert(test_branch.head.id)

    with obj.reader(mode='r') as fd:
        assert fd.read() == initial_content


def test_reset_changes(setup_repo):
    _, repo = setup_repo
    test_branch = repo.branch("main")
    paths = ["a", "b", "bar/a", "bar/b", "bar/c", "c", "foo/a", "foo/b", "foo/c", ]
    upload_data(test_branch, paths)

    validate_uncommitted_changes(test_branch, paths)

    validate_uncommitted_changes(test_branch, ["bar/a", "bar/b", "bar/c"], prefix="bar")
    test_branch.reset_changes("object", "bar/a")
    validate_uncommitted_changes(test_branch, ["a", "b", "bar/b", "bar/c", "c", "foo/a", "foo/b", "foo/c", ])

    test_branch.reset_changes("object", "bar/")

    validate_uncommitted_changes(test_branch, ["a", "b", "bar/b", "bar/c", "c", "foo/a", "foo/b", "foo/c", ])

    test_branch.reset_changes("common_prefix", "foo/")
    validate_uncommitted_changes(test_branch, ["a", "b", "bar/b", "bar/c", "c"])

    test_branch.reset_changes()
    validate_uncommitted_changes(test_branch, [])


def test_delete_object_changes(setup_repo):
    _, repo = setup_repo
    test_branch = repo.branch("main")
    path_and_data = ["a", "b", "bar/a", "bar/b", "bar/c", "c", "foo/a", "foo/b", "foo/c"]
    upload_data(test_branch, path_and_data)
    test_branch.commit("add some files", {"test_key": "test_value"})

    test_branch.delete_objects("foo/a")
    validate_uncommitted_changes(test_branch, ["foo/a"], "removed")

    paths = {"foo/b", "foo/c"}
    test_branch.delete_objects(paths)
    validate_uncommitted_changes(test_branch, ["foo/a", "foo/b", "foo/c"], "removed")
    repo = lakefs.Repository(test_branch.repo_id)
    test_branch.delete_objects([repo.ref(test_branch.head).object("a"), repo.ref(test_branch.head).object("b")])
    validate_uncommitted_changes(test_branch, ["a", "b", "foo/a", "foo/b", "foo/c"], "removed")
    with expect_exception_context(ValidationError):
        test_branch.reset_changes("unknown", "foo/")


def upload_data(branch, path_and_data, multiplier=1):
    for s in path_and_data:
        branch.object(s).upload(s * multiplier)


def validate_uncommitted_changes(branch, expected, change_type="added", prefix=""):
    count = 0
    for index, change in enumerate(branch.uncommitted(max_amount=10, prefix=prefix)):
        assert change.path == expected[index]
        assert change.path_type == "object"
        assert change.type == change_type
        assert change.size_bytes == 0 if change_type == "removed" else len(expected[index])
        count += 1
    if count != len(expected):
        raise AssertionError(f"Expected {len(expected)} changes, got {count}")


def test_transaction(setup_repo):
    _, repo = setup_repo
    path_and_data1 = ["a", "b", "bar/a", "bar/b", "bar/c", "c"]
    path_and_data2 = ["foo/a", "foo/b", "foo/c"]
    test_branch = repo.branch("main")

    with test_branch.transact(commit_message="my transaction", commit_metadata={"foo": "bar"}) as tx:
        assert tx.get_commit().id == test_branch.head.id
        upload_data(tx, path_and_data1)
        upload_data(tx, path_and_data2)
        tx.reset_changes(path_type="common_prefix", path="foo")
        tx_id = tx.id

    # Verify transaction branch was deleted
    with expect_exception_context(NotFoundException):
        repo.branch(tx.id).get_commit()

    #  Verify transaction completed successfully
    log = list(test_branch.log(amount=2))
    assert log[0].message == f"Merge transaction {tx_id} to branch"
    assert log[1].message == "my transaction"
    assert log[1].metadata.get("foo") == "bar"

    for obj in path_and_data1:
        assert test_branch.object(obj).exists()

    for obj in path_and_data2:
        assert not test_branch.object(obj).exists()

    # Reset all changes - ensure no new commits
    with test_branch.transact(commit_message="my transaction", commit_metadata={"foo": "bar"}) as tx:
        assert tx.get_commit().id == test_branch.head.id
        upload_data(tx, path_and_data2)
        tx.reset_changes()

    log = list(test_branch.log(amount=1))
    assert log[0].message == f"Merge transaction {tx_id} to branch"


def test_transaction_failure(setup_repo):
    _, repo = setup_repo
    new_data = ["a", "b", "bar/a", "bar/b"]
    common_data = ["foo/a", "foo/b", "foo/c"]
    test_branch = repo.branch("main")

    # Merge on dirty branch
    with expect_exception_context(TransactionException, "dirty branch"):
        with test_branch.transact(commit_message="my transaction") as tx:
            assert tx.get_commit().id == test_branch.head.id
            upload_data(tx, new_data)
            upload_data(test_branch, common_data, 2)

    # Verify tx branch was not deleted
    repo.branch(tx.id).get_commit()

    # Merge with conflicts
    with expect_exception_context(TransactionException, "Conflict"):
        with test_branch.transact(commit_message="my transaction") as tx:
            test_branch.commit(message="test branch commit")
            upload_data(tx, common_data)
