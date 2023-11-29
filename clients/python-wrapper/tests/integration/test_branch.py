from pydantic import ValidationError

import lakefs
from tests.utests.common import expect_exception_context


def test_revert(setup_branch):
    branch = setup_branch

    initial_content = "test_content"
    branch.object("test_object").upload(initial_content)
    branch.commit("test_commit", {"test_key": "test_value"})

    override_content = "override_test_content"
    obj = branch.object("test_object").upload(override_content)
    branch.commit("override_data")
    with obj.reader() as fd:
        assert fd.read() == override_content

    branch.revert(branch.head().id)

    with obj.reader() as fd:
        assert fd.read() == initial_content


def test_reset_changes(setup_branch):
    branch = setup_branch

    paths = ["a", "b", "bar/a", "bar/b", "bar/c", "c", "foo/a", "foo/b", "foo/c", ]
    upload_data(branch, paths)

    validate_uncommitted_changes(branch, paths)

    validate_uncommitted_changes(branch, ["bar/a", "bar/b", "bar/c"], prefix="bar")
    branch.reset_changes("object", "bar/a")
    validate_uncommitted_changes(branch, ["a", "b", "bar/b", "bar/c", "c", "foo/a", "foo/b", "foo/c", ])

    branch.reset_changes("object", "bar/")

    validate_uncommitted_changes(branch, ["a", "b", "bar/b", "bar/c", "c", "foo/a", "foo/b", "foo/c", ])

    branch.reset_changes("common_prefix", "foo/")
    validate_uncommitted_changes(branch, ["a", "b", "bar/b", "bar/c", "c"])

    branch.reset_changes()
    validate_uncommitted_changes(branch, [])


def test_delete_object_changes(setup_branch):
    branch = setup_branch

    path_and_data = ["a", "b", "bar/a", "bar/b", "bar/c", "c", "foo/a", "foo/b", "foo/c", ]
    for s in path_and_data:
        branch.object(s).upload(s)
    branch.commit("add some files", {"test_key": "test_value"})

    branch.delete_objects("foo/a")
    validate_uncommitted_changes(branch, ["foo/a"], "removed")

    paths = {"foo/b", "foo/c"}
    branch.delete_objects(paths)
    validate_uncommitted_changes(branch, ["foo/a", "foo/b", "foo/c"], "removed")
    repo = lakefs.Repository(branch.repo_id)
    branch.delete_objects([repo.ref(branch.head()).object("a"), repo.ref(branch.head()).object("b")])
    validate_uncommitted_changes(branch, ["a", "b", "foo/a", "foo/b", "foo/c"], "removed")
    with expect_exception_context(ValidationError):
        branch.reset_changes("unknown", "foo/")


def upload_data(branch, path_and_data):
    for s in path_and_data:
        branch.object(s).upload(s)


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
