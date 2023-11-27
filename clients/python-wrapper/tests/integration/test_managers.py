import pytest

from tests.integration.conftest import _setup_repo, get_storage_namespace


@pytest.fixture(name="setup_repo_with_branches_and_tags", scope="session")
def fixture_setup_repo_with_branches_and_tags():
    clt, repo = _setup_repo(get_storage_namespace("branches-and-tags"),
                            "branches-and-tags",
                            "main")
    for i in range(10):
        for j in range(100):
            b = repo.branch(f"branches{i:02d}-{j:03d}").create("main")
            repo.tag(tag_id=f"tags{i:02d}-{j:03d}").create(b)

    return clt, repo


@pytest.mark.parametrize("attr", ("branches", "tags"))
def test_branch_manager(setup_repo_with_branches_and_tags, attr):
    _, repo = setup_repo_with_branches_and_tags

    manager = getattr(repo, attr)

    total = 1000
    if attr == "branches":
        total += 1  # Including main
    assert len(list(manager.list())) == total

    after = 49
    res = list(manager.list(max_amount=100, prefix=f"{attr}02", after=f"{attr}02-{after:03d}"))
    assert len(res) == 50
    for i, b in enumerate(res):
        assert b.id == f"{attr}02-{i + after + 1:03d}"
