import pytest

from tests.integration.conftest import _setup_repo, get_storage_namespace


@pytest.fixture(name="setup_repo_with_branches_and_tags", scope="session")
def fixture_setup_repo_with_branches_and_tags():
    clt, repo = _setup_repo(get_storage_namespace("branches-and-tags"),
                            "branches-and-tags",
                            "main")
    for i in range(10):
        for j in range(20):
            b = repo.branch(f"branches{i:02d}-{j:02d}").create("main")
            repo.tag(tag_id=f"tags{i:02d}-{j:02d}").create(b)

    return clt, repo


@pytest.mark.parametrize("attr", ("branches", "tags"))
def test_repository_listings(setup_repo_with_branches_and_tags, attr):
    _, repo = setup_repo_with_branches_and_tags

    func = getattr(repo, attr)

    total = 200
    if attr == "branches":
        total += 1  # Including main
    assert len(list(func())) == total

    after = 9
    res = list(func(max_amount=100, prefix=f"{attr}01", after=f"{attr}01-{after:02d}"))
    assert len(res) == 10
    for i, b in enumerate(res):
        assert b.id == f"{attr}01-{i + after + 1:02d}"
