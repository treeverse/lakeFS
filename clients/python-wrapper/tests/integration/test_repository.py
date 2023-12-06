import uuid
import pytest

import lakefs

from tests.integration.conftest import _setup_repo, get_storage_namespace

_NUM_PREFIXES = 10
_NUM_ELEM_PER_PREFIX = 20


@pytest.fixture(name="setup_repo_with_branches_and_tags", scope="session")
def fixture_setup_repo_with_branches_and_tags():
    clt, repo = _setup_repo(get_storage_namespace("branches-and-tags"),
                            "branches-and-tags",
                            "main")
    for i in range(_NUM_PREFIXES):
        for j in range(_NUM_ELEM_PER_PREFIX):
            b = repo.branch(f"branches{i:02d}-{j:02d}").create("main")
            repo.tag(tag_id=f"tags{i:02d}-{j:02d}").create(b)

    return clt, repo


@pytest.mark.parametrize("attr", ("branches", "tags"))
def test_repository_listings(setup_repo_with_branches_and_tags, attr):
    _, repo = setup_repo_with_branches_and_tags

    generator = getattr(repo, attr)

    total = _NUM_PREFIXES * _NUM_ELEM_PER_PREFIX
    if attr == "branches":
        total += 1  # Including main
    assert len(list(generator())) == total

    after = 9
    res = list(generator(max_amount=100, prefix=f"{attr}01", after=f"{attr}01-{after:02d}"))
    assert len(res) == 10
    for i, b in enumerate(res):
        assert b.id == f"{attr}01-{i + after + 1:02d}"


def test_repositories(storage_namespace):
    repo_base_name = f"test-repo{uuid.uuid1()}-"
    for i in range(10):
        lakefs.repository(f"{repo_base_name}{i}").create(storage_namespace=f"{storage_namespace}-{i}")

    repos = list(lakefs.repositories(prefix=repo_base_name))
    assert len(repos) == 10
    for i, repo in enumerate(repos):
        assert repo.properties.id == f"{repo_base_name}{i}"
