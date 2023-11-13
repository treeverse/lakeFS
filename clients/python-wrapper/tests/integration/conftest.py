import os
import uuid
from contextlib import contextmanager
import pytest

TEST_STORAGE_NAMESPACE_BASE = os.getenv("STORAGE_NAMESPACE", "").rstrip("/")


@contextmanager
def expect_exception_context(ex, status_code=None):
    try:
        yield
        assert 0, f"No exception raised! Expected exception of type {ex.__name__}"
    except ex as e:
        if status_code is not None:
            assert e.status_code == status_code


@pytest.fixture()
def storage_namespace(request):
    test_name = request.node.name.replace("_", "-")
    return f"{TEST_STORAGE_NAMESPACE_BASE}/{uuid.uuid1()}/{test_name}"
