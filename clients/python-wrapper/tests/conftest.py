import os
import uuid

import pytest

TEST_STORAGE_NAMESPACE_BASE = os.getenv("STORAGE_NAMESPACE", "")


@pytest.fixture()
def storage_namespace(request):
    test_name = request.node.name.replace("_", "-")
    return "/".join(s.strip("/") for s in [TEST_STORAGE_NAMESPACE_BASE, str(uuid.uuid1()), test_name])
