from contextlib import contextmanager


@contextmanager
def expect_exception_context(ex, status_code=None):
    try:
        yield
        assert 0, f"No exception raised! Expected exception of type {ex.__name__}"
    except ex as e:
        if status_code is not None:
            assert e.status_code == status_code
