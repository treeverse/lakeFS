from tests.utests.common import expect_exception_context
from lakefs.namedtuple import LenientNamedTuple


class NamedTupleTest(LenientNamedTuple):
    field1: str
    field2: int
    field3: bool


def test_namedtuple():
    nt = NamedTupleTest(
        field1="1",
        field2=1,
        field3=True)
    assert nt.field1 == "1"
    assert nt.field2 == 1
    assert nt.field3

    # try to modify value
    with expect_exception_context(AttributeError):
        nt.field1 = "2"

    # Create named tuple with unknown args
    kwargs = {
        "field1": "test",
        "field2": 2,
        "field3": False,
        "field4": "something",
        "field5": None,
    }
    nt = NamedTupleTest(**kwargs)
    assert nt.field1 == "test"
    assert nt.field2 == 2
    assert not nt.field3

    with expect_exception_context(AttributeError):
        nt.field4

    with expect_exception_context(AttributeError):
        nt.field5

    # Verify extra kwargs are in 'unknown' dict
    assert nt.unknown['field4'] == "something"
    assert nt.unknown['field5'] is None

    # Missing args
    with expect_exception_context(TypeError):
        NamedTupleTest(field2=1)
