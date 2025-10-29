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
    nt2 = NamedTupleTest(**kwargs)
    assert nt.field1 != nt2.field1
    assert nt2.field1 == "test"
    assert nt2.field2 == 2
    assert not nt2.field3

    with expect_exception_context(AttributeError):
        nt2.field4  # pylint: disable=pointless-statement

    with expect_exception_context(AttributeError):
        nt2.field5  # pylint: disable=pointless-statement

    # Verify extra kwargs are in 'unknown' dict
    assert nt2.unknown['field4'] == "something"
    assert nt2.unknown['field5'] is None

    # Missing args
    with expect_exception_context(TypeError):
        NamedTupleTest(field2=1)

    # Verify comparison works with different unknown fields
    kwargs["field5"] = "something"
    nt3 = NamedTupleTest(**kwargs)
    assert nt2 == nt3
