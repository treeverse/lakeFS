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
    try:
        nt.field1 = "2"
        assert 0, "Exception expected"
    except AttributeError:
        pass

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
    try:
        nt.field4
        assert 0, "Expected exception"
    except AttributeError:
        pass

    try:
        nt.field5
        assert 0, "Expected exception"
    except AttributeError:
        pass

    # Missing args
    try:
        NamedTupleTest(field2=1)
        assert 0, "Exception expected"
    except TypeError:
        pass
