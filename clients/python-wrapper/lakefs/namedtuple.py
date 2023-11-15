"""
LenientNamedTuple Module
"""


class LenientNamedTuple:
    """
    Class which provides the NamedTuple functionality but allows initializing with unknown fields.
    The unknown fields will be ignored and mandatory fields are enforced
    """

    def __new__(cls, **kwargs):
        fields = list(cls.__dict__["__annotations__"].keys())
        for k, v in kwargs.items():
            if k in fields:
                setattr(cls, k, v)
                fields.remove(k)
        if len(fields) > 0:
            raise TypeError(f"missing {len(fields)} required arguments: {fields}")
        return super(LenientNamedTuple, cls).__new__(cls)

    def __setattr__(self, name, value):
        raise AttributeError("can't set attribute")

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        for k, v in self.__dict__.items():
            if other.__dict__[k] != v:
                return False

        return True
