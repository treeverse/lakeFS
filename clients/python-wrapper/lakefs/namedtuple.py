"""
LenientNamedTuple Module
"""


class LenientNamedTuple:
    """
    Class which provides the NamedTuple functionality but allows initializing with unknown fields.
    The unknown fields will be ignored and mandatory fields are enforced
    """

    __initialized: bool = False
    unknown: dict = {}

    def __init__(self, **kwargs):
        fields = list(self.__class__.__dict__["__annotations__"].keys())
        for k, v in kwargs.items():
            if k in fields:
                setattr(self, k, v)
                fields.remove(k)
            else:
                self.unknown[k] = v

        if len(fields) > 0:
            raise TypeError(f"missing {len(fields)} required arguments: {fields}")

        self.__initialized = True
        super().__init__()

    def __repr__(self):
        class_name = self.__class__.__name__
        if hasattr(self, 'id'):
            return f'{class_name}(id="{self.id}")'
        return f'{class_name}()'

    def __setattr__(self, name, value):
        if self.__initialized:
            raise AttributeError("can't set attribute")
        super().__setattr__(name, value)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        for k, v in self.__dict__.items():
            if k == "unknown":
                continue
            if other.__dict__[k] != v:
                return False

        return True

    def __str__(self):
        fields = {}
        for k, v in self.__dict__.items():
            if k != "unknown" and k[0] != "_":  # Filter internal and unknown fields
                fields[k] = v
        return str(fields)
