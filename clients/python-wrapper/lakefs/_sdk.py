"""
Selects the lakeFS SDK package that this package is built on top of.

lakeFS Enterprise users install ``lakefs-enterprise-sdk``, a superset of the
OSS ``lakefs-sdk``.  Use it when it is installed, and fall back to the OSS SDK
otherwise.  Every module in this package imports the SDK from here rather than
importing ``lakefs_sdk`` directly:

.. code-block:: python

    from lakefs._sdk import lakefs_sdk

The selection has to happen at module level in a module of its own.  It cannot
be wrapped in a function that other modules call: ``import x as y`` binds ``y``
in the scope in which it executes, so a helper function binds the SDK in its
own locals and its caller sees nothing.

Type checking always runs against the OSS SDK: it is the common subset of the
two SDKs, and it is the one that is always installed.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import lakefs_sdk
    import lakefs_sdk.client
    import lakefs_sdk.exceptions
else:
    import importlib

    # In order of preference.
    _SDK_PACKAGES = ('lakefs_enterprise_sdk', 'lakefs_sdk')

    # Submodules used by qualified name.  Importing the SDK package does not
    # necessarily import them.
    _SDK_SUBMODULES = ('client', 'exceptions')

    def _import_sdk():
        for package in _SDK_PACKAGES:
            try:
                sdk = importlib.import_module(package)
            except ImportError:
                continue
            for submodule in _SDK_SUBMODULES:
                importlib.import_module(f'{package}.{submodule}')
            return sdk
        raise ImportError(
            'no lakeFS SDK installed, tried: ' + ', '.join(_SDK_PACKAGES))

    lakefs_sdk = _import_sdk()
