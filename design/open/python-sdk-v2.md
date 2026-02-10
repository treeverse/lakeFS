# Python SDK V2 (V3...)

## Problem Description  
The current `lakefs-sdk` Python package (published as `lakefs_sdk`) is generated using OpenAPI Generator v7.0.1, which produces Pydantic V1 code.  
Pydantic V1 is [not supported on Python 3.14+](https://github.com/treeverse/lakeFS/issues/10004) - Python 3.14 introduces new type annotation semantics (PEP 649 / PEP 749) that V1 cannot accommodate, making Python 3.13 the last supported version for Pydantic V1 ([reference](https://pydantic.dev/articles/pydantic-v2-12-release)).

This means the current SDK will not work on Python 3.14+. At the same time, we cannot simply regenerate the existing `lakefs-sdk` package with Pydantic V2 code, because that would be a breaking change for users who depend on the current V1-based API (`.dict()`, `.json()`, `@validator`, etc.).

## Goals  
1. Publish a new Python SDK package (`lakefs-sdk-v2` / `lakefs_sdk_v2`) generated with OpenAPI Generator v7.9.0, producing native Pydantic V2 code
2. Migrate the high-level Python SDK wrapper (`lakefs`) from `lakefs-sdk` to `lakefs-sdk-v2`
3. Sunset the old `lakefs-sdk` package with a clear deprecation timeline
4. Maintain backward compatibility throughout the transition period

## Non-Goals  
1. Changing the API surface of the high-level Python SDK wrapper (`lakefs` package) - the wrapper should continue to work identically from the user's perspective
2. Supporting Pydantic V1 in the new SDK - the new package requires Pydantic >= 2.0
3. Supporting Python < 3.10 in the new SDK

## Current Architecture  
```
┌─────────────────────────────────────────────────┐
│  lakefs (PyPI: lakefs)                          │
│  High-level Python SDK wrapper                  │
│  clients/python-wrapper/                        │
│  depends on: lakefs-sdk >= 1.50, < 2            │
├─────────────────────────────────────────────────┤
│  lakefs_sdk (PyPI: lakefs-sdk)                  │
│  Auto-generated, OpenAPI Generator v7.0.1       │
│  clients/python/                                │
│  Pydantic V1 with V2 compat shim                │
└─────────────────────────────────────────────────┘
```

## Target Architecture  
```
┌─────────────────────────────────────────────────┐
│  lakefs (PyPI: lakefs)                          │
│  High-level Python SDK wrapper                  │
│  clients/python-wrapper/                        │
│  depends on: lakefs-sdk-v2 >= 1.0, < 2          │
├─────────────────────────────────────────────────┤
│  lakefs_sdk_v2 (PyPI: lakefs-sdk-v2)            │
│  Auto-generated, OpenAPI Generator v7.9.0       │
│  clients/python-v2/                             │
│  Native Pydantic V2                             │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│  lakefs_sdk (PyPI: lakefs-sdk)  [DEPRECATED]    │
│  Frozen, no new features                        │
│  clients/python/                                │
└─────────────────────────────────────────────────┘
```

## Breaking Changes Between Old and New SDK  
The following changes are introduced by the Pydantic V1 → V2 migration in the generated code:

| Area                  | Old SDK (`lakefs_sdk`)         | New SDK (`lakefs_sdk_v2`)                     |
|-----------------------|--------------------------------|-----------------------------------------------|
| Import path           | `import lakefs_sdk`            | `import lakefs_sdk_v2`                        |
| Pydantic requirement  | `pydantic >= 1.10.5`           | `pydantic >= 2.0`                             |
| Python requirement    | `>= 3.7`                       | `>= 3.9`                                      |
| Model serialization   | `.dict()`, `.json()`           | `.model_dump()`, `.model_dump_json()`         |
| Model deserialization | `.parse_obj()`, `.parse_raw()` | `.model_validate()`, `.model_validate_json()` |
| Model config          | `class Config:` inner class    | `model_config = ConfigDict(...)`              |
| Validators            | `@validator`                   | `@field_validator`                            |
| Argument validation   | `@validate_arguments`          | `@validate_call`                              |
| Constrained types     | `conint()`, `constr()`         | `Annotated[int, Field(ge=...)]`               |
| Async support         | `async_req=True` (thread pool) | Native `asyncio` (`library=asyncio`)          |

## Implementation Plan  
### Phase 1: New SDK alongside old (this branch)  
**Status: Done** (`claude/update-python-sdk-KyIG4`)

Create the new auto-generated SDK as a separate package that coexists with the old one, and publish it.

#### Deliverables  
- `clients/python-v2-static/` - codegen templates and config
- `clients/python-v2/` - generated SDK (`lakefs_sdk_v2` package, `lakefs-sdk-v2` on PyPI)
- Makefile targets for generation, packaging, and validation
- CI workflows for publishing to PyPI and TestPyPI
- Documentation website and migration guide

#### Validation  
- Functional parity - the new SDK exposes the same API endpoints and models as the old one
- Pydantic V2 correctness - model serialization/deserialization works with `model_dump()` / `model_validate()`
- Python 3.14 compatibility - no deprecation warnings on Python 3.14+

### Phase 2: Migrate the high-level Python SDK wrapper  
Move `clients/python-wrapper/` (`lakefs` package) from depending on `lakefs-sdk` to `lakefs-sdk-v2`.

1. Swap dependency from `lakefs-sdk` to `lakefs-sdk-v2` and replace all `lakefs_sdk` imports
2. Update any Pydantic V1 API usage (`.dict()`, `@validator`, etc.) to V2 equivalents
3. Remove Pydantic V1 compatibility from tests and CI
4. Version bump to signal the breaking dependency change

### Phase 3: Sunset the old SDK  
1. **Deprecation notice**: Publish a final release of `lakefs-sdk` with a deprecation warning:
    - Add a runtime warning in `lakefs_sdk/__init__.py`:
      ```python
      import warnings
      warnings.warn(
          "lakefs-sdk is deprecated. Please migrate to lakefs-sdk-v2. "
          "See https://docs.lakefs.io/sdk-migration for details.",
          DeprecationWarning,
          stacklevel=2,
      )
      ```
    - Update the PyPI description and README to point to the new package

2. **Freeze**: Stop regenerating `clients/python/` on API changes. Remove `client-python` from the `clients` Make target. Keep the directory for historical reference but stop publishing new versions.

3. **Cleanup** (after sufficient migration window):
    - Remove `clients/python/` and `clients/python-static/` from the repository
    - Remove `pydantic.sh`
    - Remove old Makefile targets (`client-python`, `package-python-sdk`, `validate-python-sdk`)
    - Remove old workflows (`python-api-client.yaml`, `python-api-client-test-pypi.yaml`)
    - Rename `clients/python-v2/` → `clients/python/` and `clients/python-v2-static/` → `clients/python-static/` (optional, for cleanliness)

## Timeline  
| Phase | Description                          | Depends on | Time |
|-------|--------------------------------------|------------|------|
| 1     | New SDK alongside old, publish, docs | -          |      |
| 2     | Migrate HL wrapper to new SDK        | Phase 1    |      |
| 3     | Sunset old SDK                       | Phase 2    |      |

## Risks and Mitigations  
| Risk                                                            | Mitigation                                                                                     |
|-----------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| HL wrapper breakage during migration                            | Run full unit + integration test suite against `lakefs-sdk-v2` before merging Phase 2          |
| Users on Pydantic V1 cannot upgrade                             | Old `lakefs-sdk` continues to work; no forced upgrade                                          |
| OpenAPI Generator v7.9.0 generates subtly different API surface | Functional parity testing in Phase 1; diff generated code against old SDK                      |
| Two SDK packages cause confusion                                | Clear documentation, deprecation warnings, and PyPI metadata pointing users to the new package |
