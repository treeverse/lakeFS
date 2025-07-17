# API Reference Template

Use this template for consistent API documentation throughout the Python documentation.

## Method Documentation Format

```markdown
#### `method_name(param1, param2=None, **kwargs)`

Brief description of the method and its purpose. Explain what it does and when to use it.

**Parameters:**
- `param1` (str): Description of the parameter, including valid values or constraints
- `param2` (Optional[int], default=None): Description of optional parameter with default value
- `**kwargs`: Additional keyword arguments passed to underlying implementation

**Returns:**
- `ReturnType`: Description of what the method returns, including type information

**Raises:**
- `SpecificException`: When this specific exception occurs and why
- `ValueError`: When invalid parameters are provided
- `ConnectionError`: When connection to lakeFS fails

**Example:**
```python
# Basic usage
result = obj.method_name("required_value")

# With optional parameters
result = obj.method_name("required_value", param2=42, extra_option=True)

# Error handling
try:
    result = obj.method_name("value")
except SpecificException as e:
    print(f"Operation failed: {e}")
```

**See Also:**
- [Related Method](../path/to/related-method.md)
- [Usage Guide](../path/to/usage-guide.md)
```

## Class Documentation Format

```markdown
### ClassName

Brief description of the class and its purpose.

**Initialization:**
```python
obj = ClassName(param1, param2=default_value)
```

**Parameters:**
- `param1` (str): Required parameter description
- `param2` (Optional[type], default=default_value): Optional parameter description

**Attributes:**
- `attribute_name` (type): Description of public attribute
- `another_attr` (type): Description of another attribute

**Methods:**
- [`method1()`](#method1): Brief description
- [`method2()`](#method2): Brief description

**Example:**
```python
# Create instance
obj = ClassName("required_param")

# Use methods
result = obj.method1()
```
```

## Exception Documentation Format

```markdown
### ExceptionName

Description of when this exception is raised and what it indicates.

**Inheritance:** `BaseException` → `CustomException` → `ExceptionName`

**Attributes:**
- `message` (str): Error message
- `error_code` (Optional[int]): Specific error code if applicable

**Example:**
```python
try:
    # Operation that might raise this exception
    result = risky_operation()
except ExceptionName as e:
    print(f"Error: {e.message}")
    if e.error_code:
        print(f"Code: {e.error_code}")
```
```

## Type Hints and Annotations

Use consistent type hints throughout documentation:

```python
from typing import Optional, List, Dict, Union, Any

def example_function(
    required_param: str,
    optional_param: Optional[int] = None,
    list_param: List[str] = None,
    dict_param: Dict[str, Any] = None
) -> Union[str, None]:
    """
    Example function with proper type hints.
    
    Args:
        required_param: Required string parameter
        optional_param: Optional integer parameter
        list_param: List of strings
        dict_param: Dictionary with string keys
    
    Returns:
        String result or None if operation fails
    """
    pass
```

## SDK-Specific API Documentation

### High-Level SDK API Format
Focus on the simplified, Pythonic interface:

```markdown
#### `repository(name)`

Get a repository object for the specified repository name.

**Parameters:**
- `name` (str): Repository name

**Returns:**
- `Repository`: Repository object for performing operations

**Example:**
```python
repo = lakefs.repository("my-repo")
```
```

### Generated SDK API Format
Include full OpenAPI-based documentation:

```markdown
#### `create_repository(repository_creation)`

Create a new repository using the Generated SDK.

**Parameters:**
- `repository_creation` (RepositoryCreation): Repository creation object containing:
  - `name` (str): Repository name
  - `storage_namespace` (str): Storage namespace URI
  - `default_branch` (Optional[str]): Default branch name

**Returns:**
- `Repository`: Created repository object

**Raises:**
- `ConflictException`: Repository already exists
- `ValidationException`: Invalid parameters

**Example:**
```python
from lakefs_sdk import RepositoryCreation

repo_creation = RepositoryCreation(
    name="my-repo",
    storage_namespace="s3://bucket/path"
)
repo = repositories_api.create_repository(repository_creation=repo_creation)
```
```

## Documentation Standards

### Consistency Guidelines
1. **Parameter Names**: Use consistent parameter names across similar operations
2. **Return Types**: Always specify return types clearly
3. **Error Handling**: Document all possible exceptions
4. **Examples**: Provide practical, runnable examples
5. **Cross-References**: Link to related documentation

### Writing Style
- Use active voice
- Be concise but complete
- Explain the "why" not just the "what"
- Include practical use cases
- Use consistent terminology

### Code Quality
- All examples must be syntactically correct
- Use realistic parameter values
- Include error handling where appropriate
- Show both basic and advanced usage patterns