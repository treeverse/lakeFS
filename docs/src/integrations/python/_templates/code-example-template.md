# Code Example Template

Use this template for consistent code examples throughout the Python documentation.

## Basic Code Example Format

```markdown
### Operation Name

Brief description of what this operation does and when to use it.

```python
# Complete, runnable example with clear comments
import lakefs

# Setup (if needed)
client = lakefs.Client(host="http://localhost:8000", ...)

# Main operation with descriptive variable names
result = client.operation_name(parameter="value")
print(result)
```

**Expected Output:**
```
Expected output here (when applicable)
```

**Key Points:**
- Important concept 1
- Important concept 2
- When to use this pattern

**See Also:**
- [Related Topic](../path/to/related.md)
- [Advanced Usage](../path/to/advanced.md)
```

## Code Example Guidelines

### Code Style
- Use clear, descriptive variable names
- Include necessary imports at the top
- Add comments explaining non-obvious operations
- Show complete, runnable examples when possible
- Use consistent indentation (4 spaces)

### Content Structure
- Start with a brief description
- Provide the code example
- Show expected output when relevant
- List key points or concepts
- Include cross-references to related topics

### Error Handling Examples
```python
try:
    # Operation that might fail
    result = client.risky_operation()
    print(f"Success: {result}")
except SpecificException as e:
    print(f"Specific error: {e}")
    # Handle specific error case
except Exception as e:
    print(f"General error: {e}")
    # Handle general error case
```

### Multi-Step Examples
For complex operations, break into clear steps:

```python
# Step 1: Setup
client = lakefs.Client(...)

# Step 2: Prepare data
data = prepare_data()

# Step 3: Execute operation
result = client.complex_operation(data)

# Step 4: Process results
processed_result = process_results(result)
```

## SDK-Specific Templates

### High-Level SDK Example
```python
import lakefs

# Use High-Level SDK patterns
repo = lakefs.repository("my-repo")
branch = repo.branch("main")
obj = branch.object("path/to/file.txt")

# Show the operation
result = obj.upload(data="content")
```

### Generated SDK Example
```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# Initialize Generated SDK client
client = LakeFSClient(configuration=lakefs_sdk.Configuration(...))
api = lakefs_sdk.ObjectsApi(client)

# Show the operation
result = api.upload_object(...)
```

### lakefs-spec Example
```python
from lakefs_spec import LakeFSFileSystem

# Initialize filesystem
fs = LakeFSFileSystem()

# Show the operation
fs.write_text("lakefs://repo/branch/file.txt", "content")
```

### Boto3 Example
```python
import boto3

# Initialize Boto3 client
s3 = boto3.client('s3', endpoint_url='http://localhost:8000', ...)

# Show the operation
s3.put_object(Bucket='repo', Key='branch/file.txt', Body=b'content')
```