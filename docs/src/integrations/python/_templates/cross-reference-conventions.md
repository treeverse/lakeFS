# Cross-Reference Conventions

This document defines the conventions for linking and cross-referencing content within the Python documentation.

## Link Types and Conventions

### Internal Links

#### Relative Path Links
Use relative paths for all internal links:

```markdown
<!-- Good -->
[Getting Started](../getting-started.md)
[High-Level SDK](../high-level-sdk/index.md)
[API Reference](../reference/api-comparison.md)

<!-- Avoid absolute paths -->
[Getting Started](/docs/src/integrations/python/getting-started.md)
```

#### Section Links
Link to specific sections within pages:

```markdown
[Repository Operations](repositories.md#creating-repositories)
[Error Handling](../reference/troubleshooting.md#common-errors)
```

#### Cross-SDK References
When referencing other SDK options:

```markdown
<!-- From High-Level SDK to Generated SDK -->
For direct API access, see the [Generated SDK](../generated-sdk/index.md).

<!-- From lakefs-spec to High-Level SDK -->
For transaction support, consider the [High-Level SDK](../high-level-sdk/transactions.md).
```

### External Links

#### Official Documentation
```markdown
[High-Level SDK Documentation](https://pydocs-lakefs.lakefs.io){:target="_blank"}
[Generated SDK Documentation](https://pydocs-sdk.lakefs.io){:target="_blank"}
[lakefs-spec Documentation](https://lakefs-spec.org/){:target="_blank"}
```

#### Third-Party Resources
```markdown
[pandas Documentation](https://pandas.pydata.org/docs/){:target="_blank"}
[Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html){:target="_blank"}
```

## "See Also" Sections

### Standard Format
Include "See Also" sections at the end of major topics:

```markdown
## See Also

- [Related Topic 1](../path/to/topic1.md) - Brief description
- [Related Topic 2](../path/to/topic2.md) - Brief description
- [External Resource](https://example.com){:target="_blank"} - Brief description
```

### Context-Specific Examples

#### From Code Examples
```markdown
**See Also:**
- [Advanced Usage](../advanced/patterns.md) - More complex examples
- [Error Handling](../reference/troubleshooting.md) - Common issues and solutions
- [Best Practices](../reference/best-practices.md) - Production recommendations
```

#### From API Documentation
```markdown
**See Also:**
- [Related Methods](#related-method) - Similar functionality
- [Usage Examples](../examples/usage.md) - Practical implementations
- [Generated SDK Equivalent](../generated-sdk/api-reference.md#equivalent-method) - Direct API access
```

## Navigation Patterns

### Breadcrumb References
Include contextual navigation hints:

```markdown
<!-- At the top of detailed pages -->
← Back to [High-Level SDK Overview](index.md)

<!-- At the bottom of sections -->
**Next:** [Branches and Commits](branches-and-commits.md) →
```

### Progressive Learning Paths
Guide users through logical learning sequences:

```markdown
## Learning Path

1. **Start Here:** [Getting Started](../getting-started.md)
2. **Basic Operations:** [Repository Management](repositories.md)
3. **Advanced Features:** [Transactions](transactions.md)
4. **Best Practices:** [Production Guide](../reference/best-practices.md)
```

## SDK Comparison References

### Feature Comparison Links
When discussing features, link to comparisons:

```markdown
The High-Level SDK provides simplified transaction support. For a complete comparison of transaction features across all SDKs, see the [API Comparison](../reference/api-comparison.md#transactions).
```

### Alternative Approaches
Show users alternative ways to accomplish tasks:

```markdown
### Alternative Approaches

- **High-Level SDK:** [Simple upload method](../high-level-sdk/objects-and-io.md#simple-upload)
- **Generated SDK:** [Direct API upload](../generated-sdk/examples.md#upload-operations)
- **lakefs-spec:** [Filesystem write](../lakefs-spec/filesystem-api.md#writing-files)
- **Boto3:** [S3-compatible upload](../boto3/s3-operations.md#upload-objects)
```

## Contextual Cross-References

### In Code Examples
Reference related concepts within code examples:

```python
# Create a repository (see Repository Management guide)
repo = lakefs.repository("my-repo").create(
    storage_namespace="s3://bucket/path"
)

# Create a branch (see Branches and Commits guide)
branch = repo.branch("feature").create(source_reference="main")

# Use transactions for atomic operations (see Transactions guide)
with branch.transact(commit_message="Atomic update") as tx:
    # Operations here...
    pass
```

### In Error Messages
Link to troubleshooting information:

```markdown
If you encounter authentication errors, see the [Authentication Troubleshooting](../reference/troubleshooting.md#authentication-issues) section.
```

## Topic Clustering

### Related Content Groups
Group related topics together:

```markdown
## Repository Operations
- [Creating Repositories](repositories.md#creating-repositories)
- [Listing Repositories](repositories.md#listing-repositories)
- [Repository Configuration](repositories.md#configuration)

## Branch Operations  
- [Creating Branches](branches-and-commits.md#creating-branches)
- [Merging Branches](branches-and-commits.md#merging-branches)
- [Branch Protection](branches-and-commits.md#branch-protection)
```

### Workflow-Based References
Link content based on common workflows:

```markdown
## Data Science Workflow
1. [Setup Environment](../getting-started.md#installation)
2. [Load Data](../lakefs-spec/integrations.md#pandas-integration)
3. [Process Data](../tutorials/data-science-workflow.md#data-processing)
4. [Version Results](../high-level-sdk/transactions.md#data-versioning)
```

## Link Maintenance

### Link Validation
- Use automated tools to check for broken links
- Regularly review and update external links
- Test internal links after restructuring content

### Consistency Checks
- Ensure consistent link text for the same destinations
- Use the same relative paths throughout
- Maintain consistent "See Also" formatting

### Update Procedures
When moving or renaming files:
1. Update all internal references
2. Add redirects if necessary
3. Update navigation menus
4. Test all affected links

## Best Practices

### Link Text Guidelines
```markdown
<!-- Good: Descriptive link text -->
[Learn about repository management](repositories.md)
[See the complete API reference](../reference/api-comparison.md)

<!-- Avoid: Generic link text -->
[Click here](repositories.md)
[Read more](../reference/api-comparison.md)
```

### Context-Aware Linking
```markdown
<!-- Provide context for why users should follow the link -->
For production deployments, review the [security best practices](../reference/best-practices.md#security) before configuring authentication.

<!-- Not just -->
See [best practices](../reference/best-practices.md).
```

### Balanced Cross-Referencing
- Don't over-link common terms
- Focus on genuinely helpful references
- Avoid circular references
- Prioritize the most relevant links

## Templates for Common Patterns

### Tutorial Cross-References
```markdown
## Prerequisites
- Complete the [Getting Started guide](../getting-started.md)
- Understand [basic repository operations](../high-level-sdk/repositories.md)

## Next Steps
- Try the [ETL Pipeline tutorial](etl-pipeline.md)
- Learn about [production deployment](../reference/best-practices.md#deployment)
```

### API Method Cross-References
```markdown
**Related Methods:**
- [`create_branch()`](branches-and-commits.md#create-branch) - Create new branches
- [`merge_into()`](branches-and-commits.md#merge-into) - Merge branches
- [`diff()`](branches-and-commits.md#diff) - Compare branches

**See Also:**
- [Branch Management Tutorial](../tutorials/branch-management.md)
- [Version Control Best Practices](../reference/best-practices.md#version-control)
```

This cross-reference system ensures users can easily navigate between related concepts and find the information they need efficiently.