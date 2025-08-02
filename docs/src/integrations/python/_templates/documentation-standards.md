# Documentation Standards

This document defines the standards and conventions for Python documentation in the lakeFS project.

## Content Organization

### File Structure
```
docs/src/integrations/python/
├── index.md                    # Main overview page
├── getting-started.md          # Installation and setup
├── high-level-sdk/            # High-Level SDK documentation
├── generated-sdk/             # Generated SDK documentation
├── lakefs-spec/               # lakefs-spec documentation
├── boto3/                     # Boto3 integration documentation
├── tutorials/                 # Real-world examples
├── reference/                 # Reference materials
└── _templates/                # Documentation templates
```

### Page Naming Conventions
- Use lowercase with hyphens: `getting-started.md`
- Be descriptive: `branches-and-commits.md` not `branches.md`
- Group related content in subdirectories
- Use `index.md` for section overview pages

## Markdown Standards

### Front Matter
Every page must include front matter:

```yaml
---
title: Page Title
description: Brief description for SEO and navigation (max 160 characters)
---
```

Optional front matter fields:
```yaml
---
title: Page Title
description: Brief description
sdk_types: ["high-level", "generated", "lakefs-spec", "boto3"]
difficulty: "beginner|intermediate|advanced"
last_updated: "2024-01-15"
---
```

### Heading Structure
- Use only one H1 (`#`) per page (the title)
- Use H2 (`##`) for major sections
- Use H3 (`###`) for subsections
- Use H4 (`####`) for API methods and detailed items
- Don't skip heading levels

### Code Blocks

#### Python Code
Always specify the language for syntax highlighting:

```python
import lakefs

# Example code with comments
repo = lakefs.repository("my-repo")
```

#### Shell Commands
```bash
pip install lakefs
```

#### Configuration Files
```yaml
# YAML configuration
key: value
```

#### Output Examples
```
Expected output here
```

### Links and Cross-References

#### Internal Links
- Use relative paths: `[link text](../path/to/page.md)`
- Link to specific sections: `[section](page.md#section-heading)`
- Use descriptive link text, not "click here"

#### External Links
- Open in new tab for external sites: `[text](https://example.com){:target="_blank"}`
- Include protocol: `https://` not `example.com`

### Lists and Formatting

#### Unordered Lists
- Use `-` for bullet points
- Be consistent with indentation (2 spaces)
- Use parallel structure in list items

#### Ordered Lists
1. Use numbers for sequential steps
2. Start each item with a capital letter
3. End with periods if items are complete sentences

#### Emphasis
- Use **bold** for UI elements and important terms
- Use *italics* for emphasis and variable names
- Use `code` for inline code, filenames, and commands

## Code Standards

### Code Quality
- All code examples must be syntactically correct
- Test code examples before publishing
- Use realistic, meaningful variable names
- Include necessary imports
- Show complete, runnable examples when possible

### Code Style
- Follow PEP 8 Python style guidelines
- Use 4 spaces for indentation
- Keep lines under 88 characters when possible
- Use descriptive variable names

### Error Handling
Include error handling in examples:

```python
try:
    result = operation()
    print(f"Success: {result}")
except SpecificException as e:
    print(f"Specific error: {e}")
except Exception as e:
    print(f"General error: {e}")
```

### Comments
- Explain non-obvious operations
- Don't comment obvious code
- Use comments to explain the "why" not the "what"

## Content Guidelines

### Writing Style
- Use active voice: "Create a repository" not "A repository is created"
- Use second person: "you" not "we" or "one"
- Be concise but complete
- Use present tense for instructions
- Use parallel structure in lists and headings

### Technical Accuracy
- Verify all technical information
- Keep content up-to-date with latest SDK versions
- Test all code examples
- Review for accuracy before publishing

### Accessibility
- Use descriptive alt text for images
- Ensure good color contrast
- Use semantic HTML elements
- Provide text alternatives for visual content

## SDK-Specific Standards

### High-Level SDK
- Focus on the simplified, Pythonic interface
- Show transaction patterns where applicable
- Emphasize ease of use and best practices
- Include streaming I/O examples

### Generated SDK
- Show direct API access patterns
- Include complete parameter documentation
- Demonstrate error handling
- Show how to access from High-Level SDK

### lakefs-spec
- Emphasize filesystem semantics
- Show data science library integrations
- Include transaction examples
- Demonstrate fsspec compatibility

### Boto3
- Show S3-compatible operations
- Include migration examples from pure S3
- Demonstrate configuration options
- Show hybrid S3/lakeFS patterns

## Templates and Consistency

### Use Provided Templates
- [Code Example Template](_templates/code-example-template.md)
- [API Reference Template](_templates/api-reference-template.md)
- [Tutorial Template](_templates/tutorial-template.md)

### Consistent Patterns
- Use the same parameter names across similar operations
- Follow the same structure for similar content types
- Use consistent terminology throughout
- Maintain the same level of detail across sections

## Review Process

### Content Review Checklist
- [ ] Technical accuracy verified
- [ ] Code examples tested
- [ ] Links work correctly
- [ ] Spelling and grammar checked
- [ ] Follows style guidelines
- [ ] Includes proper cross-references
- [ ] Accessible to target audience

### Code Review Checklist
- [ ] Syntactically correct
- [ ] Follows Python best practices
- [ ] Includes proper error handling
- [ ] Uses realistic examples
- [ ] Properly commented
- [ ] Tested and working

## Maintenance

### Regular Updates
- Review content quarterly for accuracy
- Update code examples with new SDK versions
- Fix broken links
- Update screenshots and diagrams
- Refresh external references

### Version Management
- Tag documentation versions with SDK releases
- Maintain compatibility matrices
- Document breaking changes
- Provide migration guides

## Tools and Automation

### Recommended Tools
- **Linting**: Use markdownlint for consistency
- **Link Checking**: Automated link validation
- **Code Testing**: Automated testing of code examples
- **Spell Check**: Automated spell checking

### Automation
- Set up CI/CD for documentation testing
- Automate link checking
- Test code examples in CI
- Generate API documentation from code

This standards document ensures consistency, quality, and maintainability across all Python documentation in the lakeFS project.