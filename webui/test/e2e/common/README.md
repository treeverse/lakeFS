# lafeFS Common E2E Tests

Tests in this folder are applicable to any combination of database and block adapter.
Some features (e.g., import) aren't supported with the local block adapter.
Tests that would fail on a local block adapter can be tagged with `@exclude-local`:

```JavaScript
test("Import @exclude-local", async ({ page }) => {
    // ...
});
```

When running tests using the local adapter, these tests can be excluded:

```bash
npx playwright test --grep-invert @exclude-local
```
