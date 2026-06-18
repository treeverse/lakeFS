## Generated code

Some files in the repository contain generated code.  These include:
- All files `*.gen.go`.
- All files under `clients/java`.
- All files under `clients/python`.
- All files under `clients/rust`.

DO NOT make any stylistic suggestions to these files.  Only make suggestions to these files for actual errors.

### Examples

#### Not an error, do NOT report suggest a fix

In file clients/java/src/main/java/io/lakefs/clients/sdk/AuthApi.java


```java
        } else if ( localBasePaths.length > 0 ) {
            basePath = localBasePaths[localHostIndex];
```

Do NOT comment
> Test is always false.

This is generated code, and there is no harm in this code.

#### Error, report and suggest a fix

If a generated file in clients/python contains code similar to this:

```py
  sql_statement = s"SELECT $columns FROM $table LIMIT 1"
```

then it may be susceptible to SQL injection.  Report it unless variables `column` and `table` are very clearly safe to use.
