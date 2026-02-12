# Garbage Collection — Spark 4.0 Support

## Problem Description

Users running Spark 4.0 cannot use lakeFS Garbage Collection. The GC job runs as a Spark application via the lakeFS Spark metadata client, which today only publishes a Scala 2.12 artifact (`lakefs-spark-client_2.12`).

The main challenge is that Spark 4.0 dropped Scala 2.12 support entirely — Scala 2.13 is now the only option. Since Scala 2.12 and 2.13 are binary incompatible, the existing jar simply cannot be loaded on a Spark 4.0 runtime. Spark 4.0 also requires Java 17+ (up from Java 8), though this is a smaller concern.

## Proposed Solution

Cross-compile the Spark metadata client for both Scala 2.12 and 2.13, publishing two artifacts from the same source: `lakefs-spark-client_2.12` (Spark 3.x) and `lakefs-spark-client_2.13` (Spark 4.x). This is viable because:

1. The two internal lakeFS dependencies (Java SDK and HadoopFS) are pure Java — they work on both Scala versions unchanged, with no separate releases needed.
2. The Scala 2.13 source migration is trivial — 8 import changes and 5 syntax fixes in a ~3,500 line codebase.
3. All third-party `%%` dependencies already publish `_2.13` artifacts.
4. The build already shades `scala-collection-compat`, which bridges the collection API differences between 2.12 and 2.13.

## Dependency Analysis

The Spark client has two internal lakeFS dependencies:

### Java SDK (`io.lakefs:sdk`)

- Pure Java artifact (no Scala suffix) — **not affected by Scala version changes**
- Used in `ApiClient.scala`, `UncommittedAddressLister.scala`, `ActiveCommitsAddressLister.scala`
- Transitive okhttp dependency is already shaded in the Spark assembly jar
- Same artifact works on both Spark 3.x and 4.x — no changes needed

### HadoopFS (`io.lakefs:hadoop-lakefs`)

- Pure Java artifact — **not affected by Scala version changes**
- Not a build dependency of the Spark client (zero imports). Loaded at runtime by Hadoop's FileSystem service loader if users want `lakefs://` URI support
- Not bundled in the Spark assembly jar
- Same artifact works on both Spark 3.x and 4.x — no changes needed

### Spark Client (`io.lakefs:lakefs-spark-client_2.12`)

- Scala artifact — **must be cross-compiled for 2.13**
- This is the only component that needs work

## Scala 2.13 Source Migration

The codebase is small (28 main files, ~3,500 lines) and clean. Migration issues found:

| Category                                  | Count                    | Details                                                    |
|-------------------------------------------|--------------------------|------------------------------------------------------------|
| `JavaConverters` → `CollectionConverters` | 8 imports across 6 files | Use `scala-collection-compat` (already shaded in assembly) |
| Procedure syntax (`def foo() {`)          | 5 methods in 3 files     | Add `: Unit =`                                             |

Not found (no issues): `JavaConversions`, `Stream`, `Traversable`, `CanBuildFrom`, `mapValues`/`filterKeys`, `javax.servlet`, Either projections.

All `%%` dependencies (json4s, scallop, scalapb, scalatest, testcontainers-scala, upickle, os-lib, scalameter) publish `_2.13` artifacts at the versions currently used.

## Spark 4.0 Breaking Changes Impact

| Change                          | Impact on GC                                                           |
|---------------------------------|------------------------------------------------------------------------|
| Scala 2.12 dropped              | **Blocker** — requires cross-build                                     |
| Java 8/11 dropped               | Build/CI change — update targets to Java 17                            |
| javax.servlet → jakarta.servlet | **Not affected** — zero servlet references in codebase                 |
| Hadoop 3.3 → 3.4.1              | Minor — already on 3.3.6, Hadoop maintains compat within major version |
| ANSI SQL mode default           | Low risk — GC uses DataFrame joins/filters, not SQL string coercions   |

## Existing Issues

No issues or PRs tracking Spark 4.0 or Scala 2.13 exist on the repository. Related open issues:

- **#9034** (P2) — Make GC work with EMR 7.x.x (Spark 3.5 + Java 17)
- **#10041** — Metadata client jar on EMR 7.12.0
- **#8965** (P3) — Update Hadoop dependencies of spark-client
- **#8576** (P2) — Java SDK okhttp conflict with Spark runtime

## Implementation Plan

### Phase 1: Source compatibility

Make the Scala source compile under both 2.12 and 2.13 without version-specific source directories.

1. Add `scala-collection-compat` as a dependency (it's already shaded in assembly rules)
2. Replace all 8 `import scala.collection.JavaConverters._` with `import scala.jdk.CollectionConverters._` (provided by compat lib on 2.12, native on 2.13)
3. Fix 5 procedure syntax definitions (add `: Unit =`)
4. Verify the above compiles on both 2.12 and 2.13

### Phase 2: Cross-build setup

Configure sbt to produce both `_2.12` and `_2.13` artifacts.

1. Add `crossScalaVersions := Seq("2.12.18", "2.13.14")` to `build.sbt` (also bump 2.12.12 → 2.12.18 for bug fixes)
2. Vary the Spark dependency by Scala version:
   ```scala
   "org.apache.spark" %% "spark-sql" % (if (scalaBinaryVersion.value == "2.12") "3.5.4" else "4.0.0") % "provided"
   ```
3. Vary `scalacOptions` by Scala version — keep Java 8 target for 2.12 (Spark 3.x backward compatibility), use Java 11 target for 2.13 (Scala 2.13 minimum)
4. Conditionally apply Jackson `dependencyOverrides` only for 2.12 (not needed on 2.13)
5. Verify `sbt +test` passes on both versions

### Phase 3: CI and publishing

1. Update `spark.yaml` — use Java 17 for builds, run `sbt +test` to test both Scala versions
2. Update publish workflows to produce both `_2.12` and `_2.13` assembly jars
3. Update the S3 upload task to upload both jars with appropriate names
4. Upd[lakefs-spark-client-assembly-0.19.0-expr.jar](../../clients/spark/target/scala-2.13/lakefs-spark-client-assembly-0.19.0-expr.jar)ate Maven publishing to push both cross-compiled artifacts

### Phase 4: Documentation

1. Update GC docs with Spark 4.0 jar name
2. Update Spark client compatibility matrix
3. Note that Java SDK and HadoopFS artifacts are unchanged

## Risks

| Risk                                                     | Mitigation                                                                                                       |
|----------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| Runtime behavior differences between Spark 3.x and 4.x   | Integration test GC on both Spark 3.5 and 4.0                                                                    |
| Credential provider changes (seen in PR #9013 for EMR 7) | Already handled — PR #9013 merged the fix for Hadoop 3.3.6 credential providers                                  |
| ANSI SQL mode breaking DataFrame operations              | Test all GC DataFrame logic with `spark.sql.ansi.enabled=true` on Spark 3.x before the 4.0 move                  |
| Assembly jar size / dependency conflicts on 2.13         | Verify shading rules still work; the existing `scala.collection.compat` shade rule suggests this was anticipated |