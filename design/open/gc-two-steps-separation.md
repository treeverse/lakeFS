# Separate GC into two separately runnable steps

## Motivation

Improve the resilience of GC (separation of concerns, pure functions, and traceability) and allow examination
of intermediate results.

## How?

The `lakefs.debug.gc.no_delete` flag lets users run GC and generate intermediate results,
yet GC doesn't continue over those results, and retrieving them manually is quite tedious.  
Both problems can be solved by introducing two flags and changing the `lakefs.debug.gc.no_delete`:

1. `lakefs.debug.gc.no_delete` -> `lakefs.gc.do_mark` (Boolean: default = true): This flag instructs the GC operation to mark, i.e. collect, the addresses that are intended to be deleted.
2. `lakefs.gc.do_sweep` (Boolean: default = true): The GC operation sweep, i.e. delete, the marked addresses.
3. `lakefs.gc.mark_id` (String): This flag specifies which ID the GC will use to generate intermediate results and write outputs to.

## Flags permutations

#### Only Mark

In the case of `lakefs.gc.do_sweep=false`, the GC will run, collect the addresses to be deleted, and write them to:
`STORAGE_NAMESPACE/_lakefs/retention/gc/addresses/mark_id=RANDOM_VALUE/`.

#### Only Mark and Mark ID

If `lakefs.gc.do_sweep=false`, and a `lakefs.gc.mark_id` is provided,
the GC will run and collect the addresses to be deleted, and use the `lakefs.gc.mark_id` as the `MARK_ID` in:
i.e. `STORAGE_NAMESPACE/_lakefs/retention/gc/addresses/mark_id=MARK_ID/`.

#### Only Sweep and Mark ID

If `lakefs.gc.do_mark=false`, the GC will use the provided `lakefs.gc.mark_id` and delete the objects mapped
from the addresses found under `STORAGE_NAMESPACE/_lakefs/retention/gc/addresses/mark_id=MARK_ID/`.

#### Only Sweep

If `lakefs.gc.do_mark=false`, and `lakefs.gc.mark_id` is not provided (or empty), the operation will fail.

#### Mark ID

GC will use the `lakefs.gc.mark_id` to write intermediate results to `STORAGE_NAMESPACE/_lakefs/retention/gc/addresses/mark_id=MARK_ID/`,
and will do a complete run (marking and sweeping).

#### No Mark and No Sweep

The operation will fail.

#### No flags are provided

Complete run of GC, including marking and sweeping (while writing intermediate results to `STORAGE_NAMESPACE/_lakefs/retention/gc/addresses/mark_id=RANDOM_VALUE/`).

### Note

The use of the `lakefs.gc.mark_id` flag is under the responsibility and management of the users.
It should be made clear to users that using it sequentially with the same mark ID may override results.

## Current behavior (and why we don't need the `run_id=...` with addresses)

According to the current behavior of GC, intermediate results are written to `STORAGE_NAMESPACE/_lakefs/retention/gc/addresses/run_id=RUN_ID/`.
There is no need to use run IDs here. It will be possible to limit the work of GC by using Run IDs, but this will be done with **commits** rather than addresses.
Mark ID seems to make more sense than run ID.
