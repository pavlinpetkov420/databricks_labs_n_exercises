# Databricks DE Professional — Lab Backlog
### Free Edition · JIRA-style user stories · July 3, 2026 exam guide

**How to use this:** work top to bottom. Each story has Acceptance Criteria you tick when the *outcome* is true, and Subtasks you tick as you complete the *work*. Hints point you at doc pages and function names — never full solutions. If a hint isn't enough, the doc link will be.

**Sourcing:** every doc link was verified live. ⚠️ **UNVERIFIED** marks anything I could not confirm — those are also your discovery exercises.

---

## Progress tracker

| ID | Title | Epic | Exam sections | Est. | Done |
|---|---|---|---|---|---|
| LAB-01 | Build the workspace foundation | Foundation | 8, 7 | 30 min | [ ] |
| LAB-02 | Create a file landing zone from sample data | Foundation | 2 | 30 min | [ ] |
| LAB-03 | Auto Loader ingestion with schema evolution | Ingestion | 1, 2 | 40 min | [ ] |
| LAB-04 | Streaming table vs materialized view showdown | Pipelines | 1 | 40 min | [ ] |
| LAB-05 | Refresh semantics & the reset guardrail | Pipelines | 1 | 30 min | [ ] |
| LAB-06 | Data quality: expectations and quarantine | Quality | 3, 1 | 40 min | [ ] |
| LAB-07 | AUTO CDC: SCD Type 1 vs Type 2 | Pipelines | 1, 10 | 45 min | [ ] |
| LAB-08 | Multi-task job with control flow | Jobs | 1, 9 | 45 min | [ ] |
| LAB-09 | Break it and repair it | Jobs | 9, 5 | 30 min | [ ] |
| LAB-10 | First bundle deployment | DAB | 1, 9 | 40 min | [ ] |
| LAB-11 | Bundle: variables, targets, environments | DAB | 1, 9 | 45 min | [ ] |
| LAB-12 | Bundle: pipeline + job + Git folder CI/CD | DAB | 9 | 45 min | [ ] |
| LAB-13 | UDFs and the performance hierarchy | Python | 1 | 30 min | [ ] |
| LAB-14 | Unit-testable transformations | Testing | 1 | 40 min | [ ] |
| LAB-15 | Governance: metadata, tags, lineage | Governance | 8 | 35 min | [ ] |
| LAB-16 | Security: row filters, column masks, PII | Security | 7 | 45 min | [ ] |
| LAB-17 | Right-to-be-forgotten purge pipeline | Security | 7, 6 | 40 min | [ ] |
| LAB-18 | Liquid clustering vs partitioning | Performance | 6, 10 | 45 min | [ ] |
| LAB-19 | Query profile forensics | Performance | 6, 9 | 40 min | [ ] |
| LAB-20 | Observability: system tables & event logs | Monitoring | 5 | 45 min | [ ] |
| LAB-21 | Alerting on data quality | Monitoring | 5 | 35 min | [ ] |
| LAB-22 | Delta Sharing recipient investigation | Sharing | 4 | 30 min | [ ] |
| LAB-23 | Dimensional model with point-in-time joins | Modelling | 10 | 50 min | [ ] |
| LAB-24 | Capstone: end-to-end bundle-deployed platform | Capstone | All | 2–3 h | [ ] |

---

## Free Edition constraints — read before starting

- [ ] **One active pipeline per pipeline type.** Pipeline labs run sequentially — stop one before starting the next.
- [ ] **Max 5 concurrent job tasks per account.** Keep DAGs narrow.
- [ ] **No Spark UI, no Spark logs.** Use the [query profile](https://docs.databricks.com/aws/en/sql/user/queries/query-profile) instead.
- [ ] **Serverless triggers:** only `Trigger.AvailableNow()` and `Trigger.Once()`. Omitting a trigger raises `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.
- [ ] **No Scala/R.** Python and SQL only.
- [ ] All cache APIs throw — `df.cache()`, `CACHE TABLE`, `REFRESH TABLE`.

Source: [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) · [serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)

**Your sample data** lives in the `samples` catalog (a Delta Sharing share): `nyctaxi.trips`, `tpch`, `tpcds_sf1`, `wanderbricks`, plus `bakehouse`, `accuweather`, `healthverity`, `sec`. See [sample datasets](https://docs.databricks.com/aws/en/discover/databricks-datasets).

---
---

# EPIC: Foundation

## LAB-01 — Build the workspace foundation

> **As a** data platform engineer
> **I want** a governed catalog, schema, and volume structure with least-privilege groups
> **so that** every later lab has a safe, isolated place to write.

**Exam objectives:** S8 (UC permission inheritance), S7 (ACLs, least privilege)
**Estimate:** 30 min

### Description
Everything downstream writes somewhere. Build that somewhere properly — with a medallion schema layout, a volume for landing files, and at least one group so you can practise inheritance rather than always operating as the owner.

### Acceptance Criteria
- [ ] A catalog `dev_<yourname>` exists with `bronze`, `silver`, `gold`, and `ops` schemas
- [ ] A volume `dev_<yourname>.ops.landing` exists and is writable
- [ ] A group exists with `SELECT` on the catalog but **not** on individual tables
- [ ] You can explain in one sentence why that group still can't query a table without two more privileges
- [ ] `SHOW GRANTS` output for the catalog is captured in a notebook cell

### Subtasks
- [ ] **Create the catalog and schemas** — Use `CREATE CATALOG` / `CREATE SCHEMA`. *Hint:* add a `COMMENT` to each while you're here; LAB-15 depends on it.
- [ ] **Create the volume** — *Hint:* managed volumes need no external location. Path shape is `/Volumes/<catalog>/<schema>/<volume>/`. See [volumes](https://docs.databricks.com/aws/en/volumes).
- [ ] **Create a group and add yourself** — Workspace settings → Identity and access. *Hint:* Free Edition has no account console; workspace admins can add users but not remove them.
- [ ] **Grant `SELECT` at catalog level only** — Then, as a thought experiment, list what else the group needs.
- [ ] **Verify the inheritance chain** — *Hint:* the missing pieces are `USE CATALOG` and `USE SCHEMA`. Inheritance is not traversal. This is exam question shaped.
- [ ] **Record the grant state** — Run `SHOW GRANTS ON CATALOG ...` and keep the output.

### 🎯 Exam connection
Diagnostic Q15 tested exactly this: `SELECT` inherits down the hierarchy, but querying still requires `USE CATALOG` + `USE SCHEMA`.

---

## LAB-02 — Create a file landing zone from sample data

> **As a** data engineer
> **I want** real files on a volume derived from the sample tables
> **so that** I can practise Auto Loader, which needs files rather than tables.

**Exam objectives:** S2 (ingest multiple formats), S1 (Auto Loader)
**Estimate:** 30 min

### Description
The `samples` catalog is read-only tables. Auto Loader reads *files*. Bridge the gap yourself: export slices of sample data into your volume in several formats, in batches, so you have a landing zone you control and can add to later.

### Acceptance Criteria
- [ ] At least 3 JSON files land in `/Volumes/dev_<you>/ops/landing/trips/`, each a different slice of `samples.nyctaxi.trips`
- [ ] The same data also exists as CSV and Parquet in sibling directories
- [ ] One JSON file contains an **extra column** the others don't
- [ ] One JSON file contains at least 5 deliberately **bad records** (nulls in key fields, negative fares)
- [ ] You can list the files with `dbutils.fs.ls` and see distinct file sizes

### Subtasks
- [ ] **Export batch 1** — Read `samples.nyctaxi.trips`, `limit()` a few thousand rows, write JSON to the volume. *Hint:* `.write.format("json").save(path)` writes a *directory* of part-files; that's fine and realistic.
- [ ] **Export batches 2 and 3** — Use a different `WHERE` filter each time so the batches are distinct. *Hint:* filter on `tpep_pickup_datetime` ranges.
- [ ] **Create the schema-drift file** — Add a computed column (e.g. `fare_per_mile`) before writing batch 4. This is LAB-03's evolution trigger.
- [ ] **Create the dirty file** — `union` some rows with nulls and negatives. *Hint:* build them with `spark.createDataFrame` — remember the 128 MB local row-size cap on serverless.
- [ ] **Repeat for CSV and Parquet** — Different directories. *Hint:* note which formats infer types and which don't; that's the LAB-03 lesson.
- [ ] **Verify** — `dbutils.fs.ls` each directory.

### 💡 Note
Keep this notebook. Later labs will re-run it to simulate new data arriving.

---
---

# EPIC: Ingestion

## LAB-03 — Auto Loader ingestion with schema evolution

> **As a** data engineer
> **I want** an incremental file ingestion stream that survives a schema change
> **so that** I understand what `addNewColumns` actually does to a running stream.

**Exam objectives:** S1 (Auto Loader in pipelines), S2 (ingestion)
**Estimate:** 40 min

### Description
Ingest the landing zone incrementally. Then drop the drift file in and watch what happens. The failure is the lesson — don't prevent it.

### Acceptance Criteria
- [ ] A streaming table ingests JSON from the landing volume, exactly-once
- [ ] Each row carries source file name and ingestion timestamp
- [ ] Adding the drift file causes the stream to **fail**, and a restart picks up the new column
- [ ] You can state the default value of `cloudFiles.schemaEvolutionMode` and why it differs when a schema is supplied
- [ ] You've confirmed `_rescued_data` behaviour with a deliberately mistyped value
- [ ] A second run with no new files processes **zero** records

### Subtasks
- [ ] **Build the bronze streaming table** — *Hint:* in SQL use `STREAM read_files(...)`; in Python use `spark.readStream.format("cloudFiles")`. Inside a pipeline, checkpoints are managed for you.
- [ ] **Add file metadata columns** — *Hint:* the `_metadata` column exposes `file_name`, `file_size`, `file_modification_time`.
- [ ] **Run and record the row count.**
- [ ] **Drop in the drift file and re-run** — Expect failure. Read the error text carefully before restarting.
- [ ] **Restart and confirm the new column appears.**
- [ ] **Explain the default** — Write your answer in a markdown cell, then check [schema inference and evolution](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema).
- [ ] **Test idempotency** — Re-run with no new files. Zero rows means exactly-once is working.
- [ ] **Compare formats** — Point a second stream at the CSV directory. *Hint:* JSON/CSV/XML infer everything as strings; Parquet and Avro carry types.

### 🎯 Exam connection
Diagnostic Q3. "Fails, then succeeds on restart" is correct default behaviour, not a bug.

---
---

# EPIC: Pipelines

## LAB-04 — Streaming table vs materialized view showdown

> **As a** data engineer
> **I want** to see a streaming table break on a mutating source while a materialized view doesn't
> **so that** I never again pick the wrong dataset type under exam pressure.

**Exam objectives:** S1 (ST vs MV — highest-value objective)
**Estimate:** 40 min
**⚠️ Depends on:** LAB-03

### Description
This is the single most valuable lab in the backlog. Build both dataset types over the same source, then mutate the source and watch them diverge.

### Acceptance Criteria
- [ ] A pipeline has bronze (ST), `silver_st` (ST), and `silver_mv` (MV) reading the same source
- [ ] After a `MERGE`/`UPDATE` on bronze, `silver_st` **fails** and `silver_mv` **succeeds**
- [ ] You can quote the error message that identifies the cause
- [ ] Adding `skipChangeCommits` stops the failure — and you can **prove the correction was lost**
- [ ] You can state in one line when `skipChangeCommits` is legitimate

### Subtasks
- [ ] **Build the three datasets** — *Hint:* `CREATE OR REFRESH STREAMING TABLE` vs `CREATE OR REFRESH MATERIALIZED VIEW`. Python: `@dp.table` with `readStream` vs a batch read.
- [ ] **Run and confirm both populate.**
- [ ] **Mutate the source** — `UPDATE` a historical row in bronze, e.g. change a `fare_amount`.
- [ ] **Re-run and observe divergence** — Screenshot the error.
- [ ] **Apply `skipChangeCommits` and re-run** — *Hint:* it's a read option on the streaming source.
- [ ] **Prove the loss** — Query the updated row in both targets. `silver_mv` reflects the change; `silver_st` still shows the stale value. **This is the point of the lab.**
- [ ] **Write the decision rule** — Two lines in a markdown cell.

### 🎯 Exam connection
Diagnostic Q1 and Section 1 quiz Q5. Both of your misses live here.

---

## LAB-05 — Refresh semantics & the reset guardrail

> **As a** data engineer
> **I want** to trigger a full refresh and hit the dependency cascade
> **so that** I understand why full refresh is dangerous in production.

**Exam objectives:** S1 (refresh semantics)
**Estimate:** 30 min
**⚠️ Depends on:** LAB-04

### Acceptance Criteria
- [ ] A default refresh and a full refresh produce visibly different behaviour on a streaming table
- [ ] A full refresh of an upstream ST with dependents produces the documented failure
- [ ] `pipelines.reset.allowed = false` blocks a full refresh
- [ ] You can state why a full refresh cannot always recover data
- [ ] For a materialized view, you've confirmed default and full refresh give **the same data**

### Subtasks
- [ ] **Run a default refresh, note row counts.**
- [ ] **Run a full refresh via the UI** — *Hint:* the Full Refresh button, not the normal Start. See [full refresh](https://docs.databricks.com/aws/en/ldp/full-refresh-st).
- [ ] **Observe the truncate-and-rebuild.**
- [ ] **Trigger the cascade failure** — Full refresh an upstream ST that has a dependent. Read the message.
- [ ] **Set the guardrail** — `ALTER TABLE ... SET TBLPROPERTIES ('pipelines.reset.allowed' = 'false')`, then try again.
- [ ] **Reason about Kafka retention** — Write two sentences on why full refresh can't recover expired source data.
- [ ] **Test MV refresh equivalence.**

### 🎯 Exam connection
Section 1 quiz Q4 — you answered with default-refresh behaviour.

---

## LAB-06 — Data quality: expectations and quarantine

> **As a** data engineer
> **I want** bad records dropped from silver but retained for investigation
> **so that** I can implement the quarantine pattern the exam guide names explicitly.

**Exam objectives:** S3 (quarantine process), S1 (pipelines)
**Estimate:** 40 min

### Description
Three requirements that pull against each other: exclude bad rows, keep them queryable, never fail the pipeline. One dataset can't do all three.

### Acceptance Criteria
- [ ] A valid dataset excludes records failing quality rules
- [ ] A quarantine dataset contains exactly those excluded records
- [ ] The pipeline **never fails** on bad data
- [ ] You've tested all three `ON VIOLATION` actions and can describe each
- [ ] Expectation pass/fail counts are visible in the event log
- [ ] A summary query reports the quarantine rate per run

### Subtasks
- [ ] **Define quality rules** — e.g. `fare_amount > 0`, `trip_distance > 0`, non-null pickup zip.
- [ ] **Build the valid dataset** — *Hint:* `EXPECT ... ON VIOLATION DROP ROW`.
- [ ] **Build the quarantine dataset** — *Hint:* same source, negated condition. Two flows from one source.
- [ ] **Test `FAIL UPDATE`** — Watch the pipeline die, then revert. Know what it looks like.
- [ ] **Test the no-action variant** — Records pass through but are counted.
- [ ] **Find the counts in the event log** — *Hint:* they're in `flow_progress` events under the expectations detail. This is exam-tested.
- [ ] **Write the summary query.**

### 🎯 Exam connection
Diagnostic Q5 (correct) and Q10 (correct) — reinforce both.

---

## LAB-07 — AUTO CDC: SCD Type 1 vs Type 2

> **As a** data engineer
> **I want** to apply a change feed with both SCD types
> **so that** I can see history retention and out-of-order handling directly.

**Exam objectives:** S1 (AUTO CDC), S10 (dimensional modelling)
**Estimate:** 45 min

### Description
Synthesise a CDC feed from `samples.tpch.customer` (or `wanderbricks` users), deliberately including out-of-order events, then apply it both ways.

### Acceptance Criteria
- [ ] A change feed table exists with insert, update, and delete operations plus a sequence column
- [ ] At least one event is **deliberately out of order**
- [ ] An SCD Type 1 target shows only current state
- [ ] An SCD Type 2 target shows history with `__START_AT` / `__END_AT`
- [ ] The out-of-order event did **not** overwrite newer state
- [ ] Removing `SEQUENCE BY` produces a validation failure you can quote
- [ ] You've confirmed whether an append flow can share the CDC target

### Subtasks
- [ ] **Build the change feed** — Columns: key, payload, `operation`, `updated_at`. *Hint:* generate updates by re-selecting rows with modified values and later timestamps.
- [ ] **Insert an out-of-order event** — An older `updated_at` arriving after a newer one.
- [ ] **Create the SCD Type 1 target** — *Hint:* target must be a streaming table created *without* a query, then attach the flow. See [AUTO CDC](https://docs.databricks.com/aws/en/ldp/cdc).
- [ ] **Create the SCD Type 2 target** — Compare row counts against Type 1.
- [ ] **Verify out-of-order handling** — Query the key involved. Newer state should win.
- [ ] **Break it** — Remove `SEQUENCE BY`, run, record the error, restore.
- [ ] **Test flow-type exclusivity** — Try attaching an append flow to the CDC target. *Hint:* the [flows page](https://docs.databricks.com/aws/en/ldp/concepts/flows) states a CDC target can only be targeted by other CDC flows. Confirm it.
- [ ] **Write the point-in-time join** — Feeds LAB-23.

### 🎯 Exam connection
Diagnostic Q20, Section 1 quiz Q6.

---
---

# EPIC: Jobs & Orchestration

## LAB-08 — Multi-task job with control flow

> **As a** data engineer
> **I want** a job that branches and loops based on runtime values
> **so that** I can build conditional orchestration without external tooling.

**Exam objectives:** S1 (control flow operators), S9 (deploying)
**Estimate:** 45 min
**⚠️ Watch:** max 5 concurrent job tasks per account

### Acceptance Criteria
- [ ] A job has ≥4 tasks with real dependencies
- [ ] One task sets a **task value** consumed by an If/else condition
- [ ] Different downstream tasks run on the true and false branches
- [ ] A For each task iterates an array with a nested task
- [ ] A cleanup task runs **even when an upstream task fails**, via `Run if`
- [ ] You've confirmed what happens when the condition's source task is **disabled**

### Subtasks
- [ ] **Task A: compute and publish a metric** — *Hint:* `dbutils.jobs.taskValues.set(key=..., value=...)`. Only numeric, string, and boolean survive into conditions.
- [ ] **Task B: If/else condition** — *Hint:* reference syntax is `{{tasks.<task>.values.<key>}}`. See [If/else task](https://docs.databricks.com/aws/en/jobs/if-else).
- [ ] **Tasks C and D on the branches.**
- [ ] **Add a For each task** — Iterate `["manhattan","brooklyn","queens"]` with a nested notebook that filters by the parameter.
- [ ] **Add a cleanup task with `Run if`** — *Hint:* the condition you want is one that tolerates upstream failure. See [task dependencies](https://docs.databricks.com/aws/en/jobs/run-if).
- [ ] **Force a failure and confirm cleanup runs.**
- [ ] **Disable Task A and re-run** — Expect the If/else to fail. Documented trap; verify it.
- [ ] **Add job notifications** — Email on failure. Feeds LAB-21.

### 🎯 Exam connection
Section 1 quiz Q7 (correct) — extend into the disabled-task trap you haven't seen tested.

---

## LAB-09 — Break it and repair it

> **As a** data engineer
> **I want** to repair a partially failed job run with a parameter override
> **so that** I know exactly which tasks re-execute.

**Exam objectives:** S9 (repair runs, parameter overrides), S5 (monitoring)
**Estimate:** 30 min
**⚠️ Depends on:** LAB-08

### Acceptance Criteria
- [ ] A linear 4-task job fails at task 3, leaving task 4 skipped
- [ ] A repair run re-executes tasks 3 **and** 4, not 1 and 2
- [ ] A parameter override applied during repair takes effect
- [ ] You can locate the failure cause without the Spark UI
- [ ] Run history shows the original run and the repair as related

### Subtasks
- [ ] **Build a linear chain** where task 3 fails on a parameter-controlled condition. *Hint:* make it read a job parameter and raise if the value is wrong.
- [ ] **Run it and confirm the fail/skip states.**
- [ ] **Repair with an override** — Repair run → change the parameter.
- [ ] **Record which tasks re-ran** — This is the exam fact.
- [ ] **Diagnose without Spark UI** — Task run output, driver logs, error message. *Hint:* Free Edition has no Spark UI at all; get used to it now.
- [ ] **Inspect via CLI** — `databricks jobs list-runs`, then `get-run`. *Hint:* also worth trying the `system.lakeflow` tables — see LAB-20.

---
---

# EPIC: Declarative Automation Bundles

## LAB-10 — First bundle deployment

> **As a** data engineer
> **I want** a bundle deployed from my laptop to the workspace
> **so that** I understand deployment modes by observing them.

**Exam objectives:** S1 (project structure), S9 (CI/CD)
**Estimate:** 40 min
**Prereq:** Databricks CLI + uv installed, `databricks auth login` working

### Acceptance Criteria
- [ ] `databricks bundle init default-python` scaffolds a project
- [ ] `bundle validate` passes; `bundle deploy` succeeds
- [ ] The deployed job is named `[dev <you>] ...` and its schedule is **paused**
- [ ] Switching to `mode: production` changes both, or produces a validation error you can explain
- [ ] `bundle run <job_key>` triggers the job
- [ ] `bundle destroy` cleans up

### Subtasks
- [ ] **Scaffold and read before deploying** — Open `databricks.yml`, `resources/`, `src/`. Map each file to its purpose.
- [ ] **Validate, then deploy.**
- [ ] **Find the prefix in the workspace** — Jobs list, not the job config page.
- [ ] **Check the schedule state** — Paused. That's `mode: development`.
- [ ] **Switch to production and redeploy** — *Hint:* expect validation errors about `run_as`/`permissions` or user-specific paths. See [deployment modes](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes).
- [ ] **Diff the two modes properly** — *Hint:* `bundle validate -o json` twice, then `diff`. Far clearer than the UI.
- [ ] **Run by job key** — *Hint:* the argument is the YAML resource key, not the display name or numeric ID.
- [ ] **Destroy.**

---

## LAB-11 — Bundle: variables, targets, environments

> **As a** data engineer
> **I want** one bundle deploying to isolated dev and prod namespaces
> **so that** I can simulate promotion despite having one workspace.

**Exam objectives:** S1 (configs, dependencies), S9 (CI/CD)
**Estimate:** 45 min
**⚠️ Depends on:** LAB-10

### Description
Free Edition gives you one workspace, so real cross-workspace promotion is impossible. Simulate it with two targets writing to different catalogs — which is also how you'd parameterise a real bundle.

### Acceptance Criteria
- [ ] A `catalog` variable resolves differently per target
- [ ] Deploying to each target writes to a different schema
- [ ] No catalog or schema name is hardcoded in notebook or task code
- [ ] A third-party PyPI dependency is declared **in the bundle**, not `%pip`
- [ ] The job task uses a serverless environment spec
- [ ] You can explain why `libraries:` doesn't apply to a serverless task

### Subtasks
- [ ] **Declare variables** — *Hint:* top-level `variables:` block, overridden per target.
- [ ] **Reference them** — `${var.catalog}` in resource definitions and as job parameters.
- [ ] **Deploy to both targets and verify isolation.**
- [ ] **Add a dependency** — *Hint:* serverless tasks use `environment_key` + an `environments:` block with `environment_version` and `dependencies`. See [bundle examples](https://docs.databricks.com/aws/en/dev-tools/bundles/examples).
- [ ] **Pin the version** — Databricks recommends against unpinned installs on serverless.
- [ ] **Try a local wheel** — Build a tiny wheel, reference it from a UC volume. *Hint:* [library dependencies](https://docs.databricks.com/aws/en/dev-tools/bundles/library-dependencies).
- [ ] **Document the scope hierarchy** — Where does a bundle-declared library sit in the precedence order from [install libraries](https://docs.databricks.com/aws/en/libraries/)?

---

## LAB-12 — Bundle: pipeline + job + Git folder CI/CD

> **As a** data engineer
> **I want** my pipeline and its orchestrating job defined in one bundle, backed by Git
> **so that** the whole platform is reproducible from source.

**Exam objectives:** S9 (bundles, Git folders, CI/CD)
**Estimate:** 45 min
**⚠️ Depends on:** LAB-11, and a pipeline from LAB-04/06/07

### Acceptance Criteria
- [ ] A bundle defines **both** a pipeline and a job that triggers it
- [ ] The job references the pipeline by resource key, not a hardcoded ID
- [ ] The repository is connected as a Databricks Git folder
- [ ] A branch → commit → push → redeploy cycle works end to end
- [ ] Pipeline `development` mode differs between targets
- [ ] ⚠️ **UNVERIFIED — discover this:** can you create a workspace-level service principal on Free Edition for `run_as`?

### Subtasks
- [ ] **Move pipeline source into the bundle** under `src/`.
- [ ] **Define the pipeline resource** — *Hint:* `resources.pipelines.<key>` with `libraries`/`root_path` pointing at your source.
- [ ] **Add a job with a pipeline task** — *Hint:* reference `${resources.pipelines.<key>.id}`.
- [ ] **Connect the Git folder** — Workspace → Git folder → clone your repo.
- [ ] **Do a full commit cycle.**
- [ ] **Compare pipeline dev flags across targets** — Development mode sets pipelines `development: true`.
- [ ] **Investigate service principals** — Record what you find; it's an open question in the capabilities doc.

---
---

# EPIC: Python & Testing

## LAB-13 — UDFs and the performance hierarchy

> **As a** data engineer
> **I want** measured evidence of UDF performance differences
> **so that** the hierarchy is intuition rather than memorisation.

**Exam objectives:** S1 (Pandas/Python UDFs)
**Estimate:** 30 min

### Acceptance Criteria
- [ ] The same logic is implemented as a Python UDF, a pandas UDF, and a built-in
- [ ] Timings are recorded and ordered as expected
- [ ] All four pandas UDF variants are implemented at least once
- [ ] You've measured the actual Arrow batch size
- [ ] You've hit or approached the 1 GB UDF memory cap deliberately
- [ ] ⚠️ **UNVERIFIED — discover this:** can you set `spark.sql.execution.arrow.maxRecordsPerBatch` on serverless?

### Subtasks
- [ ] **Implement all three approaches** over `samples.nyctaxi.trips`.
- [ ] **Time them** — *Hint:* write to `format("noop")` to force execution without I/O cost.
- [ ] **Series → Series** — the default variant.
- [ ] **Iterator[Series]** — *Hint:* the point is one-time setup; simulate an expensive init.
- [ ] **Iterator[Tuple[Series,...]]** — multiple columns.
- [ ] **Series → scalar** — *Hint:* used with `groupBy().agg()` and window functions; it receives the whole group.
- [ ] **Measure batch size** — Return `len(series)` from a probe UDF and aggregate.
- [ ] **Push toward the memory cap** — Fat string payloads plus a skewed `groupBy` key. *Hint:* the Series→scalar variant is where this bites.
- [ ] **Test the config** — Set it, read it back with `spark.conf.get`. If unchanged, record that finding.

---

## LAB-14 — Unit-testable transformations

> **As a** data engineer
> **I want** transformation logic in importable modules with real tests
> **so that** correctness is verified before deployment.

**Exam objectives:** S1 (testing, `DataFrame.transform`)
**Estimate:** 40 min

### Acceptance Criteria
- [ ] Transformation logic lives in `src/` as importable functions, not notebook cells
- [ ] Functions are composed via `DataFrame.transform`
- [ ] Tests use both `assertSchemaEqual` and `assertDataFrameEqual`
- [ ] A test proves row order does **not** cause failure by default
- [ ] A schema-drift test fails on a type change
- [ ] Tests run as a job task in the bundle

### Subtasks
- [ ] **Extract 3 transformation functions** — Each takes and returns a DataFrame.
- [ ] **Compose with `.transform()`** — Chain them.
- [ ] **Write schema-first tests** — *Hint:* Databricks guidance is to assert schema before data; `assertDataFrameEqual` collects both DataFrames to the driver.
- [ ] **Prove the row-order default** — Shuffle the expected rows; the assertion should still pass. *Hint:* check `checkRowOrder`'s default in the [API reference](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.testing.assertDataFrameEqual.html).
- [ ] **Test float tolerance** — Use `rtol` / `atol`.
- [ ] **Add a failing test deliberately** — Watch which assertion fires and what it prints.
- [ ] **Wire tests into the bundle** — A task that runs them before the ETL task.
- [ ] **Try a streaming DataFrame** — It should raise. Know the message.

---
---

# EPIC: Governance & Security

## LAB-15 — Governance: metadata, tags, lineage

> **As a** data steward
> **I want** discoverable, documented, tagged data assets
> **so that** PII columns can be found programmatically across the metastore.

**Exam objectives:** S8 (descriptions/metadata, discoverability)
**Estimate:** 35 min

### Acceptance Criteria
- [ ] Every table and key column has a `COMMENT`
- [ ] PII columns are tagged
- [ ] A single query returns **all** PII-tagged columns across the catalog
- [ ] Lineage is visible for a pipeline-produced table
- [ ] You can explain why tags beat a `pii_` naming convention
- [ ] ⚠️ **UNVERIFIED — discover this:** are UC tags and lineage available on Free Edition?

### Subtasks
- [ ] **Comment your tables and columns** — *Hint:* `COMMENT ON TABLE` / `ALTER TABLE ... ALTER COLUMN ... COMMENT`.
- [ ] **Apply tags** — *Hint:* `ALTER TABLE ... SET TAGS`. If this fails, record the error; that answers an open question.
- [ ] **Query tags metastore-wide** — *Hint:* look in `information_schema` for tag views.
- [ ] **Check lineage** — Catalog Explorer → table → Lineage.
- [ ] **Test search** — Can you find a table by its comment text?
- [ ] **Write the justification** — Why is a naming convention insufficient? *Hint:* can you `GRANT` on a prefix? Can you query one?

### 🎯 Exam connection
Diagnostic Q16 — you chose naming conventions.

---

## LAB-16 — Security: row filters, column masks, PII

> **As a** security engineer
> **I want** column-level masking and row-level filtering applied automatically
> **so that** protection holds regardless of how the table is queried.

**Exam objectives:** S7 (row filters, column masks, anonymization)
**Estimate:** 45 min
**⚠️ Depends on:** LAB-01 (groups)

### Description
Use a dataset with plausible PII — `samples.wanderbricks` users or `bakehouse` customers. Implement masking three ways and compare.

### Acceptance Criteria
- [ ] A column mask redacts a PII column for non-privileged groups
- [ ] A row filter restricts visible rows by group membership
- [ ] The protection applies from SQL editor **and** from a notebook
- [ ] A dynamic view achieves similar results — and you can state the trade-off
- [ ] Hashing, tokenization, and generalization are each demonstrated
- [ ] You can explain when a column mask beats a dynamic view

### Subtasks
- [ ] **Create a masking function** — *Hint:* a SQL UDF returning either the value or `'REDACTED'` based on `is_member()`.
- [ ] **Attach it as a column mask** — *Hint:* `ALTER TABLE ... ALTER COLUMN ... SET MASK`.
- [ ] **Test as both privileged and unprivileged** — Remove yourself from the group temporarily.
- [ ] **Add a row filter** — Restrict by region or franchise.
- [ ] **Build the dynamic view equivalent** — *Hint:* `CASE WHEN is_member(...)`. Then ask: what stops someone querying the base table?
- [ ] **Demonstrate hashing** — *Hint:* `sha2()`; consider whether a salt is needed.
- [ ] **Demonstrate generalization** — Exact age → age band; exact timestamp → date.
- [ ] **Write the comparison** — Mask vs view: which needs base-table permissions revoked?

### 🎯 Exam connection
Diagnostic Q13 (correct) — deepen it with the mask-vs-view trade-off.

---

## LAB-17 — Right-to-be-forgotten purge pipeline

> **As a** compliance engineer
> **I want** a purge process that actually removes data
> **so that** I understand why `DELETE` alone is not compliance.

**Exam objectives:** S7 (purging, retention), S6 (deletion vectors, VACUUM)
**Estimate:** 40 min

### Acceptance Criteria
- [ ] A `DELETE` removes rows from the current version
- [ ] You've **proven the data is still retrievable** via time travel afterwards
- [ ] `VACUUM` with default retention does **not** remove the files, and you know why
- [ ] You can state the default retention threshold
- [ ] Deletion vector behaviour is observed in `DESCRIBE HISTORY` / `DESCRIBE DETAIL`
- [ ] A documented, repeatable purge runbook exists

### Subtasks
- [ ] **Delete a customer's rows.**
- [ ] **Query the prior version** — *Hint:* `VERSION AS OF`. The data is still there. This is the whole lesson.
- [ ] **Inspect the operation** — `DESCRIBE HISTORY`. Did files get rewritten, or were rows marked?
- [ ] **Run VACUUM with defaults** — Confirm nothing is removed. Why?
- [ ] **Find the retention threshold** — Read it from the table properties, don't recall it.
- [ ] **Write the runbook** — Steps and their timing constraints. Note the risk of shortening retention.
- [ ] **Connect to deletion vectors** — With DVs on, what does a `DELETE` physically do?

### 🎯 Exam connection
Diagnostic Q11 and Q14 — both correct; this converts recall into demonstrated understanding.

---
---

# EPIC: Performance & Monitoring

## LAB-18 — Liquid clustering vs partitioning

> **As a** data engineer
> **I want** to compare layout strategies on a large table
> **so that** I can justify liquid clustering over partitioning with evidence.

**Exam objectives:** S10 (liquid clustering vs partitioning/ZORDER), S6
**Estimate:** 45 min

### Description
Use `samples.tpcds_sf1` (~1 GB) or a large slice of `nyctaxi.trips`. Build the same data three ways and measure.

### Acceptance Criteria
- [ ] Three copies exist: unoptimized, partitioned, liquid-clustered
- [ ] Query times and files-scanned counts are recorded for each
- [ ] `CLUSTER BY AUTO` is attempted — and you know which table types accept it
- [ ] You can state the maximum number of clustering keys
- [ ] You can explain why more keys can make filtering **worse**
- [ ] You've confirmed liquid clustering and partitioning are mutually exclusive

### Subtasks
- [ ] **Build the three tables** — Same data, different layouts.
- [ ] **Partition on something high-cardinality deliberately** — Observe the small-file problem. Count the files.
- [ ] **Apply liquid clustering** — *Hint:* `CLUSTER BY` on 1–2 columns that actually appear in filters.
- [ ] **Try `CLUSTER BY AUTO`** — *Hint:* it's restricted to a specific table type. Which? Diagnostic Q19 turned on this.
- [ ] **Measure** — Same filter query against each; record duration and files pruned from the query profile.
- [ ] **Test the key-count limit** — Add keys until it refuses. Record the ceiling.
- [ ] **Try combining partitioning with clustering** — Expect refusal.
- [ ] **Write the decision rule.**

### 🎯 Exam connection
Diagnostic Q19 — you were split between two answers; the deciding word was "external".

---

## LAB-19 — Query profile forensics

> **As a** data engineer
> **I want** to diagnose bad data skipping and join problems from the query profile
> **so that** I can work without the Spark UI.

**Exam objectives:** S6 (query profile, bottlenecks), S9 (debugging)
**Estimate:** 40 min
**⚠️ Note:** the exam guide names the Spark UI; Free Edition has none. This lab is the substitute.

### Acceptance Criteria
- [ ] A deliberately bad query is diagnosed from its profile alone
- [ ] Files pruned vs files scanned is located and interpreted
- [ ] A broadcast join and a shuffle join are compared in the profile
- [ ] A spill is induced and identified
- [ ] You can explain why a filter on the 150th column may not prune
- [ ] Data skew is visible in task duration distribution

### Subtasks
- [ ] **Join two large tpch tables** — Read the profile end to end.
- [ ] **Find the pruning stats** — Where in the profile? What do they mean?
- [ ] **Force a broadcast, then prevent it** — *Hint:* `broadcast()` hint vs raising/lowering the threshold. Compare profiles.
- [ ] **Induce a spill** — Big shuffle, insufficient memory. Find the spill metric.
- [ ] **Test the 32-column stats limit** — Filter on an early column vs a very late one. *Hint:* diagnostic Q12. Then check the table property that controls it.
- [ ] **Create skew** — Group by a heavily imbalanced key; look at task duration spread.
- [ ] **Write a diagnostic checklist** — Your own, from what you saw.

---

## LAB-20 — Observability: system tables & event logs

> **As a** platform engineer
> **I want** to query system tables and pipeline event logs
> **so that** I can monitor cost, jobs, and quality programmatically.

**Exam objectives:** S5 (system tables, event logs, REST/CLI monitoring)
**Estimate:** 45 min

### Acceptance Criteria
- [ ] ⚠️ **UNVERIFIED — this lab's first job is to find out:** which system schemas are available on Free Edition
- [ ] If available: a query reports job runs joined to their compute
- [ ] If available: a query reports DBU cost attribution
- [ ] Pipeline event log queries return expectation metrics
- [ ] The Jobs REST API / CLI returns run history
- [ ] You can name all six `system.lakeflow` tables from memory afterwards

### Subtasks
- [ ] **Discover what exists** — `SHOW SCHEMAS IN system;` Record the result; it answers an open question in your capabilities doc.
- [ ] **Explore `system.lakeflow`** — *Hint:* six tables; note this schema was formerly `workflow`.
- [ ] **Join runs to compute** — Which tables? *Hint:* diagnostic Q9 — `system.jobs.runs` does not exist.
- [ ] **Attribute cost** — `system.billing.usage` plus list prices.
- [ ] **Query the pipeline event log** — *Hint:* expectation metrics live in `flow_progress` events.
- [ ] **Use the CLI** — `databricks jobs list-runs`; compare with the system tables.
- [ ] **If system tables are unavailable** — Document the gap and build the equivalent from the REST API instead. That's a legitimate result.

### 🎯 Exam connection
Diagnostic Q9 (missed) and Q10 (correct).

---

## LAB-21 — Alerting on data quality

> **As a** platform engineer
> **I want** automated alerts when quality degrades or jobs fail
> **so that** problems surface without someone watching a dashboard.

**Exam objectives:** S5 (SQL Alerts, job notifications)
**Estimate:** 35 min
**⚠️ Depends on:** LAB-06

### Acceptance Criteria
- [ ] A SQL query computes a quality metric (e.g. quarantine rate)
- [ ] An alert fires when it crosses a threshold
- [ ] Job notifications are configured for failure and long runtime
- [ ] ⚠️ **UNVERIFIED — discover this:** are SQL Alerts available on Free Edition, and which version?
- [ ] You can distinguish the legacy alerts feature from the current one

### Subtasks
- [ ] **Write the metric query** — Quarantine rate per run from LAB-06.
- [ ] **Create an alert** — SQL → Alerts. Record whether it works and which UI version appears.
- [ ] **Trigger it deliberately** — Load bad data until the threshold breaks.
- [ ] **Add job notifications** — Failure, and duration warning.
- [ ] **Compare the two mechanisms** — When would you use a SQL alert over a job notification?

---
---

# EPIC: Sharing & Modelling

## LAB-22 — Delta Sharing recipient investigation

> **As a** data engineer
> **I want** to inspect the share I already receive and test provider-side operations
> **so that** I can practise Section 4 despite having one metastore.

**Exam objectives:** S4 (Delta Sharing D2D/D2O, federation)
**Estimate:** 30 min

### Description
Your `samples` catalog arrives under **Shares received** — you are already a Delta Sharing recipient. That's free Section 4 practice. Then find out how far the provider side goes on Free Edition.

### Acceptance Criteria
- [ ] The `samples` share is inspected as a recipient
- [ ] You can state what a recipient can and cannot do to shared data
- [ ] ⚠️ **UNVERIFIED — discover this:** can you create a share and a recipient on Free Edition?
- [ ] You can explain the difference between D2D and D2O and what each needs
- [ ] Federation capability is tested and the result recorded

### Subtasks
- [ ] **Inspect the received share** — *Hint:* `SHOW SHARES`, and explore `samples` in Catalog Explorer. Note it's a share, not a normal catalog.
- [ ] **Test recipient limits** — Try writing to a `samples` table. Record the error.
- [ ] **Attempt provider-side operations** — `CREATE SHARE`, `CREATE RECIPIENT`. Record results either way.
- [ ] **Work out the D2D requirement** — Why does D2D need a sharing identifier, and why is that hard with one metastore?
- [ ] **Test federation** — `SHOW CONNECTIONS`. *Hint:* outbound internet is restricted to trusted domains unless LinkedIn-verified — that's likely your blocker.
- [ ] **Optional stretch** — LinkedIn-verify, stand up a free Postgres, create a connection and foreign catalog.

### 🎯 Exam connection
Diagnostic Q7 (missed — you chose federation when open sharing was correct) and Q8 (correct).

---

## LAB-23 — Dimensional model with point-in-time joins

> **As a** data engineer
> **I want** a star schema with a Type 2 dimension queried as-of transaction time
> **so that** historical reporting is correct.

**Exam objectives:** S10 (dimensional models), S3 (window functions, joins)
**Estimate:** 50 min
**⚠️ Depends on:** LAB-07

### Acceptance Criteria
- [ ] A fact table and ≥2 dimensions exist, one of them SCD Type 2
- [ ] A point-in-time join returns the dimension version in effect at transaction time
- [ ] The join handles the open-ended current record correctly
- [ ] A window-function query computes a running total with the correct frame
- [ ] Layout is optimised with liquid clustering on real filter columns
- [ ] You can explain why `BETWEEN __START_AT AND __END_AT` alone is wrong

### Subtasks
- [ ] **Design the star** — Use `samples.tpch` (orders/customer/part) or `wanderbricks`.
- [ ] **Build the Type 2 dimension** — Reuse LAB-07's output.
- [ ] **Write the point-in-time join** — *Hint:* the predicate needs a NULL guard for the current record. Diagnostic Q20.
- [ ] **Prove it works** — Find a key with multiple versions; confirm old facts join to old dimension rows.
- [ ] **Write a running total** — *Hint:* with `ORDER BY` and no explicit frame, the default is `RANGE`, which groups peer rows. Diagnostic Q6 turned on this. Compare `ROWS` vs `RANGE` on tied values.
- [ ] **Optimise the layout** — Cluster on actual filter columns.
- [ ] **Document the model.**

### 🎯 Exam connection
Diagnostic Q6 (missed — RANGE vs ROWS) and Q20 (correct).

---
---

# EPIC: Capstone

## LAB-24 — End-to-end bundle-deployed platform

> **As a** data engineer
> **I want** every concept assembled into one deployable project
> **so that** I can prove exam readiness by building rather than recalling.

**Exam objectives:** All ten sections
**Estimate:** 2–3 hours
**⚠️ Depends on:** everything above

### Description
No new concepts. Assemble what you've built into a single bundle-deployed platform, from Git, with governance and monitoring. If any piece stalls you, that piece is your next study target.

### Acceptance Criteria
- [ ] Bundle in Git deploys the whole platform to two targets
- [ ] Auto Loader → bronze → silver (with quarantine) → gold
- [ ] One dimension maintained by AUTO CDC as SCD Type 2
- [ ] Orchestrating job with branching, looping, and failure-tolerant cleanup
- [ ] All tables commented and PII-tagged; masks and row filters applied
- [ ] Gold tables liquid-clustered on real filter columns
- [ ] Monitoring queries for cost, run history, and quality
- [ ] An alert fires on quality degradation
- [ ] Unit tests run as a task before the ETL
- [ ] Full teardown and clean redeploy from scratch succeeds
- [ ] A README documents the architecture and every design decision

### Subtasks
- [ ] **Consolidate source into one repo.**
- [ ] **Define all resources in the bundle** — pipeline, jobs, schedules.
- [ ] **Wire governance in** — tags and masks applied by a task, not by hand.
- [ ] **Add the monitoring layer.**
- [ ] **Tear down completely** — `bundle destroy`, drop the catalog.
- [ ] **Rebuild from a clean clone** — This is the real test. Anything you can't rebuild, you didn't learn.
- [ ] **Write the README** — Include why you chose each dataset type, layout, and masking approach.
- [ ] **List what you couldn't do** — And which exam objectives those gaps map to.

---
---

# Open questions this backlog will answer

These are unresolved in the Free Edition capabilities doc. Record answers as you hit them.

- [ ] **System tables** — available? Which schemas? (LAB-20)
- [ ] **UC tags** — can you `SET TAGS`? (LAB-15)
- [ ] **Lineage** — captured and visible? (LAB-15)
- [ ] **SQL Alerts** — available, and which version? (LAB-21)
- [ ] **Delta Sharing provider side** — can you `CREATE SHARE`? (LAB-22)
- [ ] **Lakehouse Federation** — blocked without LinkedIn verification? (LAB-22)
- [ ] **Service principals** — creatable at workspace level? (LAB-12)
- [ ] **Arrow batch size config** — settable on serverless? (LAB-13)
- [ ] **Predictive optimization** — enabled on managed tables? (LAB-18)
- [ ] **VS Code extension** — works against a serverless-only workspace? (LAB-14)

---

# Suggested order

**Sprint 1 — Foundation & pipelines (highest exam weight)**
LAB-01 → 02 → 03 → 04 → 05 → 06 → 07

**Sprint 2 — Orchestration & deployment**
LAB-08 → 09 → 10 → 11 → 12

**Sprint 3 — Breadth**
LAB-13 → 14 → 15 → 16 → 17

**Sprint 4 — Performance, monitoring, modelling**
LAB-18 → 19 → 20 → 21 → 22 → 23

**Sprint 5 — Capstone**
LAB-24

---

*Built 2026-08-14 against the July 3, 2026 exam guide and verified Free Edition limitations. Labs marked ⚠️ UNVERIFIED contain genuine open questions — your findings are worth more than my guesses.*
