# CI/CD in Azure DevOps (`azure-pipelines/workflows/`)

Four Azure DevOps pipelines — exact mirrors of the GitHub workflows in `.github/workflows/`.
All of them enforce the same branch policy: **non-dev stages deploy from `main` only;
`dev` may deploy from any branch.**

## Stage Registry (single source of truth)

Stage names are defined in ONE place: **`configs/environments/env_settings.json`** —
its top-level keys are the stages, its values hold each stage's catalogs/schemas.

* Pipelines: the `stage` parameter is free text (no dropdown), validated **at runtime**
  against the registry by a `Validate Stage Against Registry` step (an unknown stage fails
  immediately with the list of available stages).
* `lakeforge deploy`: reads its supported stages from the same file.
* Databricks bundles: the `targets:` blocks of the root bundle, `deployments/_template/`,
  and every `deployments/<client_id>/` are **generated** from the registry via
  `lakeforge sync-stages` (or `python -m lakeforge.cli.sync_stages`).

**To add / rename / remove a stage** (e.g. switch to just `dev` + `qa` + `prod`):

1. Edit `configs/environments/env_settings.json` (rename/remove/add keys and their settings).
2. Run `lakeforge sync-stages` — regenerates every bundle's targets (`--check` = dry run).
3. Commit. Pipelines, the CLI, and the DAB workflows pick the new stages up automatically —
   no pipeline YAML edits needed. The pipeline notebooks' `environment` dropdowns
   also read their choices from the registry automatically.

## Deploying a stage to a separate account

A stage normally deploys to the same workspace as the repo. To host a stage (e.g. `test`)
in a **different Databricks account/workspace**:

1. **Point the stage at the other workspace** — add `workspace_host` inside that stage's
   object in `configs/environments/env_settings.json`:

   ```json
   "test": {
     "catalog": "lakehouse_test",
     ...
     "workspace_host": "https://dbc-xxxxxxxx.cloud.databricks.com"
   }
   ```

2. Run `lakeforge sync-stages` — every bundle target for that stage gets a `host:` entry.

   **Never put credentials in `env_settings.json`** — it is committed to git AND synced
   into every deploy root under `/Workspace/Shared/...` (readable by all workspace users).
   Only non-secret values belong there (the host URL is fine). Secrets stay in the
   variable group: `DATABRICKS_<STAGE>_CLIENT_ID`/`_CLIENT_SECRET`/`_TOKEN`.
3. **Credentials for that account** — create a service principal (SP) in that workspace
   (needs job create/manage + write to `/Workspace/Shared`). The SP is the **identity**, not
   a token — the pipeline then authenticates as it via ONE of two credential types (entries
   in the `databricks-auth` variable group, Pipelines > Library, stage **UPPERCASE**,
   e.g. TEST for stage `test`):

   * **SP OAuth (recommended)** — on the SP's page: *Manage > OAuth secrets > Generate secret*.
     Add `DATABRICKS_TEST_CLIENT_ID` (the SP's application/UUID, plain) +
     `DATABRICKS_TEST_CLIENT_SECRET` (mark as secret). The Databricks CLI exchanges the pair
     for short-lived tokens automatically (M2M OAuth).
     **The OAuth secret is NOT a bearer token — never paste it into `DATABRICKS_TEST_TOKEN`.**
   * **SP PAT (alternative)** — generate a personal access token *for the service principal*
     and put it in `DATABRICKS_TEST_TOKEN` (mark as secret).

   Host: `DATABRICKS_TEST_HOST` is optional (the registry `workspace_host` above wins anyway).

   Precedence per stage: SP OAuth client ID+secret → per-stage PAT → global `DATABRICKS_TOKEN`.
4. Run the **dab_deploy** / **ci_cd** pipeline with that stage as usual — the "Select Stage
   Credentials" step picks the per-stage host (registry first, then the variable group entry)
   and credentials (SP OAuth pair first, then per-stage PAT, then the global
   `DATABRICKS_TOKEN`) for stages hosted in another account; same-account stages keep
   using the globals.

A separate account also means a **separate Unity Catalog metastore**: run the bundle's
`setup_environment_provisioning` task once in that workspace so its catalogs/schemas
get created there.

## First deployment to a new environment (mandatory runbook)

The **FIRST** deploy of a bundle to a stage (fresh environment, e.g. the separate-account
`test`) must provision Unity Catalog before any ETL/views job can run:

1. Run the **dab_deploy** pipeline with `run_setup` ticked — after deploying, it runs the
   bundle's `environment_setup` job (`databricks bundle run environment_setup --target <stage>`),
   which creates the stage's catalogs, schemas and `raw_files.landing` volumes —
   `client_id=all` covers the env catalog **and every client catalog**. It is idempotent.
2. Then run the client's ETL job once (its first task re-checks provisioning, then loads
   Bronze → Silver → Gold, creates views, and applies governance grants).

What is covered automatically (no extra jobs needed):

* **Catalogs/schemas/volumes** — `environment_setup` job (and the ETL job's first task).
* **Governance grants** — `governance_security_task` runs at the end of every ETL job.
* **Views** — created by the ETL/views jobs from `configs/views/`.

Not in the bundles (manual/separate): the **DQ BRICKS** job is still a UI-created job
that exists only in the dev workspace — if you need it in a new environment, give it
`client_id` + `release` tags and run the DAB export pipeline, or recreate it there.

## The Four Pipelines

| Pipeline | Trigger | Purpose |
| --- | --- | --- |
| **`ci_cd.yml`** | push to `main`, `release/*`, `dev`, `feature/*`, `bugfix/*` · PRs to `main` · manual | **Automated CI/CD via the LakeForge CLI.** Three stages: unit tests + config validation → integration tests → `python -m lakeforge.cli.deploy` (parameters: `stage`, `client_id`, `component`, `dry_run`) |
| **`pr_review.yml`** | PRs against `main` or `dev` | **Automated PR review gate.** Runs the same `lakeforge/cli/pr_reviewer.py` as GitHub (duplicate-function detection, hardcoded path/secret checks) and posts the report to the PR as a comment thread via the Azure DevOps REST API |
| **`dab_export.yml`** | manual only | **Job → bundle export.** Finds the job(s) tagged `client_id` + `release` in Databricks, generates bundle resources under `deployments/<client_id>/resources/`, validates, and commits the definition back to the branch the pipeline ran from |
| **`dab_deploy.yml`** | manual only | **Deploy a per-client bundle.** `databricks bundle validate` + `deploy --target <stage>` inside `deployments/<client_id>/` (parameters: `client_id`, `stage` with dropdown) |

## How to Configure in Azure DevOps

### 1. Create the variable group (required once)

Pipelines → Library → + Variable group → name it **`databricks-auth`**:

| Variable | Value | Secret? |
| --- | --- | --- |
| `DATABRICKS_HOST` | `https://dbc-49ef00ea-2dda.cloud.databricks.com` | no |
| `DATABRICKS_TOKEN` | Service principal token | **yes** |

Used by `dab_export.yml` and `dab_deploy.yml` (both already reference the group).

For **`ci_cd.yml`**: its DeployStage also needs `ARM_CLIENT_ID`, `ARM_CLIENT_SECRET`,
`ARM_TENANT_ID`, `ARM_SUBSCRIPTION_ID` (Terraform) — add them to the same group or a second
group, then uncomment the `- group: LakeForge-Databricks-Credentials` line in the pipeline or
link the group under Pipeline → Edit → Variables → Link variable group.

`pr_review.yml` needs nothing — it authenticates with the built-in `System.AccessToken`.

### 2. Create the four pipelines

For each file (repeat 4×):

1. Pipelines → New pipeline → **Azure Repos Git** → select this repo
2. Configure: **Existing Azure Pipelines YAML file**
3. Branch: `main`, Path: `azure-pipelines/workflows/<file>.yml` (pick ci_cd / pr_review / dab_export / dab_deploy)
4. Save (don't run yet). Give each a clear name, e.g. `LakeForge CI/CD`, `LakeForge PR Review`, `LakeForge DAB Export`, `LakeForge DAB Deploy`.

> **Permission for the export pipeline to push:** `dab_export.yml` commits back to the branch it
> ran from using `persistCredentials: true`. In Project Settings → Repositories → Security,
> make sure the **Build Service** identity has *Contribute* permission on the repo.

### 3. Make PR review a required check (recommended)

Repos → Branch Policies on `main`:
- **Build validation** → Add the `LakeForge PR Review` pipeline → Required. Now every PR must
  pass the duplicate-function / secrets gate before completing.
- Optionally require a minimum number of reviewers, prohibit self-approval.

### 4. Tag jobs for DAB export (required for `dab_export.yml`)

In Databricks → Workflows → open the job → Edit → Add tags `client_id` (e.g. `client_alpha`)
and `release` (e.g. `2026.11`). Then run the export pipeline with those exact values. On first
run for a new client it creates `deployments/<client_id>/databricks.yml` from
`deployments/_template/`; existing clients keep their own.

### 5. Optional: approval gate for staging/prod

Convert the deploy job in `dab_deploy.yml` (or `ci_cd.yml`'s DeployStage) to a `deployment:` job
and tie it to an Azure DevOps **Environment** (`Pipelines → Environments`) with an *Approvals*
check — then a human must approve every staging/prod deployment.

### 6. Run

- Automated: push to a tracked branch or open a PR — `ci_cd` / `pr_review` fire on their own.
- Manual: Pipelines → select `LakeForge DAB Export` / `DAB Deploy` → **Run pipeline** → pick
  branch + enter `client_id` (and `release` / `stage`).
