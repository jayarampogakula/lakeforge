# CI/CD in GitHub (`.github/workflows/`)

Four GitHub Actions workflows — two automated, two manual (DAB-based). All of them enforce the
same branch policy: **non-dev stages deploy from `main` only; `dev` may deploy from any branch.**

## Stage Registry (single source of truth)

Stage names are defined in ONE place: **`configs/environments/env_settings.json`** —
its top-level keys are the stages, its values hold each stage's catalogs/schemas.

* Workflows: the `stage` input is free text, validated **at runtime** against the registry
  (an unknown stage fails immediately with the list of available stages).
* `lakeforge deploy`: reads its supported stages from the same file.
* Databricks bundles: the `targets:` blocks of the root bundle, `deployments/_template/`,
  and every `deployments/<client_id>/` are **generated** from the registry via
  `lakeforge sync-stages` (or `python -m lakeforge.cli.sync_stages`).

**To add / rename / remove a stage** (e.g. switch to just `dev` + `qa` + `prod`):

1. Edit `configs/environments/env_settings.json` (rename/remove/add keys and their settings).
2. Run `lakeforge sync-stages` — regenerates every bundle's targets (`--check` = dry run).
3. Commit. CI, the CLI, and the DAB workflows pick the new stages up automatically —
   no workflow YAML edits needed. The push-to-`main` auto-promotion targets the first
   non-dev stage in the registry. The pipeline notebooks' `environment` dropdowns
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
3. **Credentials for that account** — create a service principal in that workspace (needs
   job create/manage + write to `/Workspace/Shared`) and add repo secrets:
   * `DATABRICKS_TEST_HOST` *(optional — the registry host above already wins)*
   * `DATABRICKS_TEST_TOKEN` *(required — SP OAuth client secret or PAT for that account)*

   Name secrets `DATABRICKS_<STAGE>_TOKEN` with the stage **UPPERCASE**.
4. Run **DAB Deploy** / **CI/CD** with that stage as usual — the "Select stage credentials"
   step picks the per-stage host (registry first, then the secret) and token, falling back
   to the global `DATABRICKS_HOST`/`DATABRICKS_TOKEN` when a stage has none (same account).

A separate account also means a **separate Unity Catalog metastore**: run the bundle's
`setup_environment_provisioning` task once in that workspace so its catalogs/schemas
get created there.

## First deployment to a new environment (mandatory runbook)

The **FIRST** deploy of a bundle to a stage (fresh environment, e.g. the separate-account
`test`) must provision Unity Catalog before any ETL/views job can run:

1. Run **DAB Deploy** with `run_setup` ticked — after deploying, it runs the bundle's
   `environment_setup` job (`databricks bundle run environment_setup --target <stage>`),
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
`client_id` + `release` tags and run the DAB Export workflow, or recreate it there.

## The Four Workflows

| Workflow | Trigger | Purpose |
| --- | --- | --- |
| **`ci_cd.yml`** | push to `main`, `dev`, `feature/**`, `bugfix/**` · PRs to `main`/`dev` · manual | **Automated CI/CD via the LakeForge CLI.** Runs unit tests, compiles the code, validates configs, then deploys to the chosen stage with `lakeforge deploy` (parameters: `stage`, `client_id`, `component`, `dry_run`) |
| **`pr_review.yml`** | PRs opened/updated against `main` or `dev` | **Automated PR review gate.** Runs `lakeforge/cli/pr_reviewer.py`: detects duplicate/new redundant functions, hardcoded personal paths & credentials, missing docstrings — and posts a markdown review comment on the PR |
| **`dab_export.yml`** | manual only | **Job → bundle export.** Finds the job(s) tagged `client_id` + `release` in Databricks, generates its bundle definition under `deployments/<client_id>/resources/` via `databricks bundle generate`, validates, and commits the definition back to the repo |
| **`dab_deploy.yml`** | manual only | **Deploy a per-client bundle.** Runs `databricks bundle validate` + `deploy --target <stage>` inside `deployments/<client_id>/` (parameters: `client_id`, `stage`) |

**Two deployment tracks:** `ci_cd.yml` (lakeforge CLI, full framework) and the DAB pair
(`dab_export.yml` → `dab_deploy.yml`, per-client bundles under `deployments/`). Use whichever
fits the change — they don't conflict.

## How to Configure in GitHub

### 1. Add repository secrets (required once)

Settings → Secrets and variables → Actions → New repository secret:

| Secret | Value |
| --- | --- |
| `DATABRICKS_HOST` | `https://dbc-49ef00ea-2dda.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Service principal token (workspace admin → User management → Service principals → generate token) |

Used by `ci_cd.yml` (deploy stage) and both `dab_*` workflows. `pr_review.yml` needs nothing —
it uses the built-in `GITHUB_TOKEN`.

### 2. Grant the SP access (one-time, outside GitHub)

The service principal behind `DATABRICKS_TOKEN` needs workspace-level access:
- **`dab_export.yml`** — `CAN_READ` on the jobs being exported (jobs list + read settings).
- **`dab_deploy.yml`** / `ci_cd.yml` deploy — enough to create/update jobs and write to the
  target `root_path` (`/Workspace/Shared/lakeforge*` for test/staging/prod).

### 3. Tag jobs for DAB export (required for `dab_export.yml`)

In Databricks → Workflows → open the job → Edit → Add tag:

| Tag | Example |
| --- | --- |
| `client_id` | `client_alpha` |
| `release` | `2026.11` |

Then run `dab_export.yml` with those exact values. The export creates
`deployments/<client_id>/databricks.yml` from `deployments/_template/` on first run (existing
clients keep their own).

### 4. Protect `main` (recommended)

Settings → Branches → Add branch protection rule for `main`:
- Require a pull request before merging (this makes `pr_review.yml` fire on every change)
- Block force pushes and deletions

### 5. Run

- Automated: push to any tracked branch, or open a PR — they fire on their own.
- Manual (DAB): Actions tab → **DAB Export (Job to Bundle)** or **DAB Deploy** → Run workflow →
  pick branch + enter `client_id` (and `release` / `stage`).

### One-time approval

On first run, GitHub may ask for approval on the Actions tab (or workflows may be disabled by
default on some org settings) — enable each from the Actions tab once.
