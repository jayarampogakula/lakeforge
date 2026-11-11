# CI/CD in Azure DevOps (`azure-pipelines/workflows/`)

Four Azure DevOps pipelines — exact mirrors of the GitHub workflows in `.github/workflows/`.
All of them enforce the same branch policy: **test/staging/prod deploy from `main` only;
`dev` may deploy from any branch.**

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
