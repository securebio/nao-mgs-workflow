---
name: triage-trivy
description: Triage a failing `scan-containers` Trivy CI job. For each HIGH/CRITICAL CVE, walk through a structured per-CVE assessment (vulnerability → affected functionality → pipeline reachability → fix options) leading to a Patch / Ignore / Escalate decision, plus a PR-description block reviewers audit. Structured to make `.trivyignore` the harder path.
---

# Trivy CVE triage

The `scan-containers` CI job fails on HIGH/CRITICAL vulnerabilities in any container image pinned in `configs/containers.config` that aren't already in `.trivyignore`. This skill triages those failures.

**Default agent behavior on Trivy failures has historically been "add to `.trivyignore` and move on" — that's the failure mode this skill exists to prevent.** Each step below asks for evidence; record what you find as you go, because the final PR-description block has to surface the assessment to a reviewer.

## When to use

- `scan-containers` CI job failing on a PR.
- A reviewer asks for a Trivy follow-up.
- You're updating a container yml and want to confirm no new HIGH/CRITICAL CVEs slip through.

## Inputs (required from caller)

- `pr_number`: the PR number whose `scan-containers` CI job is failing.

## Inputs (optional)

- `local_scan`: if `true`, additionally run Trivy locally against the PR's containers (useful when CI is broken or you want to scan against a `.trivyignore` you haven't pushed yet). Requires `trivy` binary + Docker.

## Branching: triage work goes in its own PR, not the failing PR

**Default: branch off `dev` and open a separate triage PR.** Most `scan-containers` failures surface global-state CVEs (libcap2, libgnutls30, Go-stdlib, etc.) that have no causal link to the failing PR's actual changes; piling security patches onto an unrelated PR conflates reviewer concerns. Use the failing PR's CI output as the *input* to the triage; deliver the fix separately.

Exception: if the failing PR itself introduced the CVE (a new yml pin in its diff), the fix belongs inline on that PR. When in doubt, ask.

## Procedure

### Step 0 — Branch

```bash
git fetch origin dev --quiet
git checkout -b coding-agent/trivy-triage-YYYY-MM-DD origin/dev
```

(If Step 1 confirms the CVE is PR-local per the Branching section above, switch to the failing PR's branch instead.)

### Step 1 — Fetch the scan results

From the latest `scan-containers` run on the PR:

```bash
# Resolve the PR's branch from the PR number, then find the latest *completed* run ID.
# (Without --status completed you may grab an in-progress run that has no artifact yet.)
BRANCH=$(gh pr view <pr_number> --json headRefName -q .headRefName)
gh run list --workflow=trivy-scan.yml --branch "$BRANCH" --status completed --limit 1 --json databaseId,headSha,conclusion
# Then download the trivy-scan-results artifact:
gh run download <RUN_ID> -n trivy-scan-results -D /tmp/trivy
```

The artifact contains one `<container>.json` per scanned image + an aggregated `summary.json`. The per-container JSON is the Trivy raw output; vulnerabilities live at `Results[].Vulnerabilities[]`.

If running locally instead (when `local_scan=true` or CI is broken):

```bash
python bin/scan_containers.py --config configs/containers.config --output-dir /tmp/trivy
```

This pulls each container, runs Trivy with the current `.trivyignore`, and writes JSON results to `/tmp/trivy/`. **Note: it scans the containers in your *current branch's* `configs/containers.config`.** After Step 0 you're branched off `dev`, so the local scan reflects the dev container set — usually fine, since most `scan-containers` failures are global-state CVEs that apply identically across branches. If the failing PR has yml or `configs/containers.config` changes in its diff, check out that branch first (or copy the relevant files in) before running the local scan.

### Step 2 — Extract the actionable CVE list

For each per-container JSON, extract HIGH + CRITICAL vulnerabilities that aren't already in `.trivyignore`. A one-liner:

```bash
jq -r '.Results[]? | .Vulnerabilities[]? | select(.Severity == "HIGH" or .Severity == "CRITICAL") |
       [.VulnerabilityID, .Severity, .PkgName, .InstalledVersion, .FixedVersion // "n/a", (.Type // ""), .Title // ""] | @tsv' \
   /tmp/trivy/<container>.json
```

`.Type` tells you which packaging ecosystem the vulnerable code lives in, which determines where a fix can come from:

- `python-pkg` → conda env site-packages: fix lands in a `containers/*.yml` pin (direct or transitive — see §3d).
- `debian` → system apt: a fix usually requires a base-image bump rather than a yml edit (Debian stable rarely backports CVE fixes into the running release; status `<no-dsa>` is the common dead end).
- `gobinary` → a statically-linked Go binary shipped by an upstream conda package: the fix has to come from that upstream rebuilding against a newer Go toolchain. The yml can only switch to a fixed upstream release if one exists (often it doesn't — Escalate).

Fall back to `PkgPath` (e.g. `opt/conda/.../site-packages/...` or `usr/bin/<name>`) if `.Type` is null.

Cross-reference each ID against `.trivyignore` and drop any that are already listed. Note `.trivyignore` carries both `CVE-*` and `GHSA-*` IDs (e.g. `GHSA-82j2-j2ch-gfr8` for Rust crates without a NVD entry), so match both:

```bash
grep -oE "CVE-[0-9]+-[0-9]+|GHSA-[a-z0-9-]+" .trivyignore | sort -u > /tmp/already-ignored.txt
```

If the scan still reports an ID that's in `.trivyignore`, the existing ignore is stale (expired or otherwise non-matching) — flag it and treat as fresh.

### Step 3 — For each CVE: gather facts before deciding

**Do this per CVE. Do not batch.** Each finding gets its own structured assessment. Run the per-CVE blocks inline, or dispatch each one to a sub-agent (good when there are many findings, to keep each context focused) — either is fine, but don't collapse multiple CVEs into a single shared assessment.

**3a. Read the CVE.** Visit `PrimaryURL` (usually NVD or the distro tracker) and read enough to understand:
- What kind of vulnerability is it? (RCE, DoS, information disclosure, privilege escalation, …)
- What component of the package is affected? (a specific function, a config option, a code path)
- What does an attacker need to trigger it? (network access, local user, malformed input, specific config)

**3b. Identify the affected package's role in our containers.** Which container(s) include it? Is it pulled in directly (in the container's conda env / apt install) or transitively (as a dep of something else)?

The Trivy JSON's `Results[].Vulnerabilities[].PkgPath` is the most useful forensic field — e.g. `opt/conda/lib/python3.13/site-packages/urllib3-...` tells you the package is in a conda env (and which env), inside a wheel, or system-level.

**3c. Assess whether the pipeline reaches the vulnerable functionality.** The load-bearing step. Don't dismiss based on "the container is isolated"; name what the pipeline actually does with this package:

- Does the pipeline invoke the affected functionality? (Read the Nextflow process scripts, `bin/` scripts, container entrypoints.)
- Is the attack vector reachable? (A network-protocol DoS doesn't apply if the binary never opens a socket; a malformed-input bug applies if we feed it arbitrary user data.)

Write the conclusion concretely — "BBDuk parses FASTQ via X; the CVE affects Y; we don't use Y because Z" or "reachable; here's how."

**3d. Search for a fix, and identify the concrete yml edit each fix-source implies.** Each source dictates a different kind of yml change in step 4a:

- **Distro update** → no yml edit; pull the next container rebuild. `apt-cache policy <pkg>` inside the container, or check the Debian/Alpine security tracker. Rarely fires for stable Debian since CVEs are usually marked `<no-dsa>` rather than backported.
- **Base-image bump** (e.g. Debian bookworm → trixie) → change the base-image config knob in `pyproject.toml` (single source of truth; see `get_base_image()` in `bin/build_ecr_container.py`). Often the right answer when "no fix in our current Debian version" is reported. **This affects every container — flag it explicitly in the PR body.**
- **Upstream conda package** → bump the existing yml pin to a fixed version, or, for the common "fix exists upstream but a feedstock pins it out of reach" pattern (urllib3 inside awscli, quinn-proto inside Polars), add an *explicit* pin for the transitive dep so the spec hash changes and the build picks up the fixed build (see step 4a). `conda search -c <channel> <pkg>` lists versions when conda is installed (check `command -v conda` first). Otherwise the anaconda.org REST API works and critically also shows what each version's deps pin:

  ```bash
  curl -s "https://api.anaconda.org/release/conda-forge/<pkg>/<version>" |
    jq '.distributions[0].attrs.depends[] | select(test("<dep_pattern>"))'
  ```
- **Upstream tool update** → bump the tool's pin in its container yml (e.g. `multiqc=1.30 → 1.31` pulls in patched deps). Check the tool's changelog first to flag any potentially breaking changes in the PR body.
- **Workaround at config level** → no yml edit; the workaround lives in the pipeline code (Nextflow process, `bin/` script). Rare; usually means Escalate so the user can decide whether the workaround is worth the complexity.

**3e. Decide.** Three legitimate outcomes:

| Outcome | When | What to do |
|---|---|---|
| **Patch** | A fix is available and applying it is safe | Update the container yml / pin version / bump base image, commit, push, and open the PR with a rebuild-handoff callout in the body (the agent role on this sandbox is ECR pull-only; the user finalizes the rebuild). Go to step 4a. |
| **Ignore** | No fix is available *and* the vulnerability is unreachable or has negligible impact in our context | Add to `.trivyignore` with detailed reasoning. Go to step 4b. |
| **Escalate** | Fix unavailable *and* the vulnerability is reachable, *or* you can't unambiguously assess reachability | Surface to the user. Don't suppress. |

Patterns that **do not** justify ignoring on their own:

- "No Debian fix available." Check whether a base-image bump or conda update exists. Only after that.
- "Not exploited in production." This isn't evidence; it's the absence of evidence. Assess the attack surface, don't rely on past silence.
- "The container is isolated." Many containers run network-facing tools or process untrusted data. Don't dismiss without naming the specific isolation.
- "Out of scope for this PR." If you're triaging Trivy, the assessment is the scope.

### Step 4 — Apply the action

**4a. Patch — yml edit, then open the PR with the rebuild-handoff callout in the body.** Edit the container yml (under `containers/`) to apply the fix:

- If the fix is in a direct dep, change the pin in the yml.
- **If the fix is in a transitive dep, add an explicit pin for the fix package itself in the yml.** This encodes the security intent *and* changes the spec hash that `bin/build_ecr_container.py` keys off (`compute_spec_hash`). Without a spec-hash change, the build script will skip the container even when the upstream conda package has shipped a fixed version — so a transitive bump that doesn't touch the yml will silently fail to rebuild.
- **Use exact pins, not ranges.**
- **Keep any inline yml comment to one line** naming the CVE IDs and the fix version. Detailed rationale belongs in the PR body, not the yml.

**Permitted edits:** add explicit pins (direct or transitive), tighten an existing range to a fixed version, bump the base-image config knob in `pyproject.toml`.

**Not permitted** (Escalate if a fix seems to need one of these): removing existing pinned deps (they're pinned for replication or compatibility reasons that aren't visible from the yml), editing the Dockerfile-generation code in `bin/build_ecr_container.py`, switching the base distro family (e.g. Debian → Alpine), or changing the conda channel list / channel-priority semantics.

Commit with the CVE ID in the message, push the branch, and open the PR as a draft per CLAUDE.md's PR conventions. **The PR body must include the rebuild-handoff callout (see Step 5) at the top**, because:

- The agent role on this sandbox is ECR pull-only and cannot publish images. The yml change does not itself clear the CVE on `scan-containers`: that CI job scans the *published* image tag pinned in `configs/containers.config`, and the new yml only takes effect once the container is rebuilt, pushed to ECR, and the tag re-pinned.
- The PR is the persistent rendezvous between agent and user. Agents that ran in subagent sessions may not be reachable later; pinning the handoff to the PR body means the user can finalize without needing the original agent back.

The PR opens with `scan-containers` red — expected, the callout explains why. The user runs the rebuild, pushes, watches CI go green, deletes the callout, and marks the PR ready for review; no agent re-invocation needed.

Do **not** add a `.trivyignore` entry for a fixable CVE to "cover the rebuild gap" — anti-pattern #1.

Ignore-outcome entries from the same triage can land on the same branch and PR — only the Patch side blocks merge until rebuild.

**4b. Add to `.trivyignore`.** Match the tightness of existing entries — typically a short header for grouped CVEs plus 4–8 comment lines total covering the four pieces below. Don't pad; if a piece is obvious from the rest, drop it.

```
# <one-line description of the vulnerability>
# <a few lines: which package, which functionality is affected, why our
#  use doesn't hit it (or why impact is bounded), what's blocking a fix,
#  what would trigger re-evaluation>
CVE-XXXX-XXXXX exp:YYYY-MM-DD
```

Pick `exp:` ~6-12 weeks out, aligned with a realistic fix arrival (upstream release cadence + buffer). No expiries beyond a year — "this will never have a fix" is an Escalate, not an Ignore.

The existing `.trivyignore` skews toward a single batch re-eval date (e.g. `2026-06-30`) used across many entries. Per-entry dates tied to a specific release cadence are more useful when the trigger is well-defined (e.g. "awscli feedstock relaxes urllib3 cap"); batch dates are reasonable when the trigger is opaque and re-eval is best done as a periodic chore. Either pattern is acceptable; pick the one that gives the next triage agent the most useful signal.

If multiple related CVEs share an assessment (e.g. several Go-stdlib CVEs in the same statically-linked binary), group them under one comment block.

### Step 5 — Generate the PR description

The PR body has two parts: a temporary rebuild-handoff callout at the top (only when there's at least one Patch outcome), and the persistent Trivy-triage assessment block. Use this as the body of the *new* triage PR (per the branching note above; the only exception is the PR-local-CVE case, where you instead append the assessment block under a `# Trivy triage` heading on the original PR):

```markdown
> **Rebuild required before merge — `scan-containers` is red until then.**
>
> The triage below patched <N> CVE(s) by editing container yml(s), but the
> agent role on this sandbox cannot push rebuilt images to ECR. To finalize:
>
>   1. Pull the branch in an environment with ECR push:
>        git fetch origin <branch>
>        git checkout <branch>
>   2. Rebuild modified containers and update the tag pins in
>      configs/containers.config:
>        bin/build_ecr_containers.py
>   3. Commit and push the updated pins:
>        git add configs/containers.config containers/
>        git commit -m "Rebuild containers for CVE-XXXX-XXXXX [+ others]"
>        git push
>   4. Once `scan-containers` is green, delete this whole "Rebuild required"
>      callout from the PR body and mark the PR ready for review.

# Trivy triage

`scan-containers` on <origin> flagged <N> HIGH/CRITICAL vulnerabilities. Each is triaged below.

## CVE-XXXX-XXXXX (<SEVERITY>, <pkg> <ver>)

<one-line vulnerability summary>. Fixed in <pkg> <fixed-ver>.

- **Action — <container(s)>:** **<Patch / Ignore / Escalate>** — <one-line reachability assessment + what we did>. <if Ignore: `.trivyignore` exp:YYYY-MM-DD, re-eval: <trigger>.>

## CVE-YYYY-YYYYY (...)
...
```

Omit the top callout entirely for Ignore-only or Escalate-only triages — it's only needed when at least one Patch outcome blocks merge on a rebuild. The callout is meant to be deleted from the PR body once the rebuild lands and CI is green; the assessment block stays as the audit trail.

**Keep it tight.** Reviewers need the outcome and why it's safe; NVD details are one click away. Don't paraphrase the vuln's internals, paste container filesystem paths, or list HTTP headers — that pads without informing.

**Mixed outcomes by container.** A single CVE sometimes splits outcomes — e.g. the urllib3 case where `multiqc` can be patched (transitive via `requests`) but the four awscli-bearing containers cannot (awscli's feedstock pins `urllib3<=2.6.3`). Split the `Action` line by container group:

```markdown
- **Action — multiqc:** **Patch** — `conda-forge::urllib3=2.7.0` pin in
  `containers/multiqc.yml` (multiqc reaches urllib3 only via `requests`).
- **Action — blast / bowtie2_samtools / kraken2 / minimap2_samtools:**
  **Ignore** — awscli pins `urllib3<=2.6.3` through every conda-forge
  build; awscli usage here is `update_blastdb.pl --source aws` and
  equivalents, no proxy or attacker-controlled compressed streams.
  `.trivyignore` exp:YYYY-MM-DD, re-eval: awscli feedstock relaxes the cap.
```

**Versioning / CHANGELOG.** A Trivy-only PR is typically a point bump under the in-flight `-dev` version (or just a CHANGELOG line if `-dev` is already cut). The CHANGELOG entry is **one short sentence**: CVE IDs, outcome, one phrase on the fix-blocker. Per-CVE rationale belongs in `.trivyignore` and the PR body, not the CHANGELOG. Defer to the `version-bump` agent (per `CLAUDE.md`) if uncertain.

### Step 6 — Verify before push

- **Local re-scan covers Ignore-side outcomes only.** `bin/scan_containers.py` and the CI `scan-containers` job both scan the *published* tag pinned in `configs/containers.config`, so neither reflects a Patch-side yml change until after the user-side rebuild. Patch-side CVEs stay red on the PR until then — by design.
- Re-run `bin/scan_containers.py` locally to confirm Ignore-side findings cleared. To wait for CI instead, push the branch and re-run the failed jobs against the latest run for the head SHA — don't push an empty commit (it fires every workflow):

  ```bash
  RUN_ID=$(gh run list --workflow=trivy-scan.yml --branch <branch> --status completed --limit 1 --json databaseId -q '.[0].databaseId')
  gh api -X POST repos/securebio/nao-mgs-workflow/actions/runs/$RUN_ID/rerun-failed-jobs
  ```
- Sanity-read the `.trivyignore` diff: every new line has a comment block with the four required pieces (vulnerability description, affected functionality + our usage, fix-blocker, expiry trigger).
- Check the PR-description block surfaces every finding, not just the ones you ignored.

## Anti-patterns this skill exists to prevent

1. **Adding a `.trivyignore` entry for a CVE that has an available fix**, to "cover the rebuild gap" or because the agent can't push images. Ignoring a fixable CVE buries a real vulnerability under stale-ignore boilerplate and conflates "unfixable" with "out of this environment's reach." Make the yml edit and hand off the rebuild per step 4a — don't ignore. (Disambiguation: a fix that exists in some upstream release but is unreachable through any current feedstock build — e.g. urllib3 2.7.0 exists, but every conda-forge `awscli` release pins `urllib3<=2.6.3` — counts as "no fix available" for the Ignore path. The distinguishing factor is whether changing the yml could actually pull a fixed build.)
2. **Bulk-adding CVEs to `.trivyignore` with one-line generic comments.** Each entry needs the four-piece assessment.
3. **"No Debian fix available" as the only stated reason.** That's a partial check, not a triage outcome. Confirm conda / base-image / upstream-tool paths are also dead ends before ignoring.
4. **Vague expiry dates** ("six months from now") rather than tied to a specific re-evaluation trigger (upstream release cadence, distro security backport window, etc.).
5. **Hiding the assessment from the PR description.** The reviewer needs to see *why* each CVE was ignored, not just the `.trivyignore` diff. A reviewer who can't audit the assessment from the PR body alone has been given the easy path to rubber-stamp.

## Cross-references

- `.trivyignore` — the file you'll be editing for ignore cases. Existing entries are the format exemplar.
- `bin/scan_containers.py` — invokable locally for fresh scans.
- `bin/build_ecr_containers.py` / `bin/build_ecr_container.py` — rebuild commands for finalizing a Patch outcome. The former iterates over `containers/`; the latter exposes `compute_spec_hash`, which decides whether a yml change forces a rebuild.
- `.github/workflows/trivy-scan.yml` — the CI job that produces the artifact.
- `containers/*.yml` — conda env files for the project's containers; updates land here for patch cases.
- `docs/developer.md` — repo conventions (commits, PR practices).
