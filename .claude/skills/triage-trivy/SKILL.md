---
name: triage-trivy
description: Triage a failing `scan-containers` Trivy CI job. For each HIGH/CRITICAL CVE, walk through a structured per-CVE assessment (vulnerability → affected functionality → pipeline reachability → fix options) leading to a Patch / Ignore / Escalate decision, plus a PR-description block reviewers audit. Structured to make `.trivyignore` the harder path.
---

# Trivy CVE triage

The `Trivy Container Vulnerability Scan` CI job fails when any container image referenced in `configs/containers.config` has HIGH or CRITICAL severity vulnerabilities that aren't already in `.trivyignore`. This skill triages those failures.

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

**Default: branch off `dev` and open a new PR for the triage**, separate from whatever PR's CI surfaced the failure. The common case is global-state CVEs (libcap2, libgnutls30, Go-stdlib, etc.) that affect every container scan regardless of which PR is in flight — those have no causal relationship to the perf change / feature work on the failing PR, and piling security patches onto an unrelated PR conflates two reviewer concerns. Use the failing PR's CI output as the *input* to the triage; deliver the fix as a separate change.

There's one exception: if the failing PR itself introduced the CVE (e.g. a new container yml in the PR added a vulnerable dep), the fix belongs inline on that PR. Decide by looking at the diff — if the failing CVE traces to a package whose version pin is in the PR's diff, it's PR-local; otherwise it's a separate-PR triage.

When in doubt, ask the user before stacking.

## Procedure

### Step 0 — Branch

```bash
git fetch origin dev --quiet
git checkout -b coding-agent/trivy-triage-YYYY-MM-DD origin/dev
```

(Or, if Step 1's CVE-source analysis later confirms the CVE is PR-local, switch to the failing PR's branch instead.)

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

This pulls each container, runs Trivy with the current `.trivyignore`, and writes JSON results to `/tmp/trivy/`.

### Step 2 — Extract the actionable CVE list

For each per-container JSON, extract HIGH + CRITICAL vulnerabilities that aren't already in `.trivyignore`. A one-liner:

```bash
jq -r '.Results[]? | .Vulnerabilities[]? | select(.Severity == "HIGH" or .Severity == "CRITICAL") |
       [.VulnerabilityID, .Severity, .PkgName, .InstalledVersion, .FixedVersion // "n/a", (.Type // ""), .Title // ""] | @tsv' \
   /tmp/trivy/<container>.json
```

`.Type` (`gobinary`, `python-pkg`, `debian`, `conda-pkg`, …) makes the direct-vs-transitive call in step 3b faster — a `python-pkg` finding lives in a conda env's site-packages, a `debian` finding is system-level apt, a `gobinary` finding is statically linked into a precompiled tool.

Cross-reference each ID against `.trivyignore` and drop any that are already listed. Note `.trivyignore` carries both `CVE-*` and `GHSA-*` IDs (e.g. `GHSA-82j2-j2ch-gfr8` for Rust crates without a NVD entry), so match both:

```bash
grep -oE "CVE-[0-9]+-[0-9]+|GHSA-[a-z0-9-]+" .trivyignore | sort -u > /tmp/already-ignored.txt
```

If the scan still reports an ID that's in `.trivyignore`, the existing ignore is stale (expired or otherwise non-matching) — flag it and treat as fresh.

### Step 3 — For each CVE: gather facts before deciding

**Do this per CVE. Do not batch.** Each finding gets its own structured assessment.

**3a. Read the CVE.** Visit `PrimaryURL` (usually NVD or the distro tracker) and read enough to understand:
- What kind of vulnerability is it? (RCE, DoS, information disclosure, privilege escalation, …)
- What component of the package is affected? (a specific function, a config option, a code path)
- What does an attacker need to trigger it? (network access, local user, malformed input, specific config)

**3b. Identify the affected package's role in our containers.** Which container(s) include it? Is it pulled in directly (in the container's conda env / apt install) or transitively (as a dep of something else)?

The Trivy JSON's `Results[].Vulnerabilities[].PkgPath` is the most useful forensic field here — e.g. `opt/conda/lib/python3.13/site-packages/urllib3-...` tells you immediately whether the package is in a conda env (and which one), inside a wheel (and which dep installed it), or system-level. Use that plus `bin/scan_containers.py`'s output to confirm.

**3c. Assess whether the pipeline uses the vulnerable functionality.** This is the key step. *Don't write off the risk based on "the container is isolated" — that's the easy way out.* Instead, name what the pipeline actually does with this package:

- Does the pipeline invoke the affected functionality? (Read the relevant Nextflow process script, the bin/ scripts, the container's entrypoints.)
- Is the attack vector reachable in our deployment? (E.g. a network-protocol DoS doesn't apply if the binary never opens a server socket; a malformed-input parser bug applies if we feed it arbitrary user data.)
- Does the affected code path get hot data? (A vuln in seldom-touched code is less concerning than one in a hot path, but neither is dismissible without evidence.)

Write this down concretely — "BBDuk in the BBTools container parses FASTQ via X; the CVE affects Y; we don't use Y because Z" or "this vulnerability is reachable; here's how."

**3d. Search for a fix.** Several places to check:

- **Distro update:** Is the package newer in the container's base distro? `apt-cache policy <pkg>` inside the container, or check the Debian/Alpine security tracker.
- **Base-image bump:** Is there a newer base image (e.g. Debian bookworm → trixie) where the package is patched? Often this is the right answer when "no fix in our current Debian version" is reported.
- **Upstream conda package:** If the package comes from conda-forge / bioconda, `conda search -c <channel> <pkg>` shows available versions. Check `command -v conda` first — on a fresh sandbox conda may not be installed, in which case the anaconda.org REST API is the agent-friendly fallback for checking version availability *and* — critically — what each version's deps pin. The "fix exists upstream but a feedstock pins it out of reach" pattern is common (urllib3 inside awscli, quinn-proto inside Polars, etc.). Recipe to check pinned deps for a specific version:

  ```bash
  curl -s "https://api.anaconda.org/release/conda-forge/<pkg>/<version>" |
    jq '.distributions[0].attrs.depends[] | select(test("<dep_pattern>"))'
  ```

  If that returns a pin like `urllib3<=2.6.3`, the fix is blocked upstream until the feedstock relaxes the cap.
- **Upstream tool update:** Some CVEs trace to a tool (e.g. multiqc) shipping a vulnerable dep. Check the tool's changelog for a release that bumps the dep.
- **Workaround at config level:** Occasionally a CVE only affects a specific config option you can disable.

If you find an available fix, this is now an update PR, not an ignore PR — go to step 4a.

**3e. Decide.** Three legitimate outcomes:

| Outcome | When | What to do |
|---|---|---|
| **Patch** | A fix is available and applying it is safe | Update the container yml / pin version / bump base image, then hand off to the user for the image rebuild (the agent role on this sandbox is ECR pull-only). Go to step 4a. |
| **Ignore** | No fix is available *and* the vulnerability is unreachable or has negligible impact in our context | Add to `.trivyignore` with detailed reasoning. Go to step 4b. |
| **Escalate** | Fix unavailable *and* the vulnerability is reachable, *or* you can't unambiguously assess reachability | Surface to the user. Don't suppress. |

Patterns that **do not** justify ignoring on their own:

- "No Debian fix available." Check whether a base-image bump or conda update exists. Only after that.
- "Not exploited in production." This isn't evidence; it's the absence of evidence. Assess the attack surface, don't rely on past silence.
- "The container is isolated." Many containers run network-facing tools or process untrusted data. Don't dismiss without naming the specific isolation.
- "Out of scope for this PR." If you're triaging Trivy, the assessment is the scope.

### Step 4 — Apply the action

**4a. Patch — yml edit, then hand off for rebuild.** Edit the container yml (under `containers/`) to bump the dep, base image, or upstream tool. Commit the change with the CVE ID in the message. **Stop there for the rebuild step — do not open the PR yet, and do not add a `.trivyignore` entry to cover the rebuild gap.**

A yml change does not itself clear the CVE on `scan-containers`: that CI job scans the *published* image tag pinned in `configs/containers.config`, and the new yml only takes effect once the container is rebuilt, pushed to ECR, and the tag re-pinned. The agent role on this sandbox is ECR pull-only and cannot publish images. Ignoring a CVE for which a fix exists is unsafe and misleading — it papers over a real vulnerability with stale-ignore boilerplate and leaves a reviewer with no way to distinguish "this is unfixable" from "we just couldn't finish the fix from this environment." So: don't.

Instead, surface a handoff to the user before opening the PR. Tell them what you changed, where the branch is, and the exact commands they need to run from an environment with ECR push permissions. Template:

```
I've patched the following CVE(s) on coding-agent/trivy-triage-YYYY-MM-DD by
editing the corresponding container yml(s):

  - CVE-XXXX-XXXXX: containers/<X>.yml — <one-line description of the change>
  - CVE-YYYY-YYYYY: containers/<Y>.yml — <one-line description of the change>

The images need to be rebuilt and re-pinned before scan-containers can clear,
which requires AWS ECR push permissions that this sandbox doesn't have. To
finish:

  1. Pull the branch in an environment with ECR push:
       git fetch origin coding-agent/trivy-triage-YYYY-MM-DD
       git checkout coding-agent/trivy-triage-YYYY-MM-DD
  2. Rebuild modified containers and update the tag pins in
     configs/containers.config:
       bin/build_ecr_containers.py
  3. Commit and push the updated pins (+ any other artifacts the script produced):
       git add configs/containers.config containers/
       git commit -m "Rebuild containers for CVE-XXXX-XXXXX [+ others]"
       git push
  4. Ping me — I'll verify the next scan-containers run is clean and open the PR.

If you'd prefer a different rebuild path (e.g. batch with other pending bumps),
let me know and I'll hold the branch.
```

If the triage also produced Ignore-disposition entries, those can land on the same branch alongside the yml edit — only the Patch side needs the handoff. Open the PR only after the rebuild has happened and CI is green.

**4b. Add to `.trivyignore`.** Format follows the existing entries:

```
# <one-line description of the vulnerability>
# <one paragraph: which package, which functionality is affected,
#  why our use of the package doesn't hit it (or why the impact is bounded),
#  what's blocking a fix, and what would trigger re-evaluation>
CVE-XXXX-XXXXX exp:YYYY-MM-DD
```

Choose `exp:` (expiry) ~6-12 weeks out — the entry will need re-evaluation once that hits. Pick a date that aligns with when you reasonably expect a fix (upstream release cadence + buffer). Don't set expiries more than a year out; if the assessment is "this will never have a fix," that's an escalation, not an ignore.

The existing `.trivyignore` skews toward a single batch re-eval date (e.g. `2026-06-30`) used across many entries. Per-entry dates tied to a specific release cadence are more useful when the trigger is well-defined (e.g. "awscli feedstock relaxes urllib3 cap"); batch dates are reasonable when the trigger is opaque and re-eval is best done as a periodic chore. Either pattern is acceptable; pick the one that gives the next triage agent the most useful signal.

If multiple related CVEs share an assessment (e.g. several Go-stdlib CVEs in the same statically-linked binary), group them under one comment block.

### Step 5 — Generate the PR description

Use the following block as the body of the *new* triage PR (per the branching note above; the only exception is the PR-local-CVE case, where you instead append the block under a `# Trivy triage` heading on the original PR). This is what the reviewer audits:

```markdown
# Trivy triage

The `scan-containers` CI job flagged <N> HIGH/CRITICAL vulnerabilities on this PR.
Each is triaged below.

## CVE-XXXX-XXXXX (<SEVERITY>, <pkg> <ver>)

- **Vulnerability:** <one-line description from NVD>
- **Affected functionality:** <what part of the package; from CVE details>
- **Our usage:** <how the pipeline uses this package, with citations to specific
  module/script files; whether the affected functionality is reached>
- **Mitigation status:** <fix available where, blocked by what, or "no upstream fix">
- **Action:** **<Patch / Ignore / Escalate>** — <one-line reason>
  <if Ignore: name the .trivyignore entry expiry date and the trigger for re-eval>

## CVE-YYYY-YYYYY (...)
...
```

Surface every finding here, including ones you patched. The reviewer should see the assessment, not just the diff.

**Container rebuilds** (add this section if any Patch outcome required a user-mediated rebuild per step 4a):

```markdown
## Container rebuilds

The Patch outcomes above required a container rebuild + tag re-pin, performed
by <user> from an ECR-push environment:

- `<container>.yml` rebuilt to address CVE-XXXX-XXXXX; new tag pinned in
  `configs/containers.config` at commit <sha>.
```

This documents the handoff so the audit trail is on the PR itself. The PR should not be opened until the rebuild lands on the branch and `scan-containers` is green — never open a Trivy-triage PR with a red `scan-containers` and a note explaining away the failure.

**Versioning / CHANGELOG.** Per `docs/versioning.md`, a Trivy-only PR is typically a point bump under the in-flight `-dev` version with a single CHANGELOG line summarizing the disposition (patched vs ignored, which packages, which re-eval trigger). The `version-bump` agent referenced in `CLAUDE.md` is the canonical authority — defer to it if uncertain.

### Step 6 — Verify before push

- Re-run `bin/scan_containers.py` locally and confirm the failing-counter is 0 HIGH/CRITICAL. If you'd rather wait for CI, push the branch and re-run the failed jobs against the latest run for the head SHA — don't push an empty commit, which fires every workflow:

  ```bash
  RUN_ID=$(gh run list --workflow=trivy-scan.yml --branch <branch> --status completed --limit 1 --json databaseId -q '.[0].databaseId')
  gh api -X POST repos/securebio/nao-mgs-workflow/actions/runs/$RUN_ID/rerun-failed-jobs
  ```
- Sanity-read the `.trivyignore` diff: every new line has a comment block with the four required pieces (vulnerability description, affected functionality + our usage, fix-blocker, expiry trigger).
- Check the PR-description block surfaces every finding, not just the ones you ignored.

## Anti-patterns this skill exists to prevent

1. **Adding a `.trivyignore` entry for a CVE that has an available fix**, to "cover the rebuild gap" or because the agent can't push images. Ignoring a fixable CVE buries a real vulnerability under stale-ignore boilerplate and conflates "unfixable" with "out of this environment's reach." Make the yml edit and hand off the rebuild per step 4a — don't ignore.
2. **Bulk-adding CVEs to `.trivyignore` with one-line generic comments.** Each entry needs the four-piece assessment.
3. **"No Debian fix available" as the only stated reason.** That's a partial check, not a triage outcome. Confirm conda / base-image / upstream-tool paths are also dead ends before ignoring.
4. **Vague expiry dates** ("six months from now") rather than tied to a specific re-evaluation trigger (upstream release cadence, distro security backport window, etc.).
5. **Hiding the assessment from the PR description.** The reviewer needs to see *why* each CVE was ignored, not just the `.trivyignore` diff. A reviewer who can't audit the assessment from the PR body alone has been given the easy path to rubber-stamp.

## Cross-references

- `.trivyignore` — the file you'll be editing for ignore cases. Existing entries are the format exemplar.
- `bin/scan_containers.py` — invokable locally for fresh scans.
- `.github/workflows/trivy-scan.yml` — the CI job that produces the artifact.
- `containers/*.yml` — conda env files for the project's containers; updates land here for patch cases.
- `docs/developer.md` — repo conventions (commits, PR practices).
