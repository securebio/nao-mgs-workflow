# DOWNSTREAM comparison report

This report compares a candidate DOWNSTREAM run with a reference run. Each run is
a **(pipeline version, index) pair**. With no ground truth, a difference is not
good or bad on its face.

> **Author instructions: delete every blockquote in the final report.** Replace
> every placeholder with values from the comparison TSVs. Follow `SKILL.md` for
> finding coverage, naming, attribution, and mechanism checks. Examples below
> describe format only; never carry example results into the report.

## Run identity

| | Candidate (`--candidate`) | Reference (`--reference`) |
|---|---|---|
| DOWNSTREAM output | `<root from run_identity.tsv>` | `<root>` |
| Index | `<index root>` | `<index root>` |
| Pipeline version | `<exact version>` | `<exact version>` |

- **What differs between the runs:** `<which of pipeline code, index, and QC
  parameters differ or are not confirmed unchanged>`.
- **Comparison scope:** `<N>` groups (`<X>` Illumina + `<Y>` ONT), matched by
  name. `<missing files/groups or "all expected outputs present">`.
- **Report generated:** `<YYYY-MM-DD HH:MM>`

> Print exactly one attribution statement. QC parameters are not emitted by the
> tool, so call them "not confirmed unchanged" unless the user confirms otherwise.

`<Either: "More than one dimension differs or is not confirmed unchanged, so a
difference cannot be attributed to a single cause." Or: "Only <dimension>
differs; the others are confirmed unchanged, so differences are attributable to
that dimension more directly.">`

---

## Summary

> One sentence of scope, then the 2-3 broadest differences, ranked by breadth and
> magnitude. Use one representative number each. Do not list stable dimensions,
> flag totals, mechanisms, or recommendations here. If nothing crossed a
> threshold, say so in one line.

Compared `<N>` groups (`<X>` Illumina + `<Y>` ONT). The differences that stand
out:

1. `<broadest difference, with one number>`
2. `<next>`
3. `<next, if warranted>`

---

## Main findings

> Write one `###` subsection for every required finding from `SKILL.md`. Title it
> after the observed result, not the metric name. Lead with what changed, then
> give the named groups/taxa, breadth, magnitude, and the minimum caveat needed to
> interpret it. Do not paste full tables.
>
> Add `**Likely mechanism:** ... **<confidence>.**` only after the matching
> cross-check supports a cause. End each finding with either a specific
> `**To confirm:**` question or, when no action is available, a short `**Note:**`.

### `<Observed result>`

`<Finding text.>`

**Likely mechanism:** `<Supported explanation and evidence.>` **<Strongly
supported | Consistent | Speculative>.**

**To confirm:** `<Specific reviewer question.>`

`<Repeat only for dimensions triggered by this comparison.>`

---

## Checked, no action needed

> One short bullet per dimension that was computed and stayed within threshold.
> Include a bounding number so the reader can distinguish "checked" from "not
> mentioned." If a metric could not be computed, state that instead of calling it
> stable. Do not add recommendations.

- `<Dimension: result and bounding number.>`
