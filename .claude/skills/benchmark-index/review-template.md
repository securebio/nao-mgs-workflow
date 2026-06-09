# `mgs-workflow` index benchmark report

- **Target index:** `s3://path/to/new/index`
- **Reference index:** `s3://path/to/old/index`
- **Report timestamp:** YYYY-MM-DD HH:MM

---

## Summary

- Concise bullet list of top-level findings (one short sentence each)
- Don't link out to other file or directory paths; the report should stand alone
- **Recommendations:** bullet list mirroring §Recommendations — one bullet per candidate change with its confidence level

---

## Findings

### 1. Staleness

| Reference | Version in target index | Latest available | Status |
|---|---|---|---|
| `db_name` | `current_version` | `latest_version` | current / **stale** |

**Findings:**
- Concise bullet list of findings from section

### 2. Database size

| DB | Size in reference index | Size in target index | Δ |
| `db_name` | old size | new size | absolute change (relative change in %) |

**Findings:**
- Concise bullet list of findings from section

### 3. Virus genomes

#### 3.1. Total

- Genome IDs lost: NNN
    - Absent from NCBI (suppressed or removed): NNN (link to table in appendix)
    - Non-current assembly version: NNN (link to table in appendix)
    - Hard-excluded: NNN (link to table in appendix)
    - Reassigned to excluded taxon: NNN (link to table in appendix)
    - Infection-status demotion: NNN (link to table in appendix)
    - Other: NNN
- Genome IDs gained: NNN
    - Newly deposited since old build: NNN (link to table in appendix)
    - Pre-existing, pulled in by source-policy change: NNN (link to table in appendix)
    - Hard-included: NNN (link to table in appendix)
    - New taxon in NCBI taxonomy: NNN (link to table in appendix)
    - Infection-status promotion: NNN (link to table in appendix)
    - Other: NNN

#### 3.2. Losses

- Bullet list summary of relevant losses, divided by category (absent from NCBI, non-current assembly, hard-excluded, reassigned to excluded taxon, infection-status demotion)
- Discuss species taxids that (a) are not hard-excluded but (b) drop to zero genomes 
- Link to tables in appendix where appropriate

#### 3.3. Gains

- Bullet list summary of relevant genome gains, divided by category (newly deposited, pulled in by source-policy change, hard-included, new taxon, infection-status promotion)
- Discuss species taxids that (a) are not hard-included but (b) go from zero to nonzero genomes
- Link to tables in appendix where appropriate

### 4. Infection status

Gains or losses of viral species assigned to each host category, ignoring hard inclusions and exclusions:

| Host | Promotions | Demotions |
| `human` | NNN | NNN |
| `primate` | NNN | NNN |
| `mammal` | NNN | NNN |
| `bird` | NNN | NNN |
| `vertebrate` | NNN | NNN |

**Findings:**
- Concise bullet list of findings from section
- Link out to tables in appendix where appropriate

### 5. Other notable changes

- Concise bullet list of other changes to the index of relevance to the benchmark
- Link out to tables in appendix where appropriate

---

## Recommendations

1. **Single change to the index (config edit, override, etc.) to apply before shipping** (high | medium | low confidence)
    - Concise bullet summary of arguments for recommendation
2. **Single change to the index (config edit, override, etc.) to apply before shipping** (high | medium | low confidence)
    - Concise bullet summary of arguments for recommendation
---

## Appendix

### A.1. Table subject

| Table header | Table header |
| table body | Table body |

### A.2. Table subject

| Table header | Table header |
| table body | Table body |

