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
    - Hard-excluded: NNN (link to table in appendix)
    - Change in infection status: NNN (link to table in appendix)
    - Change in assigned taxid: NNN (link to table in appendix)
    - Non-current genome version: NNN (link to table in appendix)
    - [Other specific reason]: NNN
    - Other: NNN
- Genome IDs gained: NNN
    - Hard-included: NNN (link to table in appendix)
    - Newly deposited for existing included taxa: NNN (link to table in appendix)
    - Change in infection status: NNN (link to table in appendix)
    - Change in assigned taxid: NNN (link to table in appendix)
    - New species in NCBI taxonomy: NNN (link to table in appendix)
    - [Other specific reason]: NNN
    - Other: NNN

#### 3.2. Losses

- Bullet list summary of relevant losses, divided by category (hard-excluded, change in infection status, etc)
- Discuss species taxids that (a) are not hard-excluded but (b) drop to zero genomes 
- Link to tables in appendix where appropriate

#### 3.3. Gains

- Bullet list summary of relevant genome gains, divided by category (hard-included, newly-deposited, etc)
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

