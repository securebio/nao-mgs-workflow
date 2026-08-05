// Minimal pipeline for tests/workflows/wave.nf.test.

process WAVE_PROBE {
    script:
    "true"
}

workflow {
    WAVE_PROBE()
}
