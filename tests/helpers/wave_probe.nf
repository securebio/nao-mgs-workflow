// Minimal pipeline for tests/workflows/wave.nf.test. Nextflow only builds its Wave
// client (and logs the resolved Wave config) once a task is submitted, so this runs one
// containerless task and nothing else.

process WAVE_PROBE {
    script:
    "true"
}

workflow {
    WAVE_PROBE()
}
