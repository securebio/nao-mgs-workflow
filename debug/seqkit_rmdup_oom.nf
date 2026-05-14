// Minimal harness for debugging the CONCATENATE_GENOME_FASTA OOM regression
// introduced in PR #758 (seqkit rmdup --by-name on the concatenated viral
// genomes FASTA — currently labelled `single` = 4 GB).
//
// Sweeps `seqkit rmdup --by-name` over a list of memory tiers in parallel
// against a pre-built virus-genomes FASTA (assumed to have the same unique
// sequence-name count as the failing CONCATENATE input). Each tier writes a
// log file capturing peak RSS (polled every 5 s from /proc/<pid>/status) and
// the seqkit exit code. On OOM-kill the cgroup terminates the entire shell,
// so the tail of the log is "started=..." plus the last `peak_kb_so_far`
// observed before kill — that's still enough to distinguish OOM from success.
//
// Usage:
//   nextflow run debug/seqkit_rmdup_oom.nf \
//       -c debug/seqkit_rmdup_oom.config \
//       -profile batch \
//       --base_dir s3://nao-restricted/jo/debug/seqkit-rmdup-oom/$(date +%Y%m%d-%H%M) \
//       --queue <batch-queue-name>
//
// To override the input FASTA or memory tiers:
//   --input_fasta s3://.../some.fasta.gz
//   --memory_tiers_gb 8,16,32

nextflow.enable.dsl=2

process SEQKIT_RMDUP_TEST {
    label "seqkit"
    tag "${mem_gb}GB"
    cpus 1
    memory { "${mem_gb} GB" }
    errorStrategy "ignore"
    publishDir "${params.base_dir}/results", mode: "copy"
    input:
        tuple val(mem_gb), path(fasta)
    output:
        path("rmdup-${mem_gb}GB.log"), optional: true
    script:
        """
        set -uo pipefail
        LOG=rmdup-${mem_gb}GB.log
        echo "tier=${mem_gb}GB input=${fasta}" > \$LOG
        echo "started=\$(date -u +%FT%TZ)" >> \$LOG

        # Run seqkit in background; poll /proc for peak RSS.
        seqkit rmdup --by-name --threads 1 \\
            -D duplicates-${mem_gb}GB.tsv \\
            -o /dev/null ${fasta} >> \$LOG 2>&1 &
        PID=\$!

        PEAK_KB=0
        while kill -0 \$PID 2>/dev/null; do
            if [[ -r /proc/\$PID/status ]]; then
                CUR=\$(awk '/^VmHWM/ {print \$2}' /proc/\$PID/status 2>/dev/null || echo 0)
                if [[ -n "\$CUR" && "\$CUR" -gt "\$PEAK_KB" ]]; then
                    PEAK_KB=\$CUR
                    # Persist on every increase so we have data even on OOM-kill.
                    echo "peak_kb_so_far=\$PEAK_KB" >> \$LOG
                fi
            fi
            sleep 5
        done

        wait \$PID
        EC=\$?
        echo "exit_code=\$EC" >> \$LOG
        echo "peak_rss_kb=\$PEAK_KB peak_rss_gb=\$(awk -v k=\$PEAK_KB 'BEGIN{printf \"%.2f\", k/1048576}')" >> \$LOG
        echo "finished=\$(date -u +%FT%TZ)" >> \$LOG
        # Always exit 0 so the channel forwards the log file even on seqkit failure.
        exit 0
        """
}

workflow {
    if (!params.base_dir || params.base_dir == "BASE_DIR_PATH") {
        error "Must supply --base_dir (S3 path)"
    }
    // Allow comma-separated CLI override: --memory_tiers_gb 4,16,32
    def tiers = params.memory_tiers_gb instanceof List
        ? params.memory_tiers_gb
        : params.memory_tiers_gb.toString().split(",").collect { it.trim() as Integer }

    fasta = Channel.fromPath(params.input_fasta)
    Channel.fromList(tiers)
        .combine(fasta)
        .set { tier_inputs }
    SEQKIT_RMDUP_TEST(tier_inputs)
}
