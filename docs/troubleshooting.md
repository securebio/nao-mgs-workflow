# Troubleshooting

## Permission issues

When attempting to run a released version of the pipeline, the most common sources of errors are AWS permission issues. Before debugging a persistent error in-depth, make sure you have all the permissions required. When running the pipeline on AWS Batch, the necessary permissions are specified in [our Batch tutorial](./batch.md#step-0-set-up-your-aws-credentials).

## Docker image failures

Another common issue is for processes to fail with some variation of the following Docker-related error:

```
docker: failed to register layer: write /usr/lib/jvm/java-11-openjdk-amd64/lib/modules: **no space left on device**.
```

This is a fairly well-known problem, which can arise even when there is substantial free storage space accessible to your EC2 instance. Following the steps recommended [here](https://www.baeldung.com/linux/docker-fix-no-space-error) or [here](https://forums.docker.com/t/docker-no-space-left-on-device/69205) typically resolves the issue, either by deleting Docker assets to free up space (e.g. via `docker system prune --all --force`) or by giving Docker more space.

## Wave container access errors

When running the pipeline via Seqera Platform, you may encounter errors like:

```
error [io.seqera.wave.plugin.exception.BadResponseException]: Wave invalid response: POST https://wave.seqera.io/v1alpha2/container [400] {"message":"Container image 'public.ecr.aws/q0n1c7g8/nao-mgs-workflow/python:cb756b23e8c4f9cd' does not exist or access is not authorized"}
```

Wave resolves the source image's digest on every container request, and reports *any*
failure of that lookup with this message — a genuinely missing tag, a credentials
problem, or an upstream registry that throttled Wave. Configure AWS ECR credentials in
your Seqera account as described in the [installation guide](./installation.md#4-configure-seqera-ecr-credentials);
besides granting access, this raises the rate limit on Wave's own lookups from 1 to 10
per second.

If the tag does exist and is publicly pullable, the error was transient. Nextflow does
not retry a Wave `400`, so a single throttled lookup aborts the run. See
[Wave containers and pull rate limits](./wave.md) for the full analysis.

## Resource constraint errors

Jobs may sometimes fail due to insufficient memory or CPU availability, especially on very large datasets or small instances. To fix this, you can:
- **Increase resource allocations in `configs/resources.config`.** This will alter the resources available to all processes with a given tag (e.g. "small").
- **Increase resource allocation to a specific process.** You can do this by editing the process in the relevant Nextflow file, most likely found at `modules/local/MODULE_NAME/main.nf`.
Note that in some cases it may not be possible to allocate enough resources to meet the needs of a given process, especially on a resource-constrained machine. In this case, you will need to use a smaller reference file (e.g. a smaller Kraken reference DB) or obtain a larger machine.

## API container errors

Jobs may sometimes fail due to using up [too many API requests to get the containers](https://docs.seqera.io/wave/api). This will look like the following:

```
Task failed to start - CannotPullImageManifestError: Error response from daemon: toomanyrequests: Request exceeded pull rate limit for IP XX.XXX.XX.XX
```

To fix this, create a Seqera account and configure your access token as described in the [installation guide](./installation.md#3-create-seqera-account). Authenticating raises the limit from 100 pulls/hour to 2,000 pulls/minute.

The same error appears as `... for user <your-seqera-email>` once you are authenticated,
because the quota is per Seqera user and is shared across all your concurrent runs. See
[Wave containers and pull rate limits](./wave.md) for why we consume so much of it and
what to change.

## Automatic reference file caching
- With the `standard`/`batch` profiles, the pipeline implements automatic caching of large reference files in the `/scratch/` directory 
- This generally causes no problems, but is something to be aware of:
     - The default `/scratch/` directory on AWS EC2 instances works fine in our experience, but if you are seeing `/scratch` directory permissions or space issues, you may have to customize the `/scratch/` directory with a UserData script in your EC2 launch template.
     - To turn off caching, you can always remove the `aws.batch.volumes = ['/scratch:/scratch']` line from the relevant profile.

## Scratch directories
- For each Fusion-enabled profile defined in `configs/profiles.config`, processes with the `use_scratch` label create a local [scratch](https://docs.seqera.io/nextflow/reference/process#scratch) directory for file operations and then stage out to Fusion at the end of the process
- SecureBio's standard Batch launch templates are sized for this. If you run on a custom launch template with a small root volume and hit scratch space issues, you can remove the `process { withLabel: 'use_scratch' { scratch = true } }` selector from the relevant profile in `configs/profiles.config`.
- If a process with the `use_scratch` label fails during stage out, Nextflow is likely trying to stage out too many files at once. Remove the scratch selector from the relevant profile in `configs/profiles.config` or reduce the number of staged out files per-process, for example by increasing parallelization at that step if that is an exposed parameter.
- In both cases, at production scale, this will likely dramatically slow down file operations for processes with the `use_scratch` tag.
