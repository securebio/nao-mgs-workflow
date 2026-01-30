# Troubleshooting

## Permission issues

When attempting to run a released version of the pipeline, the most common sources of errors are AWS permission issues. Before debugging a persistent error in-depth, make sure you have all the permissions required. When running the pipeline on AWS Batch, the necessary permissions are specified in [our Batch tutorial](./batch.md#step-0-set-up-your-aws-credentials).

Once you've verified you have the required permissions, make sure Nextflow has access to your AWS credentials (if not you may get `AccessDenied` errors):

```
eval "$(aws configure export-credentials --format env)"
```

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

This occurs because Wave needs AWS ECR registry credentials to pull container images. To fix this, configure AWS ECR credentials in your Seqera account as described in the [installation guide](./installation.md#3-configure-seqera-credentials-optional).

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

To fix this, you can [obtain a user token](https://metagenomics-pipelines.readthedocs.io/en/latest/nf_tower.html) from Seqera, which will increase your API limit by 4x. To do this:

1. Create a seqera account [here](http://cloud.seqera.io/) for free.
2. Click on the user icon, then click the 'User tokens' section.
3. Now click 'Add Token' to create a token for your account. Name it whatever you want (e.g. "my-nextflow-token"). Copy the token, and store it somewhere.
4. Finally, you need to provide your user token to Nextflow. You can do this by either (a) setting the environment variable `TOWER_ACCESS_TOKEN` to your token value, or (b) setting the variable `tower.accessToken` to your token value in your Nextflow configuration file (in this case, we recommend adding this to `configs/profiles.config` to apply to all subsequent pipeline runs).

If you still keep running into this same issue, you may consider contacting Seqera for more options.

## Automatic reference file caching
- With the `standard`/`batch` profiles, the pipeline implements automatic caching of large reference files in the `/scratch/` directory 
- This generally causes no problems, but is something to be aware of:
     - The default `/scratch/` directory on AWS EC2 instances works fine in our experience, but if you are seeing `/scratch` directory permissions or space issues, you may have to customize the `/scratch/` directory with a UserData script in your EC2 launch template.
     - To turn off caching, you can always remove the `aws.batch.volumes = ['/scratch:/scratch']` line from the relevant profile.