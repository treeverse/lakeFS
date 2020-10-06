# Collector
The collector container scrapes the Prometheus metrics from the lakeFS container,
which is being benchmarked.
Image is derived from the grafana/agent image, but with passing the config as base64
encoded env var.
It isn't currently built as part of the CI, as often changes aren't expected to the image.

## Manual push image
If however you do need to change the image or the running extracting script, you should:
1. Build:
   `docker build -t 977611293394.dkr.ecr.us-east-1.amazonaws.com/benchmark-collector:latest .`

2. Push:
    `docker push 977611293394.dkr.ecr.us-east-1.amazonaws.com/benchmark-collector:latest`
    
 