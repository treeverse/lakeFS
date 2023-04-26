FROM rclone/rclone:1.57 AS rclone

FROM python:3.9-slim-buster

WORKDIR /lakefs

COPY lakefs_export.py rclone.conf.template ./
COPY --from=rclone /usr/local/bin/rclone /usr/local/bin/

ENTRYPOINT ["python", "lakefs_export.py"]
