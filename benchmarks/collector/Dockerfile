FROM grafana/agent:v0.6.1

COPY run.sh /

CMD ["--config.file=/etc/agent/agent.yaml", "--prometheus.wal-directory=/etc/agent/data"]
ENTRYPOINT ["/run.sh"]
