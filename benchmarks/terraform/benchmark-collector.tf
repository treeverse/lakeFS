resource "aws_ecs_task_definition" "collector" {
  family                = "benchmark-collector-${var.tag}"
  requires_compatibilities = ["FARGATE"]
  cpu = "512"
  memory = "2048"
  network_mode = "awsvpc"
  execution_role_arn = data.aws_arn.BENCHMARK_VM.arn
  task_role_arn = data.aws_arn.BENCHMARK_VM.arn

  container_definitions = <<TASK_DEFINITION
[
    {
        "name": "benchmark-collector",
        "image": "${var.dockerReg}/benchmark-collector:latest",
        "environment": [
            {"name": "GRAFANA_CONFIG", "value": "${base64encode(local.collector_config.grafana)}"}
        ],
        "essential": true,
        "cpu": 512,
        "memory": 2048,
        "logConfiguration": {
          "logDriver": "awslogs",
          "options": {
            "awslogs-group": "/ecs/benchmark/${var.tag}",
            "awslogs-region": "us-east-1",
            "awslogs-stream-prefix": "collector"
          }
        }
    }
]
TASK_DEFINITION
}

resource "aws_ecs_service" "collector" {
  name            = "collector-${var.tag}"
  cluster         = aws_ecs_cluster.benchmark.id
  task_definition = aws_ecs_task_definition.collector.id
  desired_count   = 1
  launch_type = "FARGATE"

  network_configuration {
    subnets = [for s in data.aws_subnet.all : s.id]
    assign_public_ip = false
    security_groups = [ aws_security_group.benchmark_sg.id ]
  }
}

locals {
  collector_config = {
    grafana = <<CONFIG_DEF
{
  "integrations": {
    "agent": {
      "enabled": false
    },
    "node_exporter": {
      "enabled": false
    }
  },
  "prometheus": {
    "configs": [
      {
        "host_filter": false,
        "name": "test",
        "remote_write": [
          {
            "basic_auth": {
              "password": "${var.grafana-password}",
              "username": "${var.grafana-username}"
            },
            "url": "https://prometheus-us-central1.grafana.net/api/prom/push"
          }
        ],
        "scrape_configs": [
          {
            "job_name": "lakeFS_scrape",
            "static_configs": [
              {
                "labels": {
                  "build": "${var.build}",
                  "tag": "${var.tag}"
                },
                "targets": [
                  "${aws_alb.main.dns_name}:8000"
                ]
              }
            ]
          }
        ]
      }
    ],
    "global": {
      "scrape_interval": "5s"
    },
    "wal_directory": "/tmp/agent"
  },
  "server": {
    "http_listen_port": 8001,
    "log_level": "info"
  }
}
CONFIG_DEF
  }
}
