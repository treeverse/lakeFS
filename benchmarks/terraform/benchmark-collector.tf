resource "aws_ecs_task_definition" "executor" {
  family                = "benchmark-collector-${var.tag}"
  requires_compatibilities = ["FARGATE"]
  cpu = "1024"
  memory = "2048"
  network_mode = "awsvpc"
//  execution_role_arn = data.aws_arn.BENCHMARK_VM.arn
//  task_role_arn = data.aws_arn.BENCHMARK_VM.arn

  container_definitions = <<TASK_DEFINITION
[
    {
        "name": "benchmark-collector",
        "image": "grafana/agent:v0.6.1",
        "entryPoint": ["/app/benchmark-executor"],
        "environment": [
            {"name": "BENCHMARK_ENDPOINT_URL", "value": "http://${aws_alb.main.dns_name}:8000"},
            {"name": "BENCHMARK_STORAGE_NAMESPACE", "value": "s3://lakefs-benchmarking/${var.tag}"}
        ],
        "essential": true,
        "cpu": 1024,
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

