resource "aws_ecs_task_definition" "executor" {
  family                = "benchmark-executor-${var.tag}"
  requires_compatibilities = ["FARGATE"]
  cpu = "2048"
  memory = "8192"
  network_mode = "awsvpc"
  execution_role_arn = data.aws_arn.BENCHMARK_VM.arn
  task_role_arn = data.aws_arn.BENCHMARK_VM.arn

  container_definitions = <<TASK_DEFINITION
[
    {
        "name": "benchmark-executor",
        "image": "${var.dockerReg}/benchmark-executor:${var.tag}",
        "entryPoint": ["/app/benchmark-executor"],
        "environment": [
            {"name": "BENCHMARK_ENDPOINT_URL", "value": "http://${aws_alb.main.dns_name}:8000"},
            {"name": "BENCHMARK_STORAGE_NAMESPACE", "value": "s3://lakefs-benchmarking/${var.tag}"}
        ],
        "essential": true,
        "cpu": 2048,
        "memory": 8192,
        "logConfiguration": {
          "logDriver": "awslogs",
          "options": {
            "awslogs-group": "/ecs/benchmark/${var.tag}",
            "awslogs-region": "us-east-1",
            "awslogs-stream-prefix": "executor"
          }
        }
    }
]
TASK_DEFINITION
}

locals {
  executor_config = {
    network = {
      awsvpcConfiguration: {
        assignPublicIp = "DISABLED"
        subnets = aws_ecs_service.lakefs.network_configuration[0].subnets
        securityGroups = aws_ecs_service.lakefs.network_configuration[0].security_groups
      }
    }
  }
}

// Only a single run of the executor container is needed,
// hence provisioner is used instead of ecs service.
resource "null_resource" "run-executor" {
  triggers = {
    task_definition = aws_ecs_task_definition.executor.id
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command = <<EOF
set -e

aws ecs run-task \
  --cluster ${aws_ecs_cluster.benchmark.name} \
  --task-definition ${aws_ecs_task_definition.executor.family}:${aws_ecs_task_definition.executor.revision} \
  --launch-type FARGATE \
  --started-by "Terraform" \
  --network-configuration '${jsonencode(local.executor_config.network)}'
EOF
  }
}
