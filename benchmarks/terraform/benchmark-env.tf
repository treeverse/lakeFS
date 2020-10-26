#####
# DB
#####
module "db" {
  source = "github.com/terraform-aws-modules/terraform-aws-rds"

  identifier = "benchmarks-postgres-${var.tag}"

  engine            = "postgres"
  engine_version    = "11"
  instance_class    = "db.t2.medium"
  allocated_storage = 5
  storage_encrypted = false

  name = "BenchmarksDB"

  # Do NOT use 'user' as the value for 'username' as it throws:
  # "Error creating DB Instance: InvalidParameterValue: MasterUsername
  # user cannot be used as it is a reserved word used by the engine"
  username = "benchmarks"

  password = random_string.password.result
  port     = "5432"

  vpc_security_group_ids = [aws_security_group.benchmark_sg.id]

  maintenance_window = "Mon:00:00-Mon:03:00"
  backup_window      = "03:00-06:00"

  # disable backups to create DB faster
  backup_retention_period = 0

  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  # DB subnet group
  subnet_ids = data.aws_subnet_ids.all.ids

  # DB parameter group
  family = "postgres11"

  # DB option group
  major_engine_version = "11"

  skip_final_snapshot = true
  
  # Database Deletion Protection
  deletion_protection = false
}

resource "aws_ecs_task_definition" "benchmark" {
  family                = "benchmark-${var.tag}"
  requires_compatibilities = ["FARGATE"]
  cpu = "2048"
  memory = "8192"
  network_mode = "awsvpc"
  execution_role_arn = data.aws_arn.BENCHMARK_VM.arn
  task_role_arn = data.aws_arn.BENCHMARK_VM.arn

  container_definitions = <<TASK_DEFINITION
[
    {
        "name": "lakeFS",
        "image": "${var.dockerReg}/lakefs:${var.tag}",
        "entryPoint": ["/app/lakefs", "run"],
        "environment": [
            {"name": "LAKEFS_AUTH_ENCRYPT_SECRET_KEY", "value": "${random_string.encryptKey.result}"},
            {"name": "LAKEFS_DATABASE_CONNECTION_STRING", "value": "postgres://benchmarks:${random_string.password.result}@${module.db.this_db_instance_endpoint}/postgres?sslmode=disable"},
            {"name": "LAKEFS_BLOCKSTORE_TYPE", "value": "s3"},
            {"name": "LAKEFS_LOGGING_LEVEL", "value": "DEBUG"}
        ],
        "essential": true,
        "cpu": 2048,
        "memory": 8192,
        "logConfiguration": {
          "logDriver": "awslogs",
          "options": {
            "awslogs-group": "${aws_cloudwatch_log_group.benchmark.name}",
            "awslogs-region": "us-east-1",
            "awslogs-stream-prefix": "env"
          }
        },
        "portMappings": [
            {
                "containerPort": 8000,
                "hostPort": 8000
            }
        ]
    }
]
TASK_DEFINITION
}

resource "aws_ecs_service" "lakefs" {
  name            = "lakeFS-${var.tag}"
  cluster         = aws_ecs_cluster.benchmark.id
  task_definition = aws_ecs_task_definition.benchmark.id
  desired_count   = 1
  launch_type = "FARGATE"

  network_configuration {
    subnets = [for s in data.aws_subnet.all : s.id]
    assign_public_ip = false
    security_groups = [ aws_security_group.benchmark_sg.id ]
  }

  load_balancer {
    target_group_arn = aws_alb_target_group.benchmark.id
    container_name = "lakeFS"
    container_port = 8000
  }
  depends_on = [aws_alb_listener.benchmark]
}
