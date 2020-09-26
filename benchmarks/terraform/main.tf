variable "password" {
  type        = string
  description = "The password to the postgres DB."
}

variable "dockerReg" {
  type        = string
  description = "docker registry to pull the image from"
}

variable "tag" {
  type        = string
  description = "lakeFS docker image tag"
}

provider "aws" {
  region = "us-east-1"
}

//##############################################################
//# Data sources to get VPC, subnets and security group details
//##############################################################
data "aws_subnet_ids" "all" {
  vpc_id = "vpc-04b176d1264698ffc"
  tags = {"Type":"private"}
}

data "aws_subnet" "all" {
  for_each = data.aws_subnet_ids.all.ids
  id       = each.value
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-trusty-14.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

data "aws_arn" "BENCHMARK_VM" {
  arn = "arn:aws:iam::977611293394:role/BENCHMARK_VM"
}

//##############################################################
//# Resources to create sg, postgres db, and Fargate service
//##############################################################
resource "aws_security_group" "benchmark_sg" {
  name        = "benchmark_sg-${var.tag}"
  description = "Allow benchmark traffic"
  vpc_id      = "vpc-04b176d1264698ffc"

  ingress {
    description = "postgres"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [for s in data.aws_subnet.all : s.cidr_block]
  }

  ingress {
    description = "lakeFS"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = [for s in data.aws_subnet.all : s.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "allow_benchmark"
  }
}

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

  password = var.password
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

  # Snapshot name upon DB deletion
  final_snapshot_identifier = "demodb"

  # Database Deletion Protection
  deletion_protection = false
}

resource "random_string" "default" {
  length = 16
}

resource "aws_launch_configuration" "benchmark" {
  name          = "benchmark-launch-${var.tag}"
  image_id      = data.aws_ami.ubuntu.id
  instance_type = "t2.micro"
}

resource "aws_autoscaling_group" "benchmark" {
  vpc_zone_identifier = [for s in data.aws_subnet.all : s.id]
  min_size = 1
  max_size = 1
  launch_configuration = aws_launch_configuration.benchmark.id

  tag {
    key                 = "AmazonECSManaged"
    value = "true"
    propagate_at_launch = true
  }
}

resource "aws_ecs_cluster" "benchmark" {
  name = "benchmark-${var.tag}"
  capacity_providers = ["FARGATE"]
}

resource "aws_cloudwatch_log_group" "benchmark" {
  name = "/ecs/benchmark/${var.tag}"

  tags = {
    Benchmark = var.tag
  }
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
            {"name": "LAKEFS_AUTH_ENCRYPT_SECRET_KEY", "value": "${random_string.default.result}"},
            {"name": "LAKEFS_DATABASE_CONNECTION_STRING", "value": "postgres://benchmarks:${var.password}@${module.db.this_db_instance_endpoint}/postgres?sslmode=disable"},
            {"name": "LAKEFS_BLOCKSTORE_TYPE", "value": "s3"},
            {"name": "LAKEFS_LOGGING_LEVEL", "value": "DEBUG"}
        ],
        "essential": true,
        "cpu": 2048,
        "memory": 8192,
        "logConfiguration": {
          "logDriver": "awslogs",
          "options": {
            "awslogs-group": "/ecs/benchmark/${var.tag}",
            "awslogs-region": "us-east-1",
            "awslogs-stream-prefix": "ecs"
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

