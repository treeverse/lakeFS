variable "dockerReg" {
  type        = string
  description = "docker registry to pull the image from"
  default = "some-registry"
}

variable "tag" {
  type        = string
  description = "lakeFS docker image tag"
  default = "dev"
}

variable "build" {
  type        = number
  description = "Benchmark Github action build number"
  default = 0
}

variable "grafana-username" {
  type        = string
  description = "Grafana Cloud collector username"
  default = "treeverse"
}

variable "grafana-password" {
  type        = string
  description = "Grafana Cloud collector password"
  default = "notThePassw0rd"
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

resource "random_string" "encryptKey" {
  length = 16
}

resource "random_string" "password" {
  length = 8
  lower = true
  upper = false
  number = true
  special = false
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
  name = "/ecs/benchmark/${var.build}"

  retention_in_days = 14
  tags = {
    Benchmark = var.tag
  }
}
