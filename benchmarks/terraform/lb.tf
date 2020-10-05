# ALB Security Group: Edit to restrict access to the application
resource "aws_security_group" "aws-lb" {
  name = "benchmark-load-balancer-${var.tag}"
  description = "Controls access to the ALB"
  vpc_id = aws_security_group.benchmark_sg.vpc_id

  ingress {
    protocol = "tcp"
    from_port = 8000
    to_port = 8000
    cidr_blocks = [for s in data.aws_subnet.all : s.cidr_block]
  }

  egress {
    protocol = "-1"
    from_port = 0
    to_port = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name = "benchmark-load-balancer"
  }
}

resource "aws_alb" "main" {
  name = "benchmark-${var.tag}"
  subnets = [for s in data.aws_subnet.all : s.id]
  security_groups = [aws_security_group.aws-lb.id]
  internal = true
  tags = {
    Name = "benchmark-alb"
  }
}

resource "aws_alb_target_group" "benchmark" {
  name = "benchmark-${var.tag}"
  port = 8000
  protocol = "HTTP"
  vpc_id = aws_security_group.benchmark_sg.vpc_id
  target_type = "ip"
  tags = {
    Name = "benchmark-alb-target-group"
  }
}

# Redirect all traffic from the ALB to the target group
resource "aws_alb_listener" "benchmark" {
  load_balancer_arn = aws_alb.main.id
  port = 8000
  protocol = "HTTP"
  default_action {
    target_group_arn = aws_alb_target_group.benchmark.id
    type = "forward"
  }
}
