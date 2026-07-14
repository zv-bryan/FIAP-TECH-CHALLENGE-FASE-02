terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Bucket principal do data lake.
# Nele ficam Bronze, Silver e Gold separados por prefixo.
resource "aws_s3_bucket" "datalake" {
  bucket = var.bucket_name
}

# Regra simples de lifecycle para mostrar preocupação com custo.
# Depois de 30 dias, a Bronze pode ir para uma classe mais barata.
resource "aws_s3_bucket_lifecycle_configuration" "datalake_lifecycle" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "bronze-old-to-ia"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

# Stream usado para simular a parte de ingestão em tempo quase real.
resource "aws_kinesis_stream" "indicadores" {
  name             = "tc-alfabetizacao-indicadores"
  shard_count      = 1
  retention_period = 24
}

# Role básica para o Glue.
# Em um ambiente produtivo eu refinaria mais as permissões de S3.
resource "aws_iam_role" "glue_role" {
  name = "tc-alfabetizacao-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "glue.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
