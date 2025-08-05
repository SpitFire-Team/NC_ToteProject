terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket  = "nctotes-terraform-backend-state-bucket-20250805"
    key     = "nc-totes/terraform.tfstate"
    region  = "eu-west-2"
  }
}

provider "aws" {
  region = var.aws_region
}


