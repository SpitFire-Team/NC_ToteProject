variable "aws_region" {
    default = "eu-west-2"
}

variable "extraction_lambda" {
    default = "extraction_lambda"
}

variable "transform_lambda" {
    default = "transform_lambda"
}

variable "load_lambda" {
    default = "load_lambda"
}

variable "runtime" {
  default = "python3.13"
}

variable "path_layer" {
  default = "src/layer"
}

variable "user" {}
variable "password" {}
variable "host" {}
variable "port" {}
variable "database" {}