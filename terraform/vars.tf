variable "aws_region" {
    default = "eu-west-2"
}

variable "extraction_lambda" {
    default = "extraction_lambda"
}

variable "step_function" {
    default = "step_function"
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


variable "step_function_name" {
  default= "totes-project-stepfunction"
}


