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