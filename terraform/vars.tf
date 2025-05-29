variable "aws_region" {
    default = "eu-west-2"
}

# potentially rename to inestion_lambda extract_lambda and change in IAM document
variable "change_detection_lambda" {
    default = "change_detection_lambda"
}

variable "transform_lambda" {
    default = "transform_lambda"
}

variable "load_lambda" {
    default = "load_lambda"
}