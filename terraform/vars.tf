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


variable "distribution_pkg_folder" {
  description = "Folder name to create distribution files..."
  default = "lambda_dist_pkg"
}