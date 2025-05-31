resource "null_resource" "create_dependencies" {
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../src/layer/dependencies/lambda_requirements.txt -t ${path.module}/../src/layer/dependencies/python"
  }

  triggers = {
    dependencies = filemd5("${path.module}/../requirements.txt")
  }
}

resource "aws_lambda_layer_version" "layer" {
  layer_name = "lambda_layer"
  filename = "${path.module}/../src/layer/layer.zip"
  compatible_architectures = ["x86_64"]
  compatible_runtimes = [var.runtime]
}


# data "archive_file" "lambda_requirements" {
#     type        = "zip"
#     source_file = "${path.module}/../src/layer/lambda_requirements.txt"
#     output_path = "${path.module}/../src/layer/layer.zip"
# }

# data "archive_file" "layer_code" {
#   type        = "zip"
#   output_path = "${path.module}/../src/layer/layer.zip"
#   source_dir  = "${path.module}/../src/dependencies"
# }

# resource "aws_lambda_layer_version" "dependencies" {
#   layer_name = "requests_library_layer"
#   s3_bucket  = aws_s3_object.lambda_layer.bucket
#   s3_key     = aws_s3_object.lambda_layer.key
# }
