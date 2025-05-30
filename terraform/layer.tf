
# resource "aws_lambda_layer_version" "layer" {
#   layer_name = "lambda_layer"
#   filename = "layer.zip"
#   compatible_architectures = ["x86_64"]
#   compatible_runtimes = ["python3.9"]
# }


# resource "null_resource" "create_dependencies" {
#   provisioner "local-exec" {
#     command = "pip install -r ${path.module}/../requirements.txt -t ${path.module}/../dependencies/python"
#   }

#   triggers = {
#     dependencies = filemd5("${path.module}/../requirements.txt")
#   }
# }ter

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
