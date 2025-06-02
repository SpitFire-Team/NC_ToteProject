resource "null_resource" "create_dependencies" {
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../${var.path_layer}/lambda_requirements.txt -t ${path.module}/../${var.path_layer}/dependencies/python"
  }
  triggers = {
    dependencies = filemd5("${path.module}/../${var.path_layer}/lambda_requirements.txt")
  }
}

data "archive_file" "layer_code" {
    type        = "zip"
    source_dir  = "${path.module}/../${var.path_layer}/dependencies"
    output_path = "${path.module}/../${var.path_layer}/layer.zip"
    depends_on = [null_resource.create_dependencies]
}

resource "aws_lambda_layer_version" "layer" {
  layer_name = "lambda_layer"
  compatible_architectures = ["x86_64"]
  compatible_runtimes = [var.runtime]
  s3_bucket  = aws_s3_object.lambda_layer.bucket
  s3_key     = aws_s3_object.lambda_layer.key
  #   filename = "${path.module}/../${var.path_layer}/layer.zip" - code if stored locally
  depends_on = [data.archive_file.layer_code]
}

resource "aws_s3_object" "lambda_layer" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "layer/layer.zip"
  source = data.archive_file.layer_code.output_path
  depends_on = [data.archive_file.layer_code]
}
