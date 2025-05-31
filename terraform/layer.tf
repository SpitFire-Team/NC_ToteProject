resource "null_resource" "create_dependencies" {
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../${var.path_layer}/lambda_requirements.txt -t ${path.module}/../${var.path_layer}/dependencies/python"
  }
  triggers = {
    dependencies = filemd5("${path.module}/../${var.path_layer}/lambda_requirements.txt")
  }
}

data "archive_file" "lambda_requirements" {
    type        = "zip"
    source_dir  = "${path.module}/../${var.path_layer}/dependencies"
    output_path = "${path.module}/../${var.path_layer}/layer.zip"
    depends_on = [null_resource.create_dependencies]
}

resource "aws_lambda_layer_version" "layer" {
  layer_name = "lambda_layer"
  filename = "${path.module}/../${var.path_layer}/layer.zip"
  compatible_architectures = ["x86_64"]
  compatible_runtimes = [var.runtime]
  depends_on = [data.archive_file.lambda_requirements]
}


