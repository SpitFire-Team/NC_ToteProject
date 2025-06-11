#create layer 1 reseources

resource "null_resource" "create_layer1_dependencies" {
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../${var.path_layer}/lambda_requirements1.txt -t ${path.module}/../${var.path_layer}/dependencies1/python"
  }
  triggers = {
    dependencies = filemd5("${path.module}/../${var.path_layer}/lambda_requirements1.txt")
  }
}

data "archive_file" "layer1_code" {
    type        = "zip"
    source_dir  = "${path.module}/../${var.path_layer}/dependencies1"
    output_path = "${path.module}/../${var.path_layer}/layer1.zip"
    depends_on = [null_resource.create_layer1_dependencies]
}

resource "aws_lambda_layer_version" "layer1" {
  layer_name = "lambda_layer1"
  compatible_architectures = ["x86_64"]
  compatible_runtimes = [var.runtime]
  s3_bucket  = aws_s3_object.lambda_layer1.bucket
  s3_key     = aws_s3_object.lambda_layer1.key
  # source_code_hash = filebase64sha256(data.archive_file.layer1_code.output_path)
  source_code_hash = data.archive_file.layer1_code.output_base64sha256
  depends_on = [data.archive_file.layer1_code]
}

resource "aws_s3_object" "lambda_layer1" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "layer/layer1.zip"
  source = data.archive_file.layer1_code.output_path
  depends_on = [data.archive_file.layer1_code]
}



#create layer 2 reseources


resource "null_resource" "create_layer2_dependencies" {
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../${var.path_layer}/lambda_requirements2.txt -t ${path.module}/../${var.path_layer}/dependencies2/python"
  }
  triggers = {
    dependencies = filemd5("${path.module}/../${var.path_layer}/lambda_requirements2.txt")
  }
}

data "archive_file" "layer2_code" {
    type        = "zip"
    source_dir  = "${path.module}/../${var.path_layer}/dependencies2"
    output_path = "${path.module}/../${var.path_layer}/layer2.zip"
    depends_on = [null_resource.create_layer2_dependencies]
}

resource "aws_lambda_layer_version" "layer2" {
  layer_name = "lambda_layer2"
  compatible_architectures = ["x86_64"]
  compatible_runtimes = [var.runtime]
  s3_bucket  = aws_s3_object.lambda_layer2.bucket
  s3_key     = aws_s3_object.lambda_layer2.key
  # source_code_hash = filebase64sha256(data.archive_file.layer2_code.output_path)
  source_code_hash = data.archive_file.layer2_code.output_base64sha256
}

resource "aws_s3_object" "lambda_layer2" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "layer/layer2.zip"
  source = data.archive_file.layer2_code.output_path
  depends_on = [data.archive_file.layer2_code]
}
