variable "env" {}
variable "num_workers" { default = 2 }
variable "glue_iam_role_arn" {}

resource "aws_glue_job" "transform" {
  name         = "artefact-transform-${var.env}"
  role_arn     = var.glue_iam_role_arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://artefact-${var.env}-staging/scripts/transform.py"
    python_version  = "3"
  }

  default_arguments = {
    "--ENV"                              = var.env
    "--RAW_BUCKET"                       = "artefact-${var.env}-raw"
    "--STAGING_BUCKET"                   = "artefact-${var.env}-staging"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://artefact-${var.env}-staging/tmp/"
  }

  number_of_workers = var.num_workers
  worker_type       = "G.1X"
  timeout           = 10
}

output "glue_job_name" {
  value = aws_glue_job.transform.name
}