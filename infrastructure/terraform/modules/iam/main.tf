variable "env" {}

data "aws_iam_role" "glue_role" {
  name = "ArtifactGlueRole"
}

output "glue_role_arn" {
  value = data.aws_iam_role.glue_role.arn
}