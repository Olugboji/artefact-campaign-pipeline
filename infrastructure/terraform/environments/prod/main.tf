terraform {
  backend "s3" {
    bucket         = "artefact-tf-state"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "artefact-tf-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
}

module "s3" {
  source = "../../modules/s3"
  env    = "prod"
}

module "iam" {
  source = "../../modules/iam"
  env    = "prod"
}

module "glue" {
  source            = "../../modules/glue"
  env                = "prod"
  num_workers        = 4
  glue_iam_role_arn  = module.iam.glue_role_arn
}

output "raw_bucket"     { value = module.s3.raw_bucket_name }
output "staging_bucket" { value = module.s3.staging_bucket_name }
output "glue_job"       { value = module.glue.glue_job_name }