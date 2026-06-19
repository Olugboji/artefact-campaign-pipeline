terraform {
  backend "s3" {
    bucket         = "artefact-tf-state"
    key            = "dev/terraform.tfstate"
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
  env    = "dev"
}

module "iam" {
  source = "../../modules/iam"
  env    = "dev"
}

module "glue" {
  source            = "../../modules/glue"
  env                = "dev"
  num_workers        = 2
  glue_iam_role_arn  = module.iam.glue_role_arn
}

output "raw_bucket"     { value = module.s3.raw_bucket_name }
output "staging_bucket" { value = module.s3.staging_bucket_name }
output "glue_job"       { value = module.glue.glue_job_name }