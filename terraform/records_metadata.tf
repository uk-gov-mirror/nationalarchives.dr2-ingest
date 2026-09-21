locals {
  records_metadata_bucket_name = "${local.environment}-dr2-records-metadata"
  ayr_worker_role              = module.ayr_config.terraform_config["prod"]["dri-to-ayr-data-migration-worker-lambda-role"]
  ayr_coordinator_role         = module.ayr_config.terraform_config["prod"]["dri-to-ayr-data-migration-coordinator-lambda-role"]
}
module "dr2_records_metadata_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2-records-metadata"
  default_policy_variables = {
    ci_roles = [local.terraform_role_arn],
    user_roles = [
      data.aws_iam_role.org_wiz_access_role.arn,
      data.aws_ssm_parameter.dev_admin_role.value,
      module.dri_preingest.importer_lambda.role,
      local.ayr_worker_role,
      local.ayr_coordinator_role
    ]
  }
}

module "records_metadata_bucket" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name     = local.records_metadata_bucket_name
  kms_key_arn     = module.dr2_records_metadata_key.kms_key_arn
  lifecycle_rules = local.lifecycle_rules
  bucket_policy = templatefile("${path.module}/templates/s3/records_metadata_bucket_policy.json.tpl", {
    ayr_data_migration_worker_role      = local.ayr_worker_role
    ayr_data_migration_coordinator_role = local.ayr_coordinator_role
    bucket_name                         = local.records_metadata_bucket_name
  })
}
