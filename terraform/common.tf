locals {
  az_count                                             = local.environment == "prod" ? 2 : 1
  ingest_raw_cache_bucket_name                         = "${local.environment}-dr2-ingest-raw-cache"
  sample_files_bucket_name                             = "${local.environment}-dr2-sample-files"
  ingest_state_bucket_name                             = "${local.environment}-dr2-ingest-state"
  ingest_step_function_name                            = "${local.environment}-dr2-ingest"
  ingest_step_function_mapper_lambda_state_name        = "Get metadata and update Files table"
  ingest_run_workflow_step_function_name               = "${local.environment}-dr2-ingest-run-workflow"
  additional_user_roles                                = local.environment != "prod" ? [data.aws_ssm_parameter.dev_admin_role.value] : []
  e2e_test_roles                                       = local.environment == "prod" ? [] : [module.dr2_run_e2e_tests_role[0].role_arn]
  files_dynamo_table_name                              = "${local.environment}-dr2-ingest-files"
  ingest_lock_dynamo_table_name                        = "${local.environment}-dr2-ingest-lock"
  ingest_queue_dynamo_table_name                       = "${local.environment}-dr2-ingest-queue"
  enable_point_in_time_recovery                        = true
  files_table_batch_parent_global_secondary_index_name = "BatchParentPathIdx"
  ingest_lock_table_group_id_gsi_name                  = "IngestLockGroupIdx"
  ingest_lock_table_hash_key                           = "assetId"
  dev_notifications_channel_id                         = local.environment == "prod" ? "C06EDJPF0VB" : "C052LJASZ08"
  general_notifications_channel_id                     = local.environment == "prod" ? "C06E20AR65V" : "C068RLCPZFE"
  java_runtime                                         = "java21"
  architecture_arm64                                   = "arm64"
  java_lambda_memory_size                              = 512
  java_timeout_seconds                                 = 180
  python_runtime                                       = "python3.14"
  python_lambda_memory_size                            = 128
  python_timeout_seconds                               = 30
  step_function_failure_log_group                      = "step-function-failures"
  terraform_role_arn                                   = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.environment_title}TerraformRole"
  dr2_preservica_tenants                               = { "intg" = "tnatest", "staging" = "tnastage", "prod" = "tna" }
  preservica_tenant                                    = local.dr2_preservica_tenants[local.environment]
  preservica_ingest_bucket                             = "com.preservica.${local.preservica_tenant}.bulk1"
  tna_to_preservica_role_arn                           = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.environment}-tna-to-preservica-ingest-s3-${local.preservica_tenant}"
  creator                                              = "dr2-terraform-environments"
  sse_encryption                                       = "sse"
  visibility_timeout                                   = 180
  redrive_maximum_receives                             = 5
  tre_environment_name                                 = local.environment == "intg" ? "int" : local.environment
  nacl_inbound_from_subnet_https = [for idx, cidr in module.vpc.private_cidr_blocks : {
    rule_no    = 100 * (idx + 2)
    cidr_block = cidr
    action     = "allow"
    from_port  = 443
    to_port    = 443
    egress     = false
  }]
  nacl_outbound_to_subnet_ephemeral = [for idx, cidr in module.vpc.private_cidr_blocks : {
    rule_no    = 100 * (idx + 2)
    cidr_block = cidr
    action     = "allow"
    from_port  = 1024
    to_port    = 65535
    egress     = true
  }]
  aws_region_name             = data.aws_region.current.region
  ingest_run_workflow_sfn_arn = "arn:aws:states:${local.aws_region_name}:${data.aws_caller_identity.current.account_id}:stateMachine:${local.ingest_run_workflow_step_function_name}"
  dashboard_lambdas = concat([
    local.entity_event_lambda_name,
    local.get_latest_preservica_version,
    local.ingest_asset_opex_creator_lambda_name,
    local.ingest_asset_reconciler_lambda_name,
    local.ingest_failure_notifications_lambda_name,
    local.ingest_find_existing_asset_name,
    local.ingest_folder_opex_creator_lambda_name,
    local.ingest_mapper_lambda_name,
    local.ingest_parent_folder_opex_creator_lambda_name,
    local.ingest_queue_creator_name,
    local.ingest_start_workflow_lambda_name,
    local.ingest_upsert_archive_folders_lambda_name,
    local.ingest_validate_generic_ingest_inputs_lambda_name,
    local.ingest_workflow_monitor_lambda_name,
    local.ip_lock_checker_lambda_name,
    local.rotate_preservation_system_password_name,
    module.tdr_preingest.aggregator_lambda.function_name,
    module.tdr_preingest.package_builder_lambda.function_name,
    module.tdr_preingest.importer_lambda.function_name,
    module.dri_preingest.aggregator_lambda.function_name,
    module.dri_preingest.package_builder_lambda.function_name,
    module.dri_preingest.importer_lambda.function_name,
    module.ad_hoc_preingest.aggregator_lambda.function_name,
    module.ad_hoc_preingest.package_builder_lambda.function_name,
    module.ad_hoc_preingest.importer_lambda.function_name
  ])
  custodial_copy_queues = [
    module.dr2_custodial_copy_queue,
    module.dr2_custodial_copy_queue_creator_queue,
    module.dr2_custodial_copy_db_builder_queue
  ]
  ingest_queues = flatten([
    module.dr2_external_notifications_queue,
    module.cleanup_trigger_queue,
    local.custodial_copy_queues
  ])


  retry_statement            = jsonencode([{ ErrorEquals = ["States.ALL"], IntervalSeconds = 2, MaxAttempts = 6, BackoffRate = 2, JitterStrategy = "FULL" }])
  messages_visible_threshold = 1000000
  # The list comes from https://www.cloudflare.com/en-gb/ips
  cloudflare_ip_ranges                        = toset(["173.245.48.0/20", "103.21.244.0/22", "103.22.200.0/22", "103.31.4.0/22", "141.101.64.0/18", "108.162.192.0/18", "190.93.240.0/20", "188.114.96.0/20", "197.234.240.0/22", "198.41.128.0/17", "162.158.0.0/15", "104.16.0.0/13", "104.24.0.0/14", "172.64.0.0/13", "131.0.72.0/22"])
  clouflare_and_vpc_endpoints_security_groups = [module.outbound_cloudflare_https_access.security_group_id, module.https_to_vpc_endpoints_security_group.security_group_id]
  tdr_export_bucket                           = "tdr-export-${local.environment}"
  lifecycle_rules = jsondecode(local.environment == "prod" ? "[]" : jsonencode([
    { id = "delete-noncurrent-versions", status = "Enabled", noncurrent_version_expiration = { noncurrent_days = 1 } },
    { id = "expire-current-versions", status = "Enabled", expiration = { days = 29 } },
    { id = "expire-object-delete-marker", status = "Enabled", expiration = { expired_object_delete_marker = true } }
  ]))
  source_systems = ["TDR", "COURTDOC", "ADHOC", "DRI", "DEFAULT"]
  default        = local.source_systems[index(local.source_systems, "DEFAULT")]
  tdr            = local.source_systems[index(local.source_systems, "TDR")]
  adhoc          = local.source_systems[index(local.source_systems, "ADHOC")]
  dri            = local.source_systems[index(local.source_systems, "DRI")]

  flow_control_configs = {
    intg = {
      maxConcurrency = 2,
      enabled        = true,
      sourceSystems = [
        {
          systemName       = local.default
          reservedChannels = 0
          probability      = 100
        }
      ]
    }
    prod = {
      maxConcurrency = 4,
      enabled        = true,
      sourceSystems = [
        {
          systemName       = local.dri
          reservedChannels = 0
          probability      = 1
        },
        {
          systemName       = local.default
          reservedChannels = 1
          probability      = 99
        }
      ]
    }
    staging = {
      maxConcurrency = 2,
      enabled        = true,
      sourceSystems = [
        {
          systemName       = local.default
          reservedChannels = 0
          probability      = 100
        }
      ]
    }
  }
  selected_flow_control_config = local.flow_control_configs[local.environment]
}

data "aws_iam_role" "org_wiz_access_role" {
  name = "org-wiz-access-role"
}

resource "random_password" "preservica_password" {
  length = 20
}

resource "random_string" "preservica_user" {
  length  = 10
  special = false
}

resource "aws_secretsmanager_secret" "preservica_secret" {
  name = "${local.environment}-preservica-api-login-details-${random_string.preservica_user.result}"
}

resource "aws_secretsmanager_secret" "preservica_read_metadata_read_content" {
  name = "${local.environment}-preservica-api-read-metadata-read-content"
}

resource "aws_secretsmanager_secret" "preservica_read_metadata" {
  name = "${local.environment}-preservica-api-read-metadata"
}

resource "aws_secretsmanager_secret" "preservica_read_update_metadata_insert_content" {
  name = "${local.environment}-preservica-api-read-update-metadata-insert-content"
}

resource "aws_secretsmanager_secret_rotation" "secret_rotation" {
  for_each = toset([
    aws_secretsmanager_secret.preservica_secret.id,
    aws_secretsmanager_secret.preservica_read_metadata_read_content.id,
    aws_secretsmanager_secret.preservica_read_metadata.id,
    aws_secretsmanager_secret.preservica_read_update_metadata_insert_content.id
  ])
  rotation_lambda_arn = module.dr2_rotate_preservation_system_password_lambda.lambda_arn
  secret_id           = each.key
  rotation_rules {
    schedule_expression = "rate(4 hours)"
  }
}

resource "aws_secretsmanager_secret" "demo_preservica_secret" {
  name = "${local.environment}-demo-preservica-api-login-details-${random_string.preservica_user.result}"
}

data "aws_ssm_parameter" "slack_webhook_url" {
  name = "/${local.environment}/slack/cloudwatch-alarm-webhook"
}

data "aws_region" "current" {}

module "vpc" {
  source                    = "git::https://github.com/nationalarchives/da-terraform-modules//vpc"
  vpc_name                  = "${local.environment}-vpc"
  az_count                  = local.az_count
  elastic_ip_allocation_ids = data.aws_eip.eip.*.id
  use_nat_gateway           = var.disable_networking == false
  create_s3_gateway_endpoint = var.disable_networking == false
  create_dynamo_gateway_endpoint = var.disable_networking == false
  environment               = local.environment
  private_nacl_rules = concat([
    { rule_no = 100, cidr_block = "0.0.0.0/0", action = "allow", from_port = 443, to_port = 443, egress = true },
    { rule_no = 100, cidr_block = "0.0.0.0/0", action = "allow", from_port = 1024, to_port = 65535, egress = false },
  ], local.nacl_inbound_from_subnet_https, local.nacl_outbound_to_subnet_ephemeral)
  public_nacl_rules = concat([
    { rule_no = 100, cidr_block = "0.0.0.0/0", action = "allow", from_port = 1024, to_port = 65535, egress = false },
    { rule_no = 100, cidr_block = "0.0.0.0/0", action = "allow", from_port = 443, to_port = 443, egress = true },
    { rule_no = 200, cidr_block = "0.0.0.0/0", action = "allow", from_port = 1024, to_port = 65535, egress = true },
  ], local.nacl_inbound_from_subnet_https)
  s3_gateway_endpoint_policy = templatefile("${path.module}/templates/vpc/s3_endpoint_policy.json.tpl", {
    account_id               = data.aws_caller_identity.current.account_id,
    preservica_ingest_bucket = local.preservica_ingest_bucket
    tdr_export_bucket        = local.tdr_export_bucket
    tre_export_bucket_arn    = module.tre_config.terraform_config[local.tre_environment_name]["s3_common_bucket_arn"]
    object_store_bucket_name = local.object_store_bucket_name
  })
  dynamo_gateway_endpoint_policy = templatefile("${path.module}/templates/vpc/dynamo_endpoint_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id
  })

  interface_endpoints = var.disable_networking ? {
    secretsmanager = {
      name = "com.amazonaws.${local.aws_region_name}.secretsmanager",
      policy = templatefile("${path.module}/templates/vpc/default_endpoint_policy.json.tpl", {
        service_name = "secretsmanager"
        org_id       = data.aws_organizations_organization.org.id
      })
      security_group_ids = [module.interface_endpoints_security_group.security_group_id]
      enable_private_dns = true
    },
    stepfunctions = {
      name = "com.amazonaws.${local.aws_region_name}.states",
      policy = templatefile("${path.module}/templates/vpc/default_endpoint_policy.json.tpl", {
        service_name = "states"
        org_id       = data.aws_organizations_organization.org.id
      })
      security_group_ids = [module.interface_endpoints_security_group.security_group_id]
      enable_private_dns = true
    },
    sns = {
      name = "com.amazonaws.${local.aws_region_name}.sns",
      policy = templatefile("${path.module}/templates/vpc/default_endpoint_policy.json.tpl", {
        service_name = "sns"
        org_id       = data.aws_organizations_organization.org.id
      })
      security_group_ids = [module.interface_endpoints_security_group.security_group_id]
      enable_private_dns = true
    },
    sqs = {
      name = "com.amazonaws.${local.aws_region_name}.sqs",
      policy = templatefile("${path.module}/templates/vpc/default_endpoint_policy.json.tpl", {
        service_name = "sqs"
        org_id       = data.aws_organizations_organization.org.id
      })
      security_group_ids = [module.interface_endpoints_security_group.security_group_id]
      enable_private_dns = true
    },
    sts = {
      name = "com.amazonaws.${local.aws_region_name}.sts",
      policy = templatefile("${path.module}/templates/vpc/default_endpoint_policy.json.tpl", {
        service_name = "sts"
        org_id       = data.aws_organizations_organization.org.id
      })
      security_group_ids = [module.interface_endpoints_security_group.security_group_id]
      enable_private_dns = true
    },
  } : {}
}

data "aws_eip" "eip" {
  count = local.az_count
  filter {
    name   = "tag:Name"
    values = ["${local.environment}-eip-${count.index}"]
  }
}

data "aws_ec2_managed_prefix_list" "s3_prefix_list" {
  name = "com.amazonaws.eu-west-2.s3"
}

data "aws_ec2_managed_prefix_list" "dynamo_db_prefix_list" {
  name = "com.amazonaws.eu-west-2.dynamodb"
}

module "outbound_https_access_for_s3" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//security_group"
  common_tags = {}
  description = "A  security group to allow outbound HTTPS only to S3"
  name        = "${local.environment}-outbound-https-to-s3"
  vpc_id      = module.vpc.vpc.id
  rules = {
    egress = [
      {
        port           = 443
        description    = "Outbound https for S3 access"
        prefix_list_id = data.aws_ec2_managed_prefix_list.s3_prefix_list.id
        protocol       = "tcp"
      }
    ]
  }
}

module "outbound_https_access_for_dynamo_db" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//security_group"
  common_tags = {}
  description = "A  security group to allow outbound HTTPS only to DynamoDB"
  name        = "${local.environment}-outbound-https-to-dynamodb"
  vpc_id      = module.vpc.vpc.id
  rules = {
    egress = [
      {
        port           = 443
        description    = "Outbound https for DynamoDB access"
        prefix_list_id = data.aws_ec2_managed_prefix_list.dynamo_db_prefix_list.id
        protocol       = "tcp"
      }
    ]
  }
}

module "outbound_https_to_discovery" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//security_group"
  common_tags = {}
  description = "A security group to allow outbound access to discovery"
  name        = "${local.environment}-outbound-https-to-discovery"
  rules = {
    egress = [
      {
        port              = 443
        description       = "Outbound https to discovery VPC endpoint"
        security_group_id = module.discovery_inbound_https.security_group_id
        protocol          = "tcp"
      },
    ]
  }
  vpc_id = module.vpc.vpc.id
}

resource "aws_ec2_managed_prefix_list" "cloudflare_prefix_list" {
  address_family = "IPv4"
  max_entries    = length(local.cloudflare_ip_ranges) + 5
  name           = "${local.environment}-cloudflare-ranges"
  dynamic "entry" {
    for_each = local.cloudflare_ip_ranges
    content {
      cidr = entry.value
    }
  }
}

module "outbound_cloudflare_https_access" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//security_group"
  common_tags = {}
  description = "A security group to allow outbound access to Cloudflare IPs only"
  name        = "${local.environment}-outbound-https-to-cloudflare"
  vpc_id      = module.vpc.vpc.id
  rules = {
    egress = [{
      port           = 443
      description    = "Outbound https Cloudflare access",
      prefix_list_id = aws_ec2_managed_prefix_list.cloudflare_prefix_list.id
      protocol       = "tcp"
    }]
  }
}

module "dr2_kms_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2"
  default_policy_variables = {
    user_roles = concat([
      data.aws_iam_role.org_wiz_access_role.arn,
      module.ingest_find_existing_asset.lambda_role_arn,
      module.ingest_find_existing_asset.lambda_role_arn,
      module.dr2_ingest_validate_generic_ingest_inputs_lambda.lambda_role_arn,
      module.dr2_ingest_mapper_lambda.lambda_role_arn,
      module.dr2_ingest_asset_opex_creator_lambda.lambda_role_arn,
      module.dr2_ingest_folder_opex_creator_lambda.lambda_role_arn,
      module.dr2_ingest_upsert_archive_folders_lambda.lambda_role_arn,
      module.dr2_ingest_parent_folder_opex_creator_lambda.lambda_role_arn,
      module.dr2_ingest_asset_reconciler_lambda.lambda_role_arn,
      module.dr2_ingest_step_function.step_function_role_arn,
      module.tdr_preingest.aggregator_lambda.role,
      module.tdr_preingest.package_builder_lambda.role,
      module.tdr_preingest.importer_lambda.role,
      module.dri_preingest.aggregator_lambda.role,
      module.dri_preingest.package_builder_lambda.role,
      module.dri_preingest.importer_lambda.role,
      module.ad_hoc_preingest.aggregator_lambda.role,
      module.ad_hoc_preingest.package_builder_lambda.role,
      module.ad_hoc_preingest.importer_lambda.role,
      module.cc_restore_preingest.aggregator_lambda.role,
      module.cc_restore_preingest.package_builder_lambda.role,
      module.cc_restore_preingest.importer_lambda.role,
      module.court_document_preingest.aggregator_lambda.role,
      module.court_document_preingest.package_builder_lambda.role,
      module.court_document_preingest.importer_lambda.role,
      module.cleanup_handler_lambda.lambda_role_arn,
      local.tna_to_preservica_role_arn,
    ], local.additional_user_roles, local.e2e_test_roles)
    ci_roles = [local.terraform_role_arn]
    service_details = [
      { service_name = "cloudwatch" },
      { service_name = "sns", service_source_account = module.tre_config.account_numbers["prod"] },
      { service_name = "sns" },
    ]
  }
}

module "dr2_developer_key" {
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//kms"
  key_name = "${local.environment}-kms-dr2-dev"
  default_policy_variables = {
    user_roles = [
      data.aws_ssm_parameter.dev_admin_role.value,
      data.aws_iam_role.org_wiz_access_role.arn,
      module.dr2_ingest_mapper_lambda.lambda_role_arn,
      module.dr2_ingest_step_function.step_function_role_arn
    ]
    ci_roles = [local.terraform_role_arn]
    service_details = [
      { service_name = "s3" },
      { service_name = "sns" },
      { service_name = "logs.eu-west-2" },
      { service_name = "cloudwatch" }
    ]
  }
}

data "aws_ssm_parameter" "dev_admin_role" {
  name = "/${local.environment}/developer_role"
}

data "aws_ssm_parameter" "archivist_role" {
  name = "/${local.environment}/archivist_role"
}

module "ingest_raw_cache_bucket" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.ingest_raw_cache_bucket_name
  kms_key_arn = module.dr2_kms_key.kms_key_arn
  lifecycle_rules = concat(
    local.lifecycle_rules,
    [
      {
        id     = "delete-objects-with-to-be-deleted-tag"
        status = "Enabled"
        filter = {
          tag = {
            key   = "TO_BE_DELETED"
            value = "true"
          }
        }
        expiration = {
          days = 1
        }
      },
      {
        id     = "delete-expired-delete-markers"
        status = "Enabled",
        expiration = {
          expired_object_delete_marker = true
        }
      },
      {
        id     = "delete-noncurrent-versions-after-seven-days"
        status = "Enabled"
        noncurrent_version_expiration = {
          noncurrent_days = 7
        }
      }
  ])
}


module "sample_files_bucket" {
  source            = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name       = local.sample_files_bucket_name
  create_log_bucket = false
  kms_key_arn       = module.dr2_kms_key.kms_key_arn
  lifecycle_rules   = local.lifecycle_rules
}

module "create_ingest_sfn_lambda_alias" {
  source     = "./create_lambda_alias"
  alias_name = local.lambda_alias_name
  lambdas = {
    (local.ingest_mapper_lambda_name)                         = module.dr2_ingest_mapper_lambda.lambda_function.version
    (local.ingest_validate_generic_ingest_inputs_lambda_name) = module.dr2_ingest_validate_generic_ingest_inputs_lambda.lambda_function.version
    (local.ingest_find_existing_asset_name)                   = module.ingest_find_existing_asset.lambda_function.version
    (local.ingest_asset_opex_creator_lambda_name)             = module.dr2_ingest_asset_opex_creator_lambda.lambda_function.version
    (local.ingest_folder_opex_creator_lambda_name)            = module.dr2_ingest_folder_opex_creator_lambda.lambda_function.version
    (local.ingest_parent_folder_opex_creator_lambda_name)     = module.dr2_ingest_parent_folder_opex_creator_lambda.lambda_function.version
    (local.ingest_asset_reconciler_lambda_name)               = module.dr2_ingest_asset_reconciler_lambda.lambda_function.version
    (local.ingest_flow_control_lambda_name)                   = module.dr2_ingest_flow_control_lambda.lambda_function.version
  }
}


module "dr2_ingest_step_function" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sfn"
  step_function_definition = templatefile("${path.module}/templates/sfn/ingest_sfn_definition.json.tpl", {
    step_function_name                                = local.ingest_step_function_name,
    account_id                                        = data.aws_caller_identity.current.account_id
    ingest_validate_generic_ingest_inputs_lambda_name = local.ingest_validate_generic_ingest_inputs_lambda_name
    ingest_mapper_lambda_name                         = local.ingest_mapper_lambda_name
    ingest_mapper_lambda_state_name                   = local.ingest_step_function_mapper_lambda_state_name
    ingest_find_existing_asset_name_lambda_name       = local.ingest_find_existing_asset_name
    ingest_asset_opex_creator_lambda_name             = local.ingest_asset_opex_creator_lambda_name
    ingest_folder_opex_creator_lambda_name            = local.ingest_folder_opex_creator_lambda_name
    ingest_parent_folder_opex_creator_lambda_name     = local.ingest_parent_folder_opex_creator_lambda_name
    ingest_asset_reconciler_lambda_name               = local.ingest_asset_reconciler_lambda_name
    ingest_lock_table_name                            = local.ingest_lock_dynamo_table_name
    ingest_lock_table_group_id_gsi_name               = local.ingest_lock_table_group_id_gsi_name
    ingest_lock_table_hash_key                        = local.ingest_lock_table_hash_key
    ingest_run_workflow_sfn_name                      = local.ingest_run_workflow_step_function_name
    notifications_topic_name                          = local.notifications_topic_name
    ingest_state_bucket_name                          = local.ingest_state_bucket_name
    ingest_raw_cache_bucket_name                      = local.ingest_raw_cache_bucket_name
    preservica_bucket_name                            = local.preservica_ingest_bucket
    ingest_files_table_name                           = local.files_dynamo_table_name
    ingest_queue_table_name                           = local.ingest_queue_dynamo_table_name
    ingest_flow_control_lambda_name                   = local.ingest_flow_control_lambda_name
    retry_statement                                   = local.retry_statement
    postingest_table_name                             = module.postingest.postingest_table_name
    alias_name                                        = local.lambda_alias_name
  })
  step_function_name = local.ingest_step_function_name
  step_function_role_policy_attachments = {
    step_function_policy = module.dr2_ingest_step_function_policy.policy_arn
  }
  depends_on = [module.create_ingest_sfn_lambda_alias]
}

module "create_run_workflow_sfn_lambda_alias" {
  source = "./create_lambda_alias"
  lambdas = {
    (local.ingest_upsert_archive_folders_lambda_name) = module.dr2_ingest_upsert_archive_folders_lambda.lambda_function.version
    (local.ingest_start_workflow_lambda_name)         = module.dr2_ingest_start_workflow_lambda.lambda_function.version
    (local.ingest_workflow_monitor_lambda_name)       = module.dr2_ingest_workflow_monitor_lambda.lambda_function.version
  }
  alias_name = local.lambda_alias_name
}


module "dr2_ingest_run_workflow_step_function" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//sfn"
  step_function_definition = templatefile("${path.module}/templates/sfn/ingest_run_workflow_sfn_definition.json.tpl", {
    step_function_name                        = local.ingest_run_workflow_step_function_name
    account_id                                = data.aws_caller_identity.current.account_id
    ingest_upsert_archive_folders_lambda_name = local.ingest_upsert_archive_folders_lambda_name
    ingest_start_workflow_lambda_name         = local.ingest_start_workflow_lambda_name
    ingest_workflow_monitor_lambda_name       = local.ingest_workflow_monitor_lambda_name
    retry_statement                           = local.retry_statement,
    upsert_lambda_retry_statement             = jsonencode([{ ErrorEquals = ["States.ALL"], IntervalSeconds = module.dr2_ingest_upsert_archive_folders_lambda.lambda_function.timeout, MaxAttempts = 10, BackoffRate = 1, JitterStrategy = "FULL" }])
    alias_name                                = local.lambda_alias_name
  })
  step_function_name = local.ingest_run_workflow_step_function_name
  step_function_role_policy_attachments = {
    step_function_policy = module.dr2_ingest_run_workflow_step_function_policy.policy_arn
  }
  depends_on = [module.create_run_workflow_sfn_lambda_alias]
}

module "ingest_state_bucket" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//s3"
  bucket_name = local.ingest_state_bucket_name
  bucket_policy = templatefile("./templates/s3/lambda_access_bucket_policy.json.tpl", {
    lambda_role_arns = jsonencode([module.dr2_ingest_mapper_lambda.lambda_role_arn]),
    bucket_name      = local.ingest_state_bucket_name
  })
  kms_key_arn     = module.dr2_developer_key.kms_key_arn
  lifecycle_rules = local.lifecycle_rules
}

module "dr2_ingest_step_function_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-ingest-step-function-policy"
  policy_string = templatefile("${path.module}/templates/iam_policy/ingest_step_function_policy.json.tpl", {
    account_id                                        = data.aws_caller_identity.current.account_id
    ingest_validate_generic_ingest_inputs_lambda_name = local.ingest_validate_generic_ingest_inputs_lambda_name
    ingest_mapper_lambda_name                         = local.ingest_mapper_lambda_name
    ingest_upsert_archive_folders_lambda_name         = local.ingest_upsert_archive_folders_lambda_name
    ingest_find_existing_asset_lambda_name            = local.ingest_find_existing_asset_name
    ingest_asset_opex_creator_lambda_name             = local.ingest_asset_opex_creator_lambda_name
    ingest_folder_opex_creator_lambda_name            = local.ingest_folder_opex_creator_lambda_name
    ingest_parent_folder_opex_creator_lambda_name     = local.ingest_parent_folder_opex_creator_lambda_name
    ingest_start_workflow_lambda_name                 = local.ingest_start_workflow_lambda_name
    ingest_workflow_monitor_lambda_name               = local.ingest_workflow_monitor_lambda_name
    ingest_asset_reconciler_lambda_name               = local.ingest_asset_reconciler_lambda_name
    ingest_flow_control_lambda_name                   = local.ingest_flow_control_lambda_name
    ingest_lock_table_name                            = local.ingest_lock_dynamo_table_name
    ingest_lock_table_group_id_gsi_name               = local.ingest_lock_table_group_id_gsi_name
    notifications_topic_name                          = local.notifications_topic_name
    ingest_queue_table_name                           = local.ingest_queue_dynamo_table_name
    ingest_state_bucket_name                          = local.ingest_state_bucket_name
    ingest_raw_cache_bucket_name                      = local.ingest_raw_cache_bucket_name
    ingest_sfn_name                                   = local.ingest_step_function_name
    ingest_run_workflow_sfn_name                      = local.ingest_run_workflow_step_function_name
    tna_to_preservica_role_arn                        = local.tna_to_preservica_role_arn
    preingest_tdr_step_function_arn                   = module.tdr_preingest.preingest_sfn_arn
    preingest_dri_step_function_arn                   = module.dri_preingest.preingest_sfn_arn
    preingest_adhoc_step_function_arn                 = module.ad_hoc_preingest.preingest_sfn_arn
    preingest_restore_step_function_arn               = module.cc_restore_preingest.preingest_sfn_arn
    preingest_court_document_step_function_arn        = module.court_document_preingest.preingest_sfn_arn
    ingest_run_workflow_sfn_arn                       = local.ingest_run_workflow_sfn_arn
    postingest_table_name                             = module.postingest.postingest_table_name
  })
}

module "dr2_ingest_run_workflow_step_function_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "${local.environment}-dr2-ingest-run-workflow-step-function-policy"
  policy_string = templatefile("${path.module}/templates/iam_policy/ingest_run_workflow_step_function_policy.json.tpl", {
    account_id                                = data.aws_caller_identity.current.account_id
    ingest_upsert_archive_folders_lambda_name = local.ingest_upsert_archive_folders_lambda_name
    ingest_start_workflow_lambda_name         = local.ingest_start_workflow_lambda_name
    ingest_workflow_monitor_lambda_name       = local.ingest_workflow_monitor_lambda_name,
    ingest_step_function_name                 = local.ingest_step_function_name
  })
}

module "files_table" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key                       = { name = "id", type = "S" }
  range_key                      = { name = "batchId", type = "S" }
  table_name                     = local.files_dynamo_table_name
  server_side_encryption_enabled = true
  kms_key_arn                    = module.dr2_kms_key.kms_key_arn
  ttl_attribute_name             = "ttl"
  stream_enabled                 = true
  stream_view_type               = "NEW_IMAGE"
  deletion_protection_enabled    = true
  additional_attributes = [
    { name = "batchId", type = "S" },
    { name = "parentPath", type = "S" }
  ]
  global_secondary_indexes = [
    {
      name            = local.files_table_batch_parent_global_secondary_index_name
      hash_key        = "batchId"
      range_key       = "parentPath"
      projection_type = "ALL"
    }
  ]
  point_in_time_recovery_enabled = local.enable_point_in_time_recovery
}

module "ingest_lock_table" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key                       = { name = local.ingest_lock_table_hash_key, type = "S" }
  table_name                     = local.ingest_lock_dynamo_table_name
  server_side_encryption_enabled = false
  additional_attributes = [
    { name = "groupId", type = "S" }
  ]
  global_secondary_indexes = [
    {
      name            = local.ingest_lock_table_group_id_gsi_name
      hash_key        = "groupId"
      projection_type = "ALL"
    }
  ]
  point_in_time_recovery_enabled = local.enable_point_in_time_recovery
}

module "ingest_queue_table" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key                       = { name = "sourceSystem", type = "S" }
  range_key                      = { name = "queuedAt", type = "S" }
  table_name                     = local.ingest_queue_dynamo_table_name
  server_side_encryption_enabled = false
  deletion_protection_enabled    = true
  point_in_time_recovery_enabled = local.enable_point_in_time_recovery
}

data "aws_ssm_parameter" "slack_token" {
  name            = "/mgmt/slack/token"
  with_decryption = true
}

resource "aws_ssm_parameter" "flow_control_config" {
  name  = "/${local.environment}/flow-control-config"
  type  = "String"
  value = jsonencode(local.selected_flow_control_config)

  lifecycle {
    precondition {
      condition     = sum([for i in local.selected_flow_control_config.sourceSystems : i.probability]) == 100
      error_message = "The sum of probabilities for source systems must equal 100."
    }

    precondition {
      condition     = sum([for i in local.selected_flow_control_config.sourceSystems : i.reservedChannels]) <= local.selected_flow_control_config.maxConcurrency
      error_message = "The sum of reserved channels for source systems must not exceed the maximum concurrency."
    }
  }
}

module "eventbridge_alarm_notifications_destination" {
  source                     = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination"
  authorisation_header_value = "Bearer ${data.aws_ssm_parameter.slack_token.value}"
  name                       = "${local.environment}-dr2-eventbridge-slack-destination"
}

module "cloudwatch_event_alarm_event_bridge_rule_alarm_only_for_ingest_queues" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.module}/templates/eventbridge/cloudwatch_alarm_event_pattern.json.tpl", {
    cloudwatch_alarms = jsonencode(
      flatten([
        [for queue in local.ingest_queues : queue.event_alarms],
        module.postingest.postingest_queue_oldest_message_alarm_arns
      ])
    ),
    state_value = "ALARM"
  })
  name                = "${local.environment}-dr2-eventbridge-ingest-queue-alarm-only"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "alarmName"    = "$.detail.alarmName",
      "currentValue" = "$.detail.state.value"
    }
    input_template = templatefile("${path.module}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.general_notifications_channel_id
      slackMessage = ":warning: Cloudwatch alarm <alarmName> has entered state <currentValue>"
    })
  }
}

module "cloudwatch_alarm_event_bridge_rule_for_unprocessed_messages" {
  for_each = toset(["OK", "ALARM"])
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.module}/templates/eventbridge/cloudwatch_alarm_event_pattern.json.tpl", {
    cloudwatch_alarms = jsonencode(flatten([for queue in flatten([local.custodial_copy_queues, values(module.postingest.postingest_queues)]) : queue.unprocessed_message_alarm]))
    state_value       = each.value
  })
  name                = "${local.environment}-dr2-eventbridge-unprocessed-messages-${lower(each.value)}"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "alarmName"    = "$.detail.alarmName",
      "currentValue" = "$.detail.state.value"
    }
    input_template = templatefile("${path.module}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.general_notifications_channel_id
      slackMessage = ":${each.value == "OK" ? "green-tick" : "alert-noflash-slow"}: Cloudwatch alarm <alarmName> has entered state <currentValue>"
    })
  }
}

module "cloudwatch_alarm_event_bridge_rule_for_ingest_queues" {
  for_each = toset(["OK", "ALARM"])
  source   = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.module}/templates/eventbridge/cloudwatch_alarm_event_pattern.json.tpl", {
    cloudwatch_alarms = jsonencode(flatten([for queue in local.ingest_queues : queue.alarms]))
    state_value       = each.value
  })
  name                = "${local.environment}-dr2-eventbridge-ingest-queue-${lower(each.value)}"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "alarmName"    = "$.detail.alarmName",
      "currentValue" = "$.detail.state.value"
    }
    input_template = templatefile("${path.module}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.general_notifications_channel_id
      slackMessage = ":${each.value == "OK" ? "green-tick" : "alert-noflash-slow"}: Cloudwatch alarm <alarmName> has entered state <currentValue>"
    })
  }
}

module "failed_ingest_step_function_event_bridge_rule" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.module}/templates/eventbridge/step_function_failed_event_pattern.json.tpl", {
    step_function_arns = jsonencode([
      module.dr2_ingest_step_function.step_function_arn,
      module.tdr_preingest.preingest_sfn_arn,
      module.dri_preingest.preingest_sfn_arn,
      module.ad_hoc_preingest.preingest_sfn_arn,
      module.dr2_ingest_run_workflow_step_function.step_function_arn
    ])
  })
  name                = "${local.environment}-dr2-eventbridge-ingest-step-function-failure"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "name"   = "$.detail.name",
      "status" = "$.detail.status",
      "sfnArn" = "$.detail.stateMachineArn"
    }
    input_template = templatefile("${path.module}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.general_notifications_channel_id
      slackMessage = ":warning: Step function `<sfnArn>` with name <name> has <status>"
    })
  }
  log_group_destination_input_transformer = {
    log_group_name = local.step_function_failure_log_group
    input_paths = {
      "name"      = "$.detail.name",
      "status"    = "$.detail.status",
      "startDate" = "$.detail.startDate",
      "sfnArn"    = "$.detail.stateMachineArn"
    }
    input_template = templatefile("${path.module}/templates/eventbridge/cloudwatch_message_input_template.json.tpl", {
      message = "Step function `<sfnArn>` with name <name> has <status>"
    })
  }
  lambda_target_arn = "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${local.ingest_failure_notifications_lambda_name}"
}

module "guard_duty_findings_eventbridge_rule" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.module}/templates/eventbridge/source_detail_type_event_pattern.json.tpl", {
    source = "aws.guardduty", detail_type = "GuardDuty Finding"
  })
  name                = "${local.environment}-dr2-guard-duty-notify"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "account" : "$.account",
      "id" : "$.detail.id",
      "region" : "$.region",
      "title" : "$.detail.title"
    }
    input_template = templatefile("${path.module}/templates/eventbridge/guard_duty_slack_message.json.tpl", {
      channel_id = local.general_notifications_channel_id
    })
  }
}

module "secret_rotation_eventbridge_rule" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  event_pattern = templatefile("${path.module}/templates/eventbridge/secrets_manager_rotation.json.tpl", {
    rotation_event = "RotationFailed"
  })
  name                = "${local.environment}-dr2-failed-secrets-manager-rotation"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  api_destination_input_transformer = {
    input_paths = {
      "secretId" : "$.detail.additionalEventData.SecretId"
    }
    input_template = templatefile("${path.module}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.general_notifications_channel_id
      slackMessage = ":alert-noflash-slow: Secret rotation for secret `<secretId>` has failed"
    })
  }
}

module "dev_slack_message_eventbridge_rule" {
  source              = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  event_pattern       = templatefile("${path.module}/templates/eventbridge/custom_detail_type_event_pattern.json.tpl", { detail_type = "DR2DevMessage" })
  name                = "${local.environment}-dr2-eventbridge-dev-slack-message"
  api_destination_input_transformer = {
    input_paths = {
      "slackMessage" = "$.detail.slackMessage"
    }
    input_template = templatefile("${path.module}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.dev_notifications_channel_id
      slackMessage = "<slackMessage>"
    })
  }
}

module "general_slack_message_eventbridge_rule" {
  source              = "git::https://github.com/nationalarchives/da-terraform-modules//eventbridge_api_destination_rule"
  api_destination_arn = module.eventbridge_alarm_notifications_destination.api_destination_arn
  event_pattern       = templatefile("${path.module}/templates/eventbridge/custom_detail_type_event_pattern.json.tpl", { detail_type = "DR2Message" })
  name                = "${local.environment}-dr2-eventbridge-general-slack-message"
  api_destination_input_transformer = {
    input_paths = {
      "slackMessage" = "$.detail.slackMessage"
    }
    input_template = templatefile("${path.module}/templates/eventbridge/slack_message_input_template.json.tpl", {
      channel_id   = local.general_notifications_channel_id
      slackMessage = "<slackMessage>"
    })
  }
}

resource "aws_cloudwatch_log_resource_policy" "eventbridge_resource_policy" {
  policy_document = templatefile("${path.module}/templates/logs/logs_resource_policy.json.tpl", { account_id = data.aws_caller_identity.current.account_id })
  policy_name     = "${local.environment}-dr2-trust-events-to-store-log-events"
}

resource "aws_cloudwatch_dashboard" "ingest_dashboard" {
  dashboard_body = templatefile("${path.module}/templates/logs/ingest_dashboard.json.tpl", {
    account_id                      = data.aws_caller_identity.current.account_id,
    environment                     = local.environment,
    step_function_failure_log_group = local.step_function_failure_log_group
    source_list                     = join(" | ", [for lambda in local.dashboard_lambdas : format("SOURCE '/aws/lambda/%s'", lambda)])
  })
  dashboard_name = "${local.environment}-dr2-ingest-dashboard"
}

module "interface_endpoints_security_group" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//security_group"
  common_tags = {}
  description = "A security group for interface type vpc endpoints"
  name        = "${local.environment}-vpc-endpoints"
  vpc_id      = module.vpc.vpc.id
  rules = {
    ingress = [
      {
        port              = 443,
        description       = "Allow inbound https traffic to services from internal components",
        security_group_id = module.https_to_vpc_endpoints_security_group.security_group_id
      }
    ]
  }
}

module "https_to_vpc_endpoints_security_group" {
  source      = "git::https://github.com/nationalarchives/da-terraform-modules//security_group"
  common_tags = {}
  description = "A security group for outbound https to vpc endpoints"
  name        = "${local.environment}-outbound-https-to-vpc-endpoints"
  vpc_id      = module.vpc.vpc.id
  rules = {
    egress = [
      {
        port              = 443,
        description       = "Allow outbound https traffic to services through interface endpoints",
        security_group_id = module.interface_endpoints_security_group.security_group_id
      }
    ]
  }
}

module "archivist_sso_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy"
  name   = "AWSSSO_DAArchivist"
  policy_string = templatefile("${path.module}/templates/iam_policy/archivist_sso_policy.json.tpl", {
    account_id  = data.aws_caller_identity.current.account_id
    environment = local.environment
  })
}
