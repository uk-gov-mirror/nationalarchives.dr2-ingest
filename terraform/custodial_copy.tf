locals {
  custodial_copy_name                  = "${local.environment}-dr2-custodial-copy"
  custodial_copy_db_builder_queue_name = "${local.custodial_copy_name}-db-builder"
  count_metrics = {
    "PreservationSystemDownloadCount" = "$.icCacheNonHits",
    "IntelligentCacheDownloadCount"   = "$.icCacheHits"
  }
  size_metrics = {
    "PreservationSystemDownloadSize" = "$.psDownloadsInBytes"
    "IntelligentCacheDownloadSize"   = "$.icDownloadsInBytes"
  }
}

resource "aws_cloudwatch_log_metric_filter" "count_metrics" {
  for_each       = local.count_metrics
  name           = each.key
  pattern        = "{ ${each.value} = \"*\" }"
  log_group_name = "/custodial-copy-backend"

  metric_transformation {
    name      = each.key
    namespace = "CustodialCopy"
    value     = each.value
  }
}

resource "aws_cloudwatch_log_metric_filter" "size_metrics" {
  for_each       = local.size_metrics
  name           = each.key
  pattern        = "{ ${each.value} = \"*\" }"
  log_group_name = "/custodial-copy-backend"

  metric_transformation {
    name      = each.key
    namespace = "CustodialCopy"
    value     = each.value
    unit      = "Bytes"
  }
}

module "custodial_copy_user_policy" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_policy?ref=main"
  name   = local.custodial_copy_name
  policy_string = templatefile("${path.module}/templates/iam_policy/custodial_copy_policy.json.tpl", {
    account_id                     = data.aws_caller_identity.current.account_id
    secrets_manager_secret_arn     = aws_secretsmanager_secret.preservica_read_metadata_read_content.arn
    custodial_copy_queue           = module.dr2_custodial_copy_queue.sqs_arn
    custodial_copy_confirmer_queue = module.postingest.postingest_queues["CC"].sqs_queue.arn
    tape_copy_confirmer_queue      = module.postingest.postingest_queues["TC"].sqs_queue.arn
    postingest_table               = module.postingest.postingest_table_arn
    database_builder_queue         = module.dr2_custodial_copy_db_builder_queue.sqs_arn
    management_account_id          = module.config.account_numbers["mgmt"]
  })
}

module "custodial_copy_profile" {
  source = "git::https://github.com/nationalarchives/da-terraform-modules//iam_roles_anywhere?ref=main"
  roles = {
    "${local.custodial_copy_name}" = {
      x509_subject_cn    = data.aws_ssm_parameter.custodial_copy_x509_subject_cn.value
      policy_attachments = { "${local.custodial_copy_name}" = module.custodial_copy_user_policy.policy_arn }
      allowed_subnets    = jsondecode(data.aws_ssm_parameter.site_outbound_subnet.value)
    }
  }
}

module "dr2_custodial_copy_db_builder_queue" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.custodial_copy_db_builder_queue_name
  sqs_policy = templatefile("./templates/sqs/sns_send_message_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.custodial_copy_db_builder_queue_name
    topic_arn  = local.custodial_copy_topic_arn
  })
  visibility_timeout                                = 600
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  encryption_type                                   = local.sse_encryption
}

module "dr2_custodial_copy_queue" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name = local.custodial_copy_name
  fifo_queue = true
  sqs_policy = templatefile("./templates/sqs/sqs_access_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = local.custodial_copy_name
  })
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  encryption_type                                   = local.sse_encryption
  visibility_timeout                                = 3600
}
