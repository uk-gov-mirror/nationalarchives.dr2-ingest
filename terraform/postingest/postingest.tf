locals {
  postingest_state_table_name                = "${var.environment}-dr2-postingest-state"
  postingest_gsi_firstqueued_name            = "QueueFirstQueuedIdx"
  postingest_gsi_lastqueued_name             = "QueueLastQueuedIdx"
  send_to_state_change_ddb_queue_lambda_name = "${var.environment}-dr2-postingest-state-change-queue-sender"
  state_change_ddb_queue_name                = "${var.environment}-dr2-postingest-state-change-handler"
  state_change_lambda_key                    = "postingest-state-change-handler"
  state_change_lambda_name                   = "${var.environment}-dr2-${local.state_change_lambda_key}"
  resender_lambda_key                        = "postingest-message-resender"
  resender_lambda_name                       = "${var.environment}-dr2-${local.resender_lambda_key}"
  java_runtime                               = "java21"
  architecture_arm64                         = "arm64"
  java_lambda_memory_size                    = 512
  python_timeout_seconds                     = 30
  python_runtime                             = "python3.14"
  python_lambda_memory_size                  = 128
  postingest_queue_config = [ // Before adding a new queue here, update the state change handler to expect it
    { "queueAlias" : "CC", "queueOrder" : 1, "queue_name" : "${var.environment}-dr2-postingest-custodial-copy-confirmer" },
    { "queueAlias" : "TC", "queueOrder" : 2, "queue_name" : "${var.environment}-dr2-postingest-custodial-copy-tape-confirmer" }
  ]
  six_hours                  = 60 * 60 * 6
  seven_days                 = 60 * 60 * 24 * 7
  messages_visible_threshold = 1000000
  code_deploy_bucket         = var.code_deploy_bucket
  # Redefining the postingest_queue_env_var here to avoid issues with scala type system
  postingest_queue_env_var = [for queue in local.postingest_queue_config :
    {
      "queueAlias" : queue.queueAlias,
      "queueOrder" : queue.queueOrder,
      "queueUrl" : module.dr2_confirmer_queues[queue.queueAlias].sqs_queue.url
    }
  ]
}

data "aws_caller_identity" "current" {}

module "postingest_state_table" {
  source                         = "git::https://github.com/nationalarchives/da-terraform-modules//dynamo"
  hash_key                       = { name = "assetId", type = "S" }
  range_key                      = { name = "batchId", type = "S" }
  table_name                     = local.postingest_state_table_name
  server_side_encryption_enabled = false
  ttl_attribute_name             = "ttl"
  stream_enabled                 = true
  stream_view_type               = "NEW_AND_OLD_IMAGES"
  deletion_protection_enabled    = true
  additional_attributes = [
    { name = "queue", type = "S" },
    { name = "lastQueued", type = "S" },
    { name = "firstQueued", type = "S" }
  ]
  global_secondary_indexes = [
    {
      name            = local.postingest_gsi_lastqueued_name
      hash_key        = "queue"
      range_key       = "lastQueued"
      projection_type = "ALL"
    },
    {
      name            = local.postingest_gsi_firstqueued_name
      hash_key        = "queue"
      range_key       = "firstQueued"
      projection_type = "ALL"
    }
  ]
  point_in_time_recovery_enabled = true
}

module "dr2_confirmer_queues" {
  source     = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  for_each   = { for queue in local.postingest_queue_config : queue.queueAlias => queue }
  queue_name = each.value.queue_name
  sqs_policy = templatefile("./templates/sqs/sqs_access_policy.json.tpl", {
    account_id = data.aws_caller_identity.current.account_id,
    queue_name = each.value.queue_name
  })
  create_dlq                                        = false
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  visibility_timeout                                = 600
  encryption_type                                   = "sse"
  delay_seconds                                     = 900
}

module "confirmer_message_older_than_one_week_alarm" {
  source              = "git::https://github.com/nationalarchives/da-terraform-modules//cloudwatch_alarms"
  for_each            = module.dr2_confirmer_queues
  name                = "${each.value.sqs_queue.name}-messages-older-than-one-week-alarm"
  comparison_operator = "GreaterThanThreshold"
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  statistic           = "Maximum"
  treat_missing_data  = "ignore"
  datapoints_to_alarm = 1
  dimensions = {
    QueueName = each.value.sqs_queue.name
  }
  period    = local.six_hours
  threshold = local.seven_days
}

module "dr2_state_change_ddb_queue" {
  source                                            = "git::https://github.com/nationalarchives/da-terraform-modules//sqs"
  queue_name                                        = local.state_change_ddb_queue_name
  sqs_policy                                        = ""
  create_dlq                                        = true
  queue_cloudwatch_alarm_visible_messages_threshold = local.messages_visible_threshold
  visibility_timeout                                = 180
  encryption_type                                   = "sse"
}

module "dr2_send_to_state_change_ddb_queue_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  description     = "A lambda function to pass on a DynamoDB Stream event to an SQS queue"
  function_name   = local.send_to_state_change_ddb_queue_lambda_name
  handler         = "send_to_state_change_ddb_queue.lambda_handler"
  timeout_seconds = local.python_timeout_seconds
  runtime         = local.python_runtime
  memory_size     = local.python_lambda_memory_size
  dynamo_stream_config = {
    stream_arn             = module.postingest_state_table.stream_arn
    dead_letter_target_arn = module.dr2_state_change_ddb_queue.dlq_sqs_arn
  }

  policies = {
    "${local.send_to_state_change_ddb_queue_lambda_name}-policy" = templatefile("./templates/iam_policy/send_to_state_change_ddb_queue.json.tpl", {
      state_change_handler_queue_arn  = module.dr2_state_change_ddb_queue.sqs_arn
      dead_letter_target_arn          = module.dr2_state_change_ddb_queue.dlq_sqs_arn
      dynamo_db_postingest_stream_arn = module.postingest_state_table.stream_arn
      account_id                      = data.aws_caller_identity.current.account_id
      lambda_name                     = local.send_to_state_change_ddb_queue_lambda_name
    })
  }

  plaintext_env_vars = {
    QUEUE_URL = module.dr2_state_change_ddb_queue.sqs_queue_url
  }

  tags = {}
}

module "dr2_state_change_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.state_change_lambda_name
  handler         = "uk.gov.nationalarchives.postingeststatechangehandler.Lambda::handleRequest"
  timeout_seconds = 60

  policies = {
    "${local.state_change_lambda_name}-policy" = templatefile("${path.module}/templates/policies/state_change_lambda_policy.json.tpl", {
      queue_arns                     = jsonencode([for v in module.dr2_confirmer_queues : v.sqs_arn])
      dynamo_db_postingest_arn       = module.postingest_state_table.table_arn
      sns_external_notifications_arn = var.notifications_topic_arn
      account_id                     = data.aws_caller_identity.current.account_id
      lambda_name                    = local.state_change_lambda_name
      ddb_queue_arn                  = module.dr2_state_change_ddb_queue.sqs_arn
      vpc_id                         = var.vpc_id
    })
  }
  s3_bucket                      = local.code_deploy_bucket
  s3_key                         = "${var.lambda_code_version}/${local.state_change_lambda_key}"
  memory_size                    = local.java_lambda_memory_size
  runtime                        = local.java_runtime
  architecture                   = local.architecture_arm64
  sqs_queue_mapping_batch_size   = 10
  sqs_report_batch_item_failures = true
  lambda_sqs_queue_mappings = [{
    sqs_queue_arn         = "arn:aws:sqs:eu-west-2:${data.aws_caller_identity.current.account_id}:${local.state_change_ddb_queue_name}"
    ignore_enabled_status = true
  }]

  vpc_config = {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.private_security_group_ids
  }
  plaintext_env_vars = {
    POSTINGEST_STATE_DDB_TABLE                = local.postingest_state_table_name
    POSTINGEST_DDB_TABLE_LAST_QUEUED_GSI_NAME = local.postingest_gsi_lastqueued_name
    OUTPUT_TOPIC_ARN                          = var.notifications_topic_arn
    POSTINGEST_QUEUES                         = jsonencode(local.postingest_queue_env_var)
  }
  tags = {
    Name = local.state_change_lambda_name
  }
}

module "dr2_message_resender_lambda" {
  source          = "git::https://github.com/nationalarchives/da-terraform-modules//lambda"
  function_name   = local.resender_lambda_name
  handler         = "uk.gov.nationalarchives.postingestresender.Lambda::handleRequest"
  timeout_seconds = 900

  policies = {
    "${local.resender_lambda_name}-policy" = templatefile("${path.module}/templates/policies/message_resender_lambda_policy.json.tpl", {
      queue_arns           = jsonencode(values(module.dr2_confirmer_queues)[*].sqs_arn)
      postingest_state_arn = module.postingest_state_table.table_arn
      account_id           = data.aws_caller_identity.current.account_id
      lambda_name          = local.resender_lambda_name
      gsi_name             = local.postingest_gsi_lastqueued_name
      vpc_id               = var.vpc_id
    })
  }
  s3_bucket = local.code_deploy_bucket
  s3_key    = "${var.lambda_code_version}/${local.resender_lambda_key}"
  lambda_invoke_permissions = {
    "events.amazonaws.com" = module.dr2_message_resender_cloudwatch_event.event_arn
  }
  memory_size  = local.java_lambda_memory_size
  runtime      = local.java_runtime
  architecture = local.architecture_arm64
  plaintext_env_vars = {
    POSTINGEST_STATE_DDB_TABLE                = local.postingest_state_table_name
    POSTINGEST_DDB_TABLE_LAST_QUEUED_GSI_NAME = local.postingest_gsi_lastqueued_name
    POSTINGEST_QUEUES                         = jsonencode(local.postingest_queue_env_var)
  }
  vpc_config = {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.private_security_group_ids
  }
  tags = {
    Name = local.resender_lambda_name
  }
}

module "dr2_message_resender_cloudwatch_event" {
  source                  = "git::https://github.com/nationalarchives/da-terraform-modules//cloudwatch_events"
  rule_name               = "${var.environment}-dr2-postingest-resender-schedule"
  schedule                = "rate(1 hour)"
  lambda_event_target_arn = "arn:aws:lambda:eu-west-2:${data.aws_caller_identity.current.account_id}:function:${local.resender_lambda_name}"
}
