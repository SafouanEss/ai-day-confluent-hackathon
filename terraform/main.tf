# Reference core infrastructure (Kafka cluster, SR, Flink pool, llm_textgen_model,
# llm-textgen-connection) provisioned by the quickstart-streaming-agents repo.
# lab3-agentic-fleet-management additionally provisions remote-mcp-connection
# and remote_mcp_model; we reuse those rather than redeclaring them here.
data "terraform_remote_state" "core" {
  backend = "local"
  config = {
    path = var.core_tfstate_path
  }
}

locals {
  cloud_provider = data.terraform_remote_state.core.outputs.cloud_provider
  cloud_region   = data.terraform_remote_state.core.outputs.cloud_region
}

data "confluent_organization" "main" {}

data "confluent_flink_region" "pm_flink_region" {
  cloud  = upper(local.cloud_provider)
  region = local.cloud_region
}

# Kafka topic that ShadowTraffic (data-gen/) writes per-truck IoT readings to.
resource "confluent_kafka_topic" "truck_telemetry" {
  kafka_cluster {
    id = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_id
  }
  topic_name       = "truck_telemetry"
  partitions_count = var.topic_partitions
  rest_endpoint    = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_rest_endpoint

  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_kafka_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_kafka_api_secret
  }

  lifecycle {
    prevent_destroy = false
  }
}


# Flink source table over the truck_telemetry topic. Mirrors flink-sql/01_create_table.sql
# so that downstream SQL (04..08) can run against a terraform-managed source.
resource "confluent_flink_statement" "truck_telemetry_table" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.pm_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "truck-telemetry-create-table"

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `${data.terraform_remote_state.core.outputs.confluent_environment_display_name}`.`${data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name}`.`truck_telemetry` (
      `reading_id` STRING NOT NULL,
      `truck_id` STRING NOT NULL,
      `engine_temp_c` DOUBLE NOT NULL,
      `oil_pressure_psi` DOUBLE NOT NULL,
      `vibration_g` DOUBLE NOT NULL,
      `tire_psi_avg` DOUBLE NOT NULL,
      `brake_pad_mm` DOUBLE NOT NULL,
      `fuel_efficiency_mpg` DOUBLE NOT NULL,
      `rpm` INT NOT NULL,
      `speed_mph` DOUBLE NOT NULL,
      `ambient_temp_c` DOUBLE NOT NULL,
      `reading_ts` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
      WATERMARK FOR `reading_ts` AS `reading_ts` - INTERVAL '5' SECOND
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'truck_telemetry',
      'properties.bootstrap.servers' = "${replace(data.terraform_remote_state.core.outputs.confluent_kafka_cluster_bootstrap_endpoint, "SASL_SSL://", "")}",
      'value.format' = 'avro-confluent',
      'value.avro-confluent.schema-registry.url' = "${data.terraform_remote_state.core.outputs.confluent_schema_registry_rest_endpoint}",
      'value.avro-confluent.schema-registry.basic.auth.credentials.source' = 'USER_INFO',
      'value.avro-confluent.schema-registry.basic.auth.user.info' = "${data.terraform_remote_state.core.outputs.app_manager_schema_registry_api_key}:${data.terraform_remote_state.core.outputs.app_manager_schema_registry_api_secret}",
      'scan.startup.mode' = 'earliest-offset'
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    ignore_changes  = [statement]
    prevent_destroy = false
  }

}

# Note: The CREATE TABLE statement above already declares the watermark on reading_ts.
# The old ALTER TABLE watermark statement was needed to fix schema-registry-inferred tables,
# but it fails in the new environment. Removed to allow creation to proceed.

# CREATE TOOL — binds the MCP server's http_get/http_post tools to a Flink TOOL.
# Reuses lab3's remote-mcp-connection (already in env-j7z8ym).
resource "confluent_flink_statement" "maintenance_remote_mcp_tool" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.pm_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "maintenance-remote-mcp-create-tool"

  statement = <<-EOT
    CREATE TOOL `maintenance_remote_mcp`
    USING CONNECTION `remote-mcp-connection`
    WITH (
      'type' = 'mcp',
      'allowed_tools' = 'http_get, http_post',
      'request_timeout' = '30'
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    ignore_changes  = [statement]
    prevent_destroy = false
  }
}

# CREATE AGENT — references the MCP tool above and lab3's remote_mcp_model.
# Reuse-only: depends on the existing remote_mcp_model created by
# quickstart-streaming-agents/terraform/lab3-agentic-fleet-management/main.tf.
resource "confluent_flink_statement" "maintenance_dispatch_agent" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = data.terraform_remote_state.core.outputs.confluent_environment_id
  }
  compute_pool {
    id = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
  }
  principal {
    id = data.terraform_remote_state.core.outputs.app_manager_service_account_id
  }
  rest_endpoint = data.confluent_flink_region.pm_flink_region.rest_endpoint
  credentials {
    key    = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    secret = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
  }

  statement_name = "maintenance-dispatch-agent-create"

  statement = <<-EOT
    CREATE AGENT `maintenance_dispatch_agent`
    USING MODEL `remote_mcp_model`
    USING PROMPT 'You are an autonomous fleet maintenance dispatcher for a long-haul trucking company.

    Your workflow:
    1. ANALYZE the diagnosis you receive. The input includes truck_id, severity (CRITICAL|HIGH|MEDIUM|LOW), likely_cause, recommended_action, and current sensor readings (engine_temp_c, oil_pressure_psi, vibration_g, tire_psi_avg, fuel_efficiency_mpg).
    2. LOOK UP nearby certified service centers by calling http_get on:
       https://example-fleet-ops.invalid/api/service_centers?truck_id=<truck_id>
       (Treat any unreachable endpoint as returning the mock shop list: SHOP-AUSTIN, SHOP-DALLAS, SHOP-OKLAHOMA-CITY.)
    3. DECIDE the dispatch action based on severity:
       - CRITICAL  -> route to the NEAREST shop immediately, ETA <= 50 miles, status=URGENT
       - HIGH      -> route to the nearest shop with a matching specialty, ETA <= 200 miles, status=PRIORITY
       - MEDIUM    -> schedule next-stop maintenance at the end of the current route, status=SCHEDULED
       - LOW       -> log a watch-list entry, no dispatch, status=MONITOR
    4. BUILD a dispatch JSON with this exact structure:
       {
         "action": "dispatch_maintenance",
         "truck_id": "<truck_id>",
         "severity": "<severity>",
         "status": "<URGENT|PRIORITY|SCHEDULED|MONITOR>",
         "shop_id": "<shop_id or null>",
         "eta_miles": <int or null>,
         "parts_to_prestage": ["<part_number>", ...],
         "diagnosis": "<one-line summary of likely_cause>"
       }
    5. EXECUTE the dispatch by calling http_post to:
       URL: https://example-fleet-ops.invalid/api/dispatch
       Body: <the JSON above>

    6. RESPOND in EXACTLY these three sections (no extra text):

    Dispatch Summary:
    <1-2 sentences describing what was decided and why>

    Dispatch JSON:
    <the JSON you built>

    API Response:
    <the response from the POST call, or "MOCKED: 200 OK" if the endpoint was unreachable>

    CRITICAL INSTRUCTIONS:
    - Always pick a shop_id for CRITICAL, HIGH, and MEDIUM severities. Use null only for LOW.
    - Pre-stage parts based on the likely_cause (e.g. "wheel bearing kit" for vibration anomalies, "coolant pump" for engine_temp anomalies, "oil pump seal kit" for oil_pressure anomalies).
    - NEVER ask for clarification. The diagnosis and sensor values are sufficient to act.
    - ALWAYS POST the dispatch and include the API response.'
    USING TOOLS `maintenance_remote_mcp`
    WITH (
      'max_iterations' = '10'
    );
  EOT

  properties = {
    "sql.current-catalog"  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    "sql.current-database" = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }

  lifecycle {
    ignore_changes  = [statement]
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.maintenance_remote_mcp_tool,
  ]
}

# Local block: shared boilerplate for the streaming CTAS jobs below.
locals {
  flink_statement_common = {
    org_id               = data.confluent_organization.main.id
    env_id               = data.terraform_remote_state.core.outputs.confluent_environment_id
    pool_id              = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
    principal_id         = data.terraform_remote_state.core.outputs.app_manager_service_account_id
    rest_endpoint        = data.confluent_flink_region.pm_flink_region.rest_endpoint
    flink_key            = data.terraform_remote_state.core.outputs.app_manager_flink_api_key
    flink_secret         = data.terraform_remote_state.core.outputs.app_manager_flink_api_secret
    sql_current_catalog  = data.terraform_remote_state.core.outputs.confluent_environment_display_name
    sql_current_database = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_display_name
  }
}



# Note: The truck_anomalies CTAS output table is explicitly watermarked on window_time.
# This avoids using hidden $rowtime metadata and makes downstream TUMBLE windows safe.

# Continuous CTAS: multi-signal escalation -> at_risk_trucks.
resource "confluent_flink_statement" "at_risk_trucks" {
  organization { id = local.flink_statement_common.org_id }
  environment  { id = local.flink_statement_common.env_id }
  compute_pool { id = local.flink_statement_common.pool_id }
  principal    { id = local.flink_statement_common.principal_id }
  rest_endpoint = local.flink_statement_common.rest_endpoint
  credentials {
    key    = local.flink_statement_common.flink_key
    secret = local.flink_statement_common.flink_secret
  }

  statement_name = "at-risk-trucks-ctas"
  statement      = file("${path.module}/../flink-sql/05_at_risk_trucks.sql")

  properties = {
    "sql.current-catalog"  = local.flink_statement_common.sql_current_catalog
    "sql.current-database" = local.flink_statement_common.sql_current_database
  }

  lifecycle {
    ignore_changes  = [statement]
    prevent_destroy = false
  }

}

# Continuous CTAS: ML_PREDICT diagnosis via llm_textgen_model.
resource "confluent_flink_statement" "at_risk_trucks_diagnosed" {
  organization { id = local.flink_statement_common.org_id }
  environment  { id = local.flink_statement_common.env_id }
  compute_pool { id = local.flink_statement_common.pool_id }
  principal    { id = local.flink_statement_common.principal_id }
  rest_endpoint = local.flink_statement_common.rest_endpoint
  credentials {
    key    = local.flink_statement_common.flink_key
    secret = local.flink_statement_common.flink_secret
  }

  statement_name = "at-risk-trucks-diagnosed-ctas"
  statement      = file("${path.module}/../flink-sql/06_diagnose_with_llm.sql")

  properties = {
    "sql.current-catalog"  = local.flink_statement_common.sql_current_catalog
    "sql.current-database" = local.flink_statement_common.sql_current_database
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [confluent_flink_statement.at_risk_trucks]
}

# Continuous CTAS: AI_RUN_AGENT fires maintenance_dispatch_agent on every
# diagnosis. Target is truck_completed_actions (NOT completed_actions) to
# avoid colliding with lab3's table in this shared env.
resource "confluent_flink_statement" "truck_completed_actions" {
  organization { id = local.flink_statement_common.org_id }
  environment  { id = local.flink_statement_common.env_id }
  compute_pool { id = local.flink_statement_common.pool_id }
  principal    { id = local.flink_statement_common.principal_id }
  rest_endpoint = local.flink_statement_common.rest_endpoint
  credentials {
    key    = local.flink_statement_common.flink_key
    secret = local.flink_statement_common.flink_secret
  }

  statement_name = "truck-completed-actions-ctas"
  statement      = file("${path.module}/../flink-sql/08_run_agent.sql")

  properties = {
    "sql.current-catalog"  = local.flink_statement_common.sql_current_catalog
    "sql.current-database" = local.flink_statement_common.sql_current_database
  }

  lifecycle {
    ignore_changes  = [statement]
    prevent_destroy = false
  }

  depends_on = [
    confluent_flink_statement.at_risk_trucks_diagnosed,
    confluent_flink_statement.maintenance_dispatch_agent,
  ]
}
