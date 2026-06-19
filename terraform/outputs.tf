output "lab_name" {
  value = "predictive-maintenance-${local.cloud_provider}"
}

output "confluent_environment_id" {
  value = data.terraform_remote_state.core.outputs.confluent_environment_id
}

output "confluent_kafka_cluster_id" {
  value = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_id
}

output "confluent_kafka_bootstrap_endpoint" {
  value = data.terraform_remote_state.core.outputs.confluent_kafka_cluster_bootstrap_endpoint
}

output "confluent_schema_registry_id" {
  value = data.terraform_remote_state.core.outputs.confluent_schema_registry_id
}

output "confluent_schema_registry_endpoint" {
  value = data.terraform_remote_state.core.outputs.confluent_schema_registry_rest_endpoint
}

output "confluent_flink_compute_pool_id" {
  value = data.terraform_remote_state.core.outputs.confluent_flink_compute_pool_id
}

output "truck_telemetry_topic" {
  value       = confluent_kafka_topic.truck_telemetry.topic_name
  description = "Kafka topic ShadowTraffic publishes truck IoT readings to"
}

# Commented out because truck_telemetry_table resource is managed externally
# output "truck_telemetry_table_statement_id" {
#   value       = confluent_flink_statement.truck_telemetry_table.id
#   description = "Flink statement ID for the truck_telemetry source table"
# }

output "maintenance_remote_mcp_tool_statement_id" {
  value       = confluent_flink_statement.maintenance_remote_mcp_tool.id
  description = "Flink statement ID for CREATE TOOL maintenance_remote_mcp"
}

output "maintenance_dispatch_agent_statement_id" {
  value       = confluent_flink_statement.maintenance_dispatch_agent.id
  description = "Flink statement ID for CREATE AGENT maintenance_dispatch_agent"
}


output "at_risk_trucks_statement_id" {
  value       = confluent_flink_statement.at_risk_trucks.id
  description = "Flink statement ID for the at-risk escalation CTAS"
}

output "at_risk_trucks_diagnosed_statement_id" {
  value       = confluent_flink_statement.at_risk_trucks_diagnosed.id
  description = "Flink statement ID for the LLM diagnosis CTAS"
}

output "truck_completed_actions_statement_id" {
  value       = confluent_flink_statement.truck_completed_actions.id
  description = "Flink statement ID for the AI_RUN_AGENT CTAS (truck_completed_actions)"
}
