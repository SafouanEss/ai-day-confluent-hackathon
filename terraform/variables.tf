variable "core_tfstate_path" {
  description = "Absolute path to the core terraform.tfstate from the quickstart-streaming-agents repo"
  type        = string
  default     = "/Users/safouan.essebbar/quickstart-streaming-agents/terraform/core/terraform.tfstate"
}

variable "topic_partitions" {
  description = "Partition count for the truck_telemetry topic"
  type        = number
  default     = 6
}
