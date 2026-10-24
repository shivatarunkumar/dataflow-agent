variable "project_id" {
  description = "GCP project to deploy into."
  type        = string
}

variable "region" {
  description = "Region for all regional resources."
  type        = string
  default     = "europe-west2"
}

variable "prefix" {
  description = "Prefix for resource names. Bucket names are <project_id>-<prefix>-*."
  type        = string
  default     = "dfa"
}

variable "audit_dataset" {
  description = "Dataset holding job_metadata and validation_metadata."
  type        = string
  default     = "file_audit"
}

variable "target_dataset" {
  description = "Dataset holding the target tables (customer, user, orders)."
  type        = string
  default     = "raw"
}

variable "agent_operator" {
  description = "IAM member allowed to impersonate the df-agent SA (e.g. user:you@example.com). Empty = nobody."
  type        = string
  default     = ""
}

variable "max_concurrent_jobs" {
  description = "Max Dataflow jobs running at once. Each job uses ~2 external IPs (launcher + 1 worker); the default quota is 4."
  type        = number
  default     = 1
}

variable "target_tables" {
  description = "Target tables to create; each needs infra/bq/<name>.json."
  type        = list(string)
  default     = ["customer", "user", "orders"]
}
