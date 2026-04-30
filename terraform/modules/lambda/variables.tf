variable "function_name" { type = string }
variable "handler" { type = string }
variable "lambda_role_arn" { type = string }
variable "lambda_role_name" { type = string }
variable "source_dir" { type = string }
variable "env_vars" { type = map(string) }

variable "layers" {
  type    = list(string)
  default = []
}

variable "memory_size" {
  type    = number
  default = 128
}

variable "timeout" {
  type    = number
  default = 30
}

variable "bronze_bucket_arn" {
  type    = string
  default = ""
}

variable "enable_schedule" {
  type    = bool
  default = false
}

variable "enable_alerts" {
  type    = bool
  default = false
}

variable "alert_email" {
  type        = string
  description = "Email address for SNS alerts"
  default     = "your-email@example.com"
}