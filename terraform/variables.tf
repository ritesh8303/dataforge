variable "alert_email" {
  description = "Email address for CloudWatch alarm notifications"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "dashboard_origin" {
  description = "GitHub Pages origin allowed to call the Metrics Lambda (e.g. https://your-username.github.io)"
  type        = string
  default     = "*"
}

variable "company_careers_config_s3_uri" {
  description = "Optional S3 URI for the direct company-careers target registry JSON"
  type        = string
  default     = ""
}

variable "company_careers_config_mode" {
  description = "Use append to add registry targets to defaults, or replace to use only the registry"
  type        = string
  default     = "append"
}

variable "company_careers_config_url" {
  description = "Optional HTTP/HTTPS URL for the direct company-careers target registry JSON"
  type        = string
  default     = ""
}
