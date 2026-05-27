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
