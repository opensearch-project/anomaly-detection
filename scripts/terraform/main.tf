terraform {
  required_version = ">= 1.5.0"

  required_providers {
    opensearch = {
      source  = "opensearch-project/opensearch"
      version = "~> 2.3.2"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

############################
# Connection (localhost)
############################
variable "opensearch_url" {
  type    = string
  default = "http://localhost:9200"
}

# Leave these empty if your local cluster has security disabled.
variable "opensearch_username" {
  type    = string
  default = "" # e.g., "admin"
}

variable "opensearch_password" {
  type      = string
  default   = "" # e.g., "admin" or your configured password
  sensitive = true
}

provider "opensearch" {
  url      = var.opensearch_url
  username = var.opensearch_username
  password = var.opensearch_password
}

############################
# Detector config
############################
variable "detector_name" {
  type    = string
  default = "tf-detector"
}

variable "indices" {
  type    = list(string)
  default = ["server-metrics"] # <-- change to your index / pattern
}

variable "time_field" {
  type    = string
  default = "@timestamp" # <-- change to your time field
}

variable "feature_field" {
  type    = string
  default = "deny" # <-- change to your numeric field
}

variable "detection_interval_minutes" {
  type    = number
  default = 1
}

variable "window_delay_minutes" {
  type    = number
  default = 1
}

variable "result_index" {
  type    = string
  default = "opensearch-ad-plugin-result-tf"
}

locals {
  detector_body = {
    name        = var.detector_name
    description = "Detector created by Terraform"
    time_field  = var.time_field
    indices     = var.indices

    feature_attributes = [
      {
        feature_name    = "feature_1"
        feature_enabled = true
        aggregation_query = {
          feature_1 = {
            max = { field = var.feature_field }
          }
        }
      }
    ]

    detection_interval = {
      period = {
        interval = var.detection_interval_minutes
        unit     = "Minutes"
      }
    }

    window_delay = {
      period = {
        interval = var.window_delay_minutes
        unit     = "Minutes"
      }
    }

    result_index = var.result_index
  }

  detector_body_json = jsonencode(local.detector_body)
  detector_body_sha  = sha256(local.detector_body_json)
}

resource "opensearch_anomaly_detection" "detector" {
  body = local.detector_body_json
}

############################
# Start / stop the job
############################
resource "null_resource" "start_detector_job" {
  # Re-run if the detector is recreated OR its config changes.
  triggers = {
    detector_id       = opensearch_anomaly_detection.detector.id
    detector_body_sha = local.detector_body_sha
    opensearch_url    = var.opensearch_url
    opensearch_user   = var.opensearch_username
    opensearch_pass   = var.opensearch_password
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    environment = {
      OPENSEARCH_URL      = var.opensearch_url
      OPENSEARCH_USERNAME = var.opensearch_username
      OPENSEARCH_PASSWORD = var.opensearch_password
      DETECTOR_ID         = self.triggers.detector_id
    }

    command = <<EOT
set -eo pipefail

# Make apply idempotent across re-runs/config updates:
if [ -n "$OPENSEARCH_USERNAME" ] || [ -n "$OPENSEARCH_PASSWORD" ]; then
  curl -sS -XPOST "$OPENSEARCH_URL/_plugins/_anomaly_detection/detectors/$DETECTOR_ID/_stop" \
    -H 'Content-Type: application/json' \
    --user "$OPENSEARCH_USERNAME:$OPENSEARCH_PASSWORD" >/dev/null || true

  curl -sSf -XPOST "$OPENSEARCH_URL/_plugins/_anomaly_detection/detectors/$DETECTOR_ID/_start" \
    -H 'Content-Type: application/json' \
    --user "$OPENSEARCH_USERNAME:$OPENSEARCH_PASSWORD" >/dev/null
else
  curl -sS -XPOST "$OPENSEARCH_URL/_plugins/_anomaly_detection/detectors/$DETECTOR_ID/_stop" \
    -H 'Content-Type: application/json' >/dev/null || true

  curl -sSf -XPOST "$OPENSEARCH_URL/_plugins/_anomaly_detection/detectors/$DETECTOR_ID/_start" \
    -H 'Content-Type: application/json' >/dev/null
fi
EOT
  }

  # Stop before the detector is deleted (destroy order is reverse of depends_on).
  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    environment = {
      OPENSEARCH_URL      = self.triggers.opensearch_url
      OPENSEARCH_USERNAME = self.triggers.opensearch_user
      OPENSEARCH_PASSWORD = self.triggers.opensearch_pass
      DETECTOR_ID         = self.triggers.detector_id
    }

    command = <<EOT
set -eo pipefail

if [ -n "$OPENSEARCH_USERNAME" ] || [ -n "$OPENSEARCH_PASSWORD" ]; then
  curl -sS -XPOST "$OPENSEARCH_URL/_plugins/_anomaly_detection/detectors/$DETECTOR_ID/_stop" \
    -H 'Content-Type: application/json' \
    --user "$OPENSEARCH_USERNAME:$OPENSEARCH_PASSWORD" >/dev/null || true
else
  curl -sS -XPOST "$OPENSEARCH_URL/_plugins/_anomaly_detection/detectors/$DETECTOR_ID/_stop" \
    -H 'Content-Type: application/json' >/dev/null || true
fi
EOT
  }

  depends_on = [opensearch_anomaly_detection.detector]
}

output "detector_id" {
  value = opensearch_anomaly_detection.detector.id
}
