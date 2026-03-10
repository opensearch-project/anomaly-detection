# OpenSearch Anomaly Detection with Terraform

## Purpose

This project uses Terraform to manage an OpenSearch Anomaly Detection detector and its job lifecycle.

It does two things:

- Creates or updates a detector via `opensearch_anomaly_detection`.
- Automatically stops and restarts the detector job on apply (and stops it on destroy) using `null_resource` + `local-exec` calls to the OpenSearch AD APIs.

## What This Configuration Creates

- 1 anomaly detector with:
  - configurable index pattern (`indices`)
  - configurable time field (`time_field`)
  - one feature using `max(<feature_field>)`
  - configurable detection interval and window delay
  - configurable result index
- Output:
  - `detector_id`

## Prerequisites

- Terraform `>= 1.5.0`
- OpenSearch cluster reachable from your machine
- OpenSearch Anomaly Detection plugin/API available
- `curl` installed (used by `local-exec` provisioners)

## Configuration

Defaults in [`main.tf`](main.tf) target local development:

- `opensearch_url = "http://localhost:9200"`
- `opensearch_username = ""`
- `opensearch_password = ""`

You can change `opensearch_url` to your remote OpenSearch endpoint, for example `https://your-cluster.example.com:9200`.

If your cluster has security enabled, prefer environment variables for credentials in production. `terraform.tfvars` and CLI flags also work, but they are less suitable for secrets.

Preferred example: environment variables

```bash
export TF_VAR_opensearch_url='https://your-cluster.example.com:9200'
export TF_VAR_opensearch_username='admin'
export TF_VAR_opensearch_password='myStrongPassword123!'

terraform plan
```

Example `terraform.tfvars`:

```hcl
opensearch_url      = "https://your-cluster.example.com:9200"
opensearch_username = "admin"
opensearch_password = "myStrongPassword123!"
```

Example CLI flags:

```bash
terraform plan \
  -var='opensearch_url=https://your-cluster.example.com:9200' \
  -var='opensearch_username=admin' \
  -var='opensearch_password=myStrongPassword123!'
```

Avoid `-var` for passwords in production when possible, since command-line arguments can leak into shell history or CI logs.

If you use `terraform.tfvars`, make sure it is excluded from version control.

Important: this configuration currently stores connection settings in `null_resource` triggers so the destroy-time provisioner can stop the detector job. That means credentials may still be written to Terraform state even when provided via `TF_VAR_...` environment variables.

Common detector variables:

- `detector_name`
- `indices`
- `time_field`
- `feature_field`
- `detection_interval_minutes`
- `window_delay_minutes`
- `result_index`

## How To Use

1. Initialize providers:

```bash
terraform init
```

2. (Optional) Create `terraform.tfvars`:

```hcl
opensearch_url      = "http://localhost:9200"
opensearch_username = ""
opensearch_password = ""

detector_name                = "tf-detector"
indices                      = ["server-metrics"]
time_field                   = "@timestamp"
feature_field                = "deny"
detection_interval_minutes   = 1
window_delay_minutes         = 1
result_index                 = "opensearch-ad-plugin-result-tf"
```

3. Review the plan:

```bash
terraform plan
```

4. Apply:

```bash
terraform apply
```

5. Get the detector ID:

```bash
terraform output detector_id
```

## Destroy

To remove resources:

```bash
terraform destroy
```

The configuration attempts to stop the detector job before deletion.

## Notes

- `null_resource.start_detector_job` is trigger-based and re-runs when detector config or connection settings change.
- Credentials are optional for unsecured local clusters; provide them for secured clusters.
