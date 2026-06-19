"""Lab3-style wrapper exposing `uv run deploy|plan|destroy` over the terraform module.

Loads cloud provider + Azure OpenAI credentials from the parent quickstart-streaming-agents
credentials.env (the same file `uv run deploy` consumes in that repo), validates the
Azure-specific fields, and exports them as TF_VAR_* env vars before shelling to terraform.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).parent
TF_DIR = ROOT / "terraform"
CREDENTIALS_FILE = Path(
    "/Users/safouan.essebbar/quickstart-streaming-agents/credentials.env"
)

# Required across all clouds.
BASE_REQUIRED = {
    "TF_VAR_confluent_cloud_api_key": "Confluent Cloud API Key",
    "TF_VAR_confluent_cloud_api_secret": "Confluent Cloud API Secret",
    "TF_VAR_cloud_provider": "Cloud Provider (aws or azure)",
    "TF_VAR_cloud_region": "Cloud Region",
}

# Cloud-specific LLM credentials (matches lab3 deploy.py).
AZURE_REQUIRED = {
    "TF_VAR_azure_openai_endpoint_raw": "Azure OpenAI Endpoint",
    "TF_VAR_azure_openai_api_key": "Azure OpenAI API Key",
}
AWS_REQUIRED = {
    "TF_VAR_aws_bedrock_access_key": "AWS Bedrock Access Key",
    "TF_VAR_aws_bedrock_secret_key": "AWS Bedrock Secret Key",
}


def _load_and_validate() -> dict[str, str]:
    if not CREDENTIALS_FILE.exists():
        print(f"Error: credentials.env not found at {CREDENTIALS_FILE}")
        sys.exit(1)

    creds = {k: v for k, v in dotenv_values(str(CREDENTIALS_FILE)).items() if v}
    cloud = creds.get("TF_VAR_cloud_provider", "").lower()

    required = dict(BASE_REQUIRED)
    if cloud == "azure":
        required.update(AZURE_REQUIRED)
    elif cloud == "aws":
        required.update(AWS_REQUIRED)
    else:
        print(
            f"Error: TF_VAR_cloud_provider must be 'azure' or 'aws' (got '{cloud}')."
        )
        sys.exit(1)

    missing = [label for key, label in required.items() if not creds.get(key)]
    if missing:
        print(f"Error: credentials.env is incomplete. Missing or empty:")
        for label in missing:
            print(f"  - {label}")
        sys.exit(1)

    return creds


def _print_banner(creds: dict[str, str]) -> None:
    cloud = creds["TF_VAR_cloud_provider"].lower()
    print("=== predictive-maintenance deploy ===")
    print(f"✓ Credentials loaded from {CREDENTIALS_FILE}")
    print(f"  Cloud:        {cloud}")
    print(f"  Region:       {creds['TF_VAR_cloud_region']}")
    if cloud == "azure":
        print(f"  Azure OpenAI: {creds['TF_VAR_azure_openai_endpoint_raw']}")
    print(f"  Reusing:      core (Kafka + SR + Flink pool + llm_textgen_model)")
    print(f"                lab3 (remote-mcp-connection + remote_mcp_model)")
    print(f"  Adding:       truck_telemetry topic + Flink source table")
    print()


def _export_tf_vars(creds: dict[str, str]) -> None:
    for key, value in creds.items():
        if key.startswith("TF_VAR_"):
            os.environ[key] = value


def _run(*args: str) -> int:
    cmd = ["terraform", *args]
    print(f"$ {' '.join(cmd)}  (cwd={TF_DIR})", flush=True)
    return subprocess.run(cmd, cwd=TF_DIR).returncode


def _ensure_init() -> None:
    if not (TF_DIR / ".terraform").exists():
        rc = _run("init", "-input=false")
        if rc != 0:
            sys.exit(rc)


def _prelude() -> None:
    creds = _load_and_validate()
    _print_banner(creds)
    _export_tf_vars(creds)
    _ensure_init()


def main() -> None:
    _prelude()
    sys.exit(_run("apply", "-auto-approve"))


def plan() -> None:
    _prelude()
    sys.exit(_run("plan"))


def destroy() -> None:
    _prelude()
    sys.exit(_run("destroy", "-auto-approve"))


def datagen() -> None:
    """Run the local Python data generator with Kafka + SR creds from core tfstate."""
    core_tf_dir = Path(
        "/Users/safouan.essebbar/quickstart-streaming-agents/terraform/core"
    )
    if not (core_tf_dir / "terraform.tfstate").exists():
        print(f"Error: core terraform.tfstate not found at {core_tf_dir}")
        sys.exit(1)

    def _tf_output(name: str) -> str:
        r = subprocess.run(
            ["terraform", "output", "-raw", name],
            cwd=core_tf_dir, capture_output=True, text=True, check=True,
        )
        return r.stdout.strip()

    bootstrap = _tf_output("confluent_kafka_cluster_bootstrap_endpoint")
    # Producer's bootstrap.servers takes host:port (no SASL_SSL:// prefix).
    if "://" in bootstrap:
        bootstrap = bootstrap.split("://", 1)[1]

    env = os.environ.copy()
    env.update({
        "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
        "KAFKA_API_KEY": _tf_output("app_manager_kafka_api_key"),
        "KAFKA_API_SECRET": _tf_output("app_manager_kafka_api_secret"),
        "SCHEMA_REGISTRY_URL": _tf_output("confluent_schema_registry_rest_endpoint"),
        "SCHEMA_REGISTRY_API_KEY": _tf_output("app_manager_schema_registry_api_key"),
        "SCHEMA_REGISTRY_API_SECRET": _tf_output("app_manager_schema_registry_api_secret"),
    })

    script = ROOT / "data-gen" / "generate.py"
    print(f"$ python {script} {' '.join(sys.argv[1:])}", flush=True)
    sys.exit(subprocess.run(
        [sys.executable, str(script), *sys.argv[1:]], env=env
    ).returncode)
