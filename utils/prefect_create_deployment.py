"""
Generate Prefect Deployment YAML Files

This script walks through a local source directory tree to identify Python flow modules
and generates a corresponding `-deploy.yaml` for each one using a standardized format.

Features:
- Converts local paths to src-mapped Prefect-compatible paths
- Configurable scheduling, worker pool, and entrypoint logic
- Auto-creates output folders and filenames for each deployment

Useful for:
- Quickly scaffolding Prefect 2.x+ deployment YAMLs without manual editing
- Automating CI/CD or multi-flow registration
"""

import os
import re
from pathlib import Path

prefect_version = "3.0.1"  # Prefect version string for deployment compatibility

src_root = "/prefect/src"  # Container or deployment path prefix
local_root = "d:/Git/example_data_etl/src"  # Local root path
output_folder = "d:/exports/prefect_jobs"  # Output directory for generated YAMLs


def create_deployment_yaml(
        project_name: str = "undefined",
        project_dir: str = "staging/api/undefined",
        deployment_name: str = "undefined",
        entrypoint: str = "module.py:function",
        worker_pool: str = "default-worker-pool",
        schedule_interval: float = 3600.0,
        schedule_timezone: str = "UTC",
        schedule_active: str = 'true'
) -> None:
    """
    Generate a deployment YAML file for a given flow.
    """
    yaml_body = f"""# Welcome to your prefect.yaml file! You can use this file for storing and managing
# configuration for deploying your flows. We recommend committing this file to source
# control along with your flow code.

name: {project_name}
prefect-version: {prefect_version}

build: null
push: null

pull:
- prefect.deployments.steps.set_working_directory:
    directory: {src_root}

deployments:
- name: {deployment_name}
  version: null
  tags: []
  concurrency_limit: null
  description: null
  entrypoint: {entrypoint}
  parameters: {{ }}
  work_pool:
    name: {worker_pool}
    work_queue_name: null
    job_variables: {{ }}
enforce_parameter_schema: true
schedules:
- interval: {schedule_interval}
  anchor_date: '2024-01-01T01:00:00+00:00'
  timezone: {schedule_timezone}
  active: {schedule_active}
  max_active_runs: null
  catchup: false
""".lstrip()

    Path(output_folder).mkdir(parents=True, exist_ok=True)

    with open(f"{output_folder}/{deployment_name}-deploy.yaml", "w") as f:
        f.write(yaml_body)


for root, dirs, files in os.walk(local_root):
    for d in dirs:
        if d == 'src':
            for file in os.listdir(f"{root}/{d}"):
                if file.endswith(".py"):
                    deployment = file.split(".")[0]
                    print(f"Creating deployment for {deployment}")
                    create_deployment_yaml(
                        project_name=os.path.basename(root),
                        project_dir=f"{root}/{d}".replace("\\", "/").replace(local_root, src_root),
                        deployment_name=deployment,
                        entrypoint=f"{root}/{d}/{deployment}.py:{deployment}".replace("\\", "/").replace(local_root, src_root),
                        worker_pool="default-worker-pool",
                        schedule_interval=3600.0,
                        schedule_timezone="UTC",
                        schedule_active='true'
                    )
        else:
            continue