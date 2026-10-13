"""Create or update one client's Kubernetes work pool and its queues.

    python scripts/setup_pool.py <pool> <namespace> <pvc-claim>

Base job template: jobs run in the client's namespace, mount the client's lake,
and get both secrets (Prefect API, client source credentials) from that
namespace. Queues: feeds (high priority), default, backfill (low priority,
one at a time).
"""

import json
import subprocess
import sys

pool, namespace, claim = sys.argv[1:4]
IMAGE = "client-pipelines:latest"

t = json.loads(subprocess.check_output(["prefect", "work-pool", "get-default-base-job-template", "--type", "kubernetes"]))
v = t["variables"]["properties"]
v["namespace"]["default"] = namespace
v["service_account_name"]["default"] = "prefect-worker"
v["image"]["default"] = IMAGE
v["image_pull_policy"]["default"] = "Never"
v["finished_job_ttl"]["default"] = 300
v["stream_output"]["default"] = True
spec = t["job_configuration"]["job_manifest"]["spec"]["template"]["spec"]
spec["volumes"] = [{"name": "lake", "persistentVolumeClaim": {"claimName": claim}}]
spec["containers"][0]["volumeMounts"] = [{"name": "lake", "mountPath": "/data"}]
spec["containers"][0]["envFrom"] = [{"secretRef": {"name": "prefect-api"}}, {"secretRef": {"name": "client-source-credentials"}}]

path = f"k8s/generated/{pool}.base-job-template.json"
subprocess.run(["mkdir", "-p", "k8s/generated"], check=True)
with open(path, "w") as f:
    json.dump(t, f, indent=1)

# --overwrite: the worker may already have auto-created the pool with the default template.
subprocess.run(["prefect", "work-pool", "create", pool, "--type", "kubernetes", "--base-job-template", path, "--overwrite"], check=True)

for name, prio, limit in (("feeds", 1, None), ("default", 3, None), ("backfill", 5, None)):
    probe = subprocess.run(["prefect", "work-queue", "inspect", name, "--pool", pool], capture_output=True)
    if probe.returncode != 0:
        subprocess.run(["prefect", "work-queue", "create", name, "--pool", pool, "--priority", str(prio)] + (["--limit", str(limit)] if limit else []), check=True)
    else:
        subprocess.run(["prefect", "work-queue", "set-concurrency-limit", name, str(limit), "--pool", pool], check=True) if limit else None
print(f"pool {pool}: namespace={namespace} claim={claim} queues=feeds(1) default(3) backfill(5)")
