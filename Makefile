# Multi-client pipelines on Prefect Cloud. Demo cluster = docker-desktop (kind node "desktop-control-plane").
ENV ?= prod
CLIENT ?= acme-manufacturing
export PREFECT_API_KEY ?= $(shell sed -n 's/^PREFECT_API_KEY=//p' .env 2>/dev/null)
export PREFECT_API_URL ?= $(shell sed -n 's/^PREFECT_API_URL=//p' .env 2>/dev/null)
export PATH := $(PWD)/.venv/bin:$(PATH)

.PHONY: validate image load client dev deploy automations prebake run-feeds run-backfill lake

validate:         ## what CI runs: parse every client file, run the extractor + DuckDB in-process
	.venv/bin/python -m pipelines.validate

image:            ## build the shared pipelines image
	docker build -t client-pipelines:latest .

load: image       ## copy it into the kind node (docker-desktop's node has its own image store)
	docker save client-pipelines:latest | docker exec -i desktop-control-plane ctr -n k8s.io images import -

client:           ## provision one client: namespace, secrets, lake PVC, worker, work pool + queues
	scripts/setup_client.sh $(CLIENT)

dev:              ## the shared dev pool feature branches deploy into
	scripts/setup_client.sh dev

deploy:           ## what GitHub Actions does on push (ENV=dev) / merge to main (ENV=prod)
	.venv/bin/python -m pipelines.deploy --env $(ENV)

automations:      ## fan-in, gold hop, late/failed e-mails, stale-run cancel
	.venv/bin/python scripts/setup_automations.py

run-feeds:        ## kick the 15-minute feeds for CLIENT now
	prefect deployment run "feeds-refresh/$(CLIENT)-feeds-15m"

run-backfill:     ## start a ~90 s backfill for CLIENT (30 pages of 100k rows)
	prefect deployment run "backfill/$(CLIENT)-backfill"

lake:             ## peek at CLIENT's lake:  make lake CLIENT=acme-manufacturing CMD='find /data -name "*.parquet" | head'
	kubectl -n client-$(CLIENT) run lake-shell --rm -i --restart=Never --image=client-pipelines:latest --image-pull-policy=Never \
	  --overrides='{"spec":{"containers":[{"name":"lake-shell","image":"client-pipelines:latest","imagePullPolicy":"Never","command":["sh","-c","$(CMD)"],"volumeMounts":[{"name":"lake","mountPath":"/data"}]}],"volumes":[{"name":"lake","persistentVolumeClaim":{"claimName":"$(shell .venv/bin/python -c "import yaml;print(yaml.safe_load(open('clients/$(CLIENT).yaml'))['lake_container'])")"}}]}}'
