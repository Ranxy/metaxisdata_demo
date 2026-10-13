# metaxisdata demo -- run every target from the repository root.
#
#   make up                        start the stack (creates .env on first run)
#   make up PROFILE=extra          start it with an optional profile
#   make smoke                     is the demo populated?
#   make reset                     drop everything, including volumes, and start over
#
# See README.md for the walkthrough and docs/design.md for the why.

SHELL := /usr/bin/env bash

DOCKER_COMPOSE ?= docker compose
PROFILE ?=

COMPOSE_FILES := -f docker-compose.yml
ifneq ($(wildcard docker-compose.profiles.yml),)
COMPOSE_FILES += -f docker-compose.profiles.yml
endif

COMPOSE := $(DOCKER_COMPOSE) $(COMPOSE_FILES)
PROFILE_FLAG := $(if $(PROFILE),--profile $(PROFILE),)

.PHONY: help up down reset logs ps check-secrets

help:
	@echo "metaxisdata demo -- start it, then open http://localhost:8083"
	@echo ""
	@echo "  make up           start everything and wait until the demo is ready"
	@echo "  make smoke        check that the demo is populated"
	@echo "  make logs         follow the logs"
	@echo "  make down         stop it, keep the data"
	@echo "  make reset        stop it and delete the data"
	@echo "  make evolve       add a column and re-sync, to see metadata history"
	@echo "  make ol-demo      run an OpenLineage producer from outside Airflow"
	@echo "  make ps           show service state"
	@echo ""
	@echo "See README.md for the guided tour."

# Compose reads .env automatically; create it from the documented example on first use.
.env:
	@cp .env.example .env
	@echo "created .env from .env.example"

up: .env
	$(COMPOSE) $(PROFILE_FLAG) up -d --build
	@echo "waiting for the pipeline to finish (it takes a couple of minutes on a cold start)"
	@$(COMPOSE) wait etl-runner
	@$(MAKE) --no-print-directory smoke

down:
	$(COMPOSE) $(PROFILE_FLAG) down

# `--profile *` makes sure optional services are stopped too, and -v drops the data.
reset:
	$(COMPOSE) --profile '*' down -v --remove-orphans

logs:
	$(COMPOSE) $(PROFILE_FLAG) logs -f --tail=100

ps:
	$(COMPOSE) $(PROFILE_FLAG) ps

# Is the demo populated? `up` runs this, so a half-prepared stack is never reported as ready.
smoke:
	$(COMPOSE) run --rm --no-deps --entrypoint python3 bootstrap /scripts/smoke.py

# One schema change plus a re-sync, so the metadata browser's history and diff light up.
evolve:
	$(COMPOSE) --profile tools run --rm schema-evolve
	$(COMPOSE) run --rm --no-deps --entrypoint python3 bootstrap /scripts/resync.py

check-secrets:
	@scripts/check_secrets.sh

# Show the namespace-mapping story: the same event first lands outside any instance, then
# resolves to a real one once a mapping exists.
ol-demo:
	$(COMPOSE) --profile ol run --rm ol-producer
	$(COMPOSE) --profile ol run --rm ol-producer --map
