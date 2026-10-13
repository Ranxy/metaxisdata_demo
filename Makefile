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
	@echo "metaxisdata demo"
	@echo ""
	@echo "  make up               start the stack (PROFILE=<name> for an optional profile)"
	@echo "  make down             stop the stack, keep its data"
	@echo "  make reset            stop it and drop every volume"
	@echo "  make logs             follow the logs"
	@echo "  make ps               show service state"
	@echo "  make check-secrets    fail if a credential looks committed"

# Compose reads .env automatically; create it from the documented example on first use.
.env:
	@cp .env.example .env
	@echo "created .env from .env.example"

up: .env
	$(COMPOSE) $(PROFILE_FLAG) up -d --build

down:
	$(COMPOSE) $(PROFILE_FLAG) down

# `--profile *` makes sure optional services are stopped too, and -v drops the data.
reset:
	$(COMPOSE) --profile '*' down -v --remove-orphans

logs:
	$(COMPOSE) $(PROFILE_FLAG) logs -f --tail=100

ps:
	$(COMPOSE) $(PROFILE_FLAG) ps

check-secrets:
	@scripts/check_secrets.sh
