.PHONY: help version check-version bump-version lint lint-fix security test build \
       docker-build docker-build-slim docker-build-distroless docker-test docker-test-slim \
       docker-test-distroless docker-clean package-mcpb package-skill package clean \
       create-release release

SHELL := /usr/bin/env bash
VERSION := $(shell jq -r '.version' manifest.json)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

version: ## Print the current version
	@echo $(VERSION)

check-version: ## Verify all version files are in sync
	@MANIFEST_VER=$(VERSION); \
	TOML_VER=$$(grep '^version' pyproject.toml | head -1 | sed 's/.*"\(.*\)".*/\1/'); \
	SKILL_VER=$$(grep '^version:' skills/rapid7-bulk-export/SKILL.md | head -1 | sed 's/version: *//'); \
	PLUGIN_VER=$$(jq -r '.version' plugin.json); \
	echo "manifest.json:  $$MANIFEST_VER"; \
	echo "pyproject.toml: $$TOML_VER"; \
	echo "SKILL.md:       $$SKILL_VER"; \
	echo "plugin.json:    $$PLUGIN_VER"; \
	if [ "$$MANIFEST_VER" != "$$TOML_VER" ]; then \
		echo "ERROR: manifest.json ($$MANIFEST_VER) != pyproject.toml ($$TOML_VER)"; exit 1; \
	fi; \
	if [ "$$MANIFEST_VER" != "$$SKILL_VER" ]; then \
		echo "ERROR: manifest.json ($$MANIFEST_VER) != SKILL.md ($$SKILL_VER)"; exit 1; \
	fi; \
	if [ "$$MANIFEST_VER" != "$$PLUGIN_VER" ]; then \
		echo "ERROR: manifest.json ($$MANIFEST_VER) != plugin.json ($$PLUGIN_VER)"; exit 1; \
	fi; \
	echo "All versions in sync: $$MANIFEST_VER"

bump-version: ## Set a new version: make bump-version V=0.3.0
	@if [ -z "$(V)" ]; then \
		echo "Usage: make bump-version V=0.3.0"; exit 1; \
	fi
	@if ! echo "$(V)" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+'; then \
		echo "Error: Version must be semver (e.g., 0.3.0)"; exit 1; \
	fi
	jq --arg v "$(V)" '.version = $$v' manifest.json > manifest.tmp && mv manifest.tmp manifest.json
	jq --arg v "$(V)" '.version = $$v' plugin.json > plugin.tmp && mv plugin.tmp plugin.json
	sed -i.bak 's/^version = ".*"/version = "$(V)"/' pyproject.toml && rm -f pyproject.toml.bak
	sed -i.bak 's/^version: .*/version: $(V)/' skills/rapid7-bulk-export/SKILL.md && rm -f skills/rapid7-bulk-export/SKILL.md.bak
	uv lock
	@echo "Bumped to $(V)"

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------

lint: ## Run ruff linter and format check
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

security: ## Run bandit security scan
	uv run bandit -r src/ -c pyproject.toml

lint-fix: ## Auto-fix lint and format issues
	uv run ruff check --fix src/ tests/
	uv run ruff format src/ tests/

test: ## Run the full test suite (unit + Docker integration test)
	@echo "Running unit tests..."
	@uv run pytest --tb=short
	@echo ""
	@echo "Running Docker integration test..."
	@$(MAKE) docker-test

local-test: ## Run end-to-end live test against the real Rapid7 API (requires RAPID7_API_KEY)
	@if [ -z "$$RAPID7_API_KEY" ]; then \
		echo ""; \
		echo "RAPID7_API_KEY is not set."; \
		echo ""; \
		echo "Set it directly:"; \
		echo "  export RAPID7_API_KEY=<your-key>"; \
		echo "  make local-test"; \
		echo ""; \
		echo "Or via 1Password CLI:"; \
		echo "  op run --env-file=.env.1password -- make local-test"; \
		echo ""; \
		echo "  (.env.1password example:)"; \
		echo "  RAPID7_API_KEY=op://vault/Rapid7 API/credential"; \
		echo "  RAPID7_REGION=us"; \
		exit 1; \
	fi
	@echo "Running live end-to-end test against Rapid7 API (region: $${RAPID7_REGION:-us})..."
	uv run pytest tests/test_local_e2e.py -v -s --tb=short

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

docker-build: ## Build the Docker image (UBI Minimal)
	docker build -t rapid7-bulk-export-mcp:$(VERSION) -t rapid7-bulk-export-mcp:latest .

docker-test: ## Test the Docker image (build, run, verify MCP endpoint)
	@echo "Testing UBI Minimal image..."
	@docker build -t rapid7-bulk-export-mcp:test . >/dev/null
	@CONTAINER_ID=$$(docker run -d \
		-p 8001:8000 \
		-e RAPID7_API_KEY=test-key \
		-e RAPID7_REGION=us \
		-v rapid7-test-data:/data \
		--tmpfs /tmp \
		--read-only \
		--security-opt no-new-privileges:true \
		rapid7-bulk-export-mcp:test); \
	echo "Started container: $$CONTAINER_ID"; \
	echo "Waiting for MCP endpoint..."; \
	READY=0; \
	for i in 1 2 3 4 5 6 7 8 9 10; do \
		if curl -s http://localhost:8001/mcp -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" -d '{"jsonrpc":"2.0","method":"tools/list","id":1}' 2>/dev/null | grep -q '"jsonrpc"'; then \
			READY=1; \
			break; \
		fi; \
		sleep 1; \
	done; \
	if [ $$READY -eq 1 ]; then \
		echo "✓ UBI Minimal MCP endpoint responding"; \
		docker stop $$CONTAINER_ID >/dev/null 2>&1; \
		docker rm $$CONTAINER_ID >/dev/null 2>&1; \
		exit 0; \
	else \
		echo "✗ UBI Minimal MCP endpoint not responding after 10 attempts"; \
		docker logs $$CONTAINER_ID; \
		docker stop $$CONTAINER_ID >/dev/null 2>&1; \
		docker rm $$CONTAINER_ID >/dev/null 2>&1; \
		exit 1; \
	fi


docker-clean: ## Remove test Docker images and volumes
	docker rmi rapid7-bulk-export-mcp:test 2>/dev/null || true
	docker volume rm rapid7-test-data 2>/dev/null || true

# ---------------------------------------------------------------------------
# Packaging
# ---------------------------------------------------------------------------

package-mcpb: ## Build the MCPB bundle
	@command -v mcpb >/dev/null 2>&1 || { echo "mcpb not found -- run: npm install -g @anthropic-ai/mcpb"; exit 1; }
	mcpb pack
	@MCPB=$$(ls *.mcpb 2>/dev/null | head -1); \
	if [ -z "$$MCPB" ]; then echo "ERROR: mcpb pack produced no .mcpb file"; exit 1; fi; \
	echo "Built $$MCPB"

package-skill: ## Zip the agent skill directory
	cd skills/rapid7-bulk-export && zip -r "../../rapid7-bulk-export-skill-$(VERSION).zip" .
	@echo "Built rapid7-bulk-export-skill-$(VERSION).zip"

package: package-mcpb package-skill ## Build all release artifacts

# ---------------------------------------------------------------------------
# Release (used by CI -- requires GH_TOKEN and gh CLI)
# ---------------------------------------------------------------------------

create-release: ## Upload artifacts to a new GitHub release
	@MCPB=$$(ls *.mcpb 2>/dev/null | head -1); \
	SKILL="rapid7-bulk-export-skill-$(VERSION).zip"; \
	if [ -z "$$MCPB" ]; then echo "ERROR: no .mcpb artifact found -- run make package first"; exit 1; fi; \
	if [ ! -f "$$SKILL" ]; then echo "ERROR: $$SKILL not found -- run make package first"; exit 1; fi; \
	echo "Creating release v$(VERSION)"; \
	echo "  MCPB:  $$MCPB"; \
	echo "  Skill: $$SKILL"; \
	gh release create "v$(VERSION)" \
		--title "Release v$(VERSION)" \
		--generate-notes \
		"$$MCPB" \
		"$$SKILL"

release: check-version package create-release ## Full release: verify, build, and publish

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

clean: ## Remove build artifacts
	rm -f *.mcpb rapid7-bulk-export-skill-*.zip
