.PHONY: help version check-version bump-version lint lint-fix security test build \
       package-mcpb package-skill package clean create-release release

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
	SKILL_VER=$$(grep '^version:' rapid7-bulk-export-skill/SKILL.md | head -1 | sed 's/version: *//'); \
	echo "manifest.json:  $$MANIFEST_VER"; \
	echo "pyproject.toml: $$TOML_VER"; \
	echo "SKILL.md:       $$SKILL_VER"; \
	if [ "$$MANIFEST_VER" != "$$TOML_VER" ]; then \
		echo "ERROR: manifest.json ($$MANIFEST_VER) != pyproject.toml ($$TOML_VER)"; exit 1; \
	fi; \
	if [ "$$MANIFEST_VER" != "$$SKILL_VER" ]; then \
		echo "ERROR: manifest.json ($$MANIFEST_VER) != SKILL.md ($$SKILL_VER)"; exit 1; \
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
	sed -i.bak 's/^version = ".*"/version = "$(V)"/' pyproject.toml && rm -f pyproject.toml.bak
	sed -i.bak 's/^version: .*/version: $(V)/' rapid7-bulk-export-skill/SKILL.md && rm -f rapid7-bulk-export-skill/SKILL.md.bak
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

test: ## Run the test suite
	uv run pytest --tb=short

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
	cd rapid7-bulk-export-skill && zip -r "../rapid7-bulk-export-skill-$(VERSION).zip" .
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
