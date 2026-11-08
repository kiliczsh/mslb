.DEFAULT_GOAL := help
.PHONY: help ide build-python build-frontend lint format-check format ty pyright check lint-local format-check-local format-python-local ty-local pyright-local check-python-local check-windows check-linux verify generate-mcp-schemas check-mcp-schemas generate-openapi-schemas check-openapi-schemas build up down test-python test-frontend test-persistence check-runtime check-api check-mcp check-worker check-traffic check-docker-proxy check-docker-public check-browser check-scenarios check-controller-faults plan-output-maintenance dev dev-stop config status logs migrate test-unit bundle verify-bundle check-source-quality check-deployment

help:
	@just --list

ide build-python build-frontend lint format-check format ty pyright check lint-local format-check-local format-python-local ty-local pyright-local check-python-local check-windows check-linux verify generate-mcp-schemas check-mcp-schemas generate-openapi-schemas check-openapi-schemas build up down test-python test-frontend test-persistence check-runtime check-api check-mcp check-worker check-traffic check-docker-proxy check-docker-public check-browser check-scenarios check-controller-faults plan-output-maintenance dev dev-stop config status logs migrate test-unit bundle verify-bundle check-source-quality check-deployment:
	@just $@
