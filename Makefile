.DEFAULT_GOAL := help
.PHONY: help lint format-check format ty pyright check check-windows check-linux verify generate-mcp-schemas check-mcp-schemas generate-openapi-schemas check-openapi-schemas
.PHONY: build up down dev dev-stop test-python test-frontend test-persistence check-runtime check-api check-mcp check-worker check-traffic check-docker-proxy check-docker-public check-browser check-scenarios check-controller-faults plan-output-maintenance

help:
	@just --list

lint format-check format ty pyright check check-windows check-linux verify generate-mcp-schemas check-mcp-schemas generate-openapi-schemas check-openapi-schemas:
	@just $@

build up down dev dev-stop test-python test-frontend test-persistence check-runtime check-api check-mcp check-worker check-traffic check-docker-proxy check-docker-public check-browser check-scenarios check-controller-faults plan-output-maintenance:
	@just $@
