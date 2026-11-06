.DEFAULT_GOAL := help
.PHONY: help lint format-check format ty pyright check check-windows check-linux verify generate-mcp-schemas check-mcp-schemas generate-openapi-schemas check-openapi-schemas

help:
	@just --list

lint format-check format ty pyright check check-windows check-linux verify generate-mcp-schemas check-mcp-schemas generate-openapi-schemas check-openapi-schemas:
	@just $@
