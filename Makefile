.DEFAULT_GOAL := help
.PHONY: help lint format-check format ty pyright check check-windows check-linux verify generate-mcp-schemas check-mcp-schemas

help:
	@just --list

lint format-check format ty pyright check check-windows check-linux verify generate-mcp-schemas check-mcp-schemas:
	@just $@
