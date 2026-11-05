# Documentation

Start with the root [README](../README.md) to run mslb. Choose the document for your task:

| Document | Read it to |
| --- | --- |
| [DEVELOPMENT](DEVELOPMENT.md) | Set up tooling, run checks, troubleshoot and maintain the installation. |
| [DESIGN](DESIGN.md) | Understand implemented architecture, simulation rules, contracts and trade-offs. |
| [PLAN](PLAN.md) | Read the implementation plan, updated through development and consolidated into a build brief. |
| [QUESTIONNAIRE](QUESTIONNAIRE.md) | Review planning questions, actual preferences and their reasons. |
| [AI-USAGE](AI-USAGE.md) | Follow initial planning, candidate direction, reviewed decisions and verification with AI assistance. |
| [MCP](MCP.md) | Connect a client and use tools, resources and bounded waiting. |
| [TREE](TREE.md) | Locate the main source and configuration owners. |
| [Assignment](assignment/requirements.md) | Read the unchanged original requirements. |

The [local API reference](http://127.0.0.1:8080/api/docs) is the REST endpoint reference.
Tracked generated contracts live in [contracts/openapi](contracts/openapi/) and
[contracts/mcp](contracts/mcp/); regenerate them through the developer commands.

Supplied examples and the unchanged evaluator are in `assignment/test-data/core/`.
Additional scenarios are in `assignment/test-data/expanded/`. Their local README files
preserve dataset provenance and mappings; they do not introduce another product guide.
The presence of fixture files alone does not establish a passing execution.

Development followed an initial plan, staged implementation, review and verification,
and revised decisions. DESIGN describes the implemented behavior. PLAN consolidates the
updated implementation plan; QUESTIONNAIRE distinguishes initial choices from later
refinements. AI-USAGE explains the candidate's direction and review through selected
examples. These delivery documents describe that process without presenting the final
consolidation as a verbatim original prompt or a complete conversation record.
