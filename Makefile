.PHONY: new-app build preview setup

REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Install dependencies
setup:
	npm install

# Create a new app from a template.
# Usage:
#   make new-app NAME=my-app                    (blank template, default)
#   make new-app NAME=my-app TEMPLATE=refined   (sidebar shell + drill drawer)
# Templates live under templates/ — each must contain app.json and src/{main,App}.tsx.
TEMPLATE ?= blank
new-app:
ifndef NAME
	$(error NAME is required. Usage: make new-app NAME=my-app [TEMPLATE=refined])
endif
	@if [ ! -d "templates/$(TEMPLATE)" ]; then echo "Error: unknown template \"$(TEMPLATE)\". Available: $$(ls templates/ | tr '\n' ' ')"; exit 1; fi
	@if [ -d "examples/$(NAME)" ]; then echo "Error: examples/$(NAME) already exists"; exit 1; fi
	cp -R "templates/$(TEMPLATE)" "examples/$(NAME)"
	@echo "Created examples/$(NAME) from templates/$(TEMPLATE). Edit app.json and src/App.tsx to get started."

# Build an app to dist/index.html
# Usage: make build NAME=my-app
build:
ifndef NAME
	$(error NAME is required. Usage: make build NAME=my-app)
endif
	node build.mjs "examples/$(NAME)"

# Build with preview data for local testing
# Usage: make preview NAME=revenue-explorer
preview:
ifndef NAME
	$(error NAME is required. Usage: make preview NAME=revenue-explorer)
endif
	node preview.mjs "$(NAME)"
