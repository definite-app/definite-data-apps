.PHONY: new-app build preview setup

REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Install dependencies
setup:
	npm install

# Create a new app from the blank template
# Usage: make new-app NAME=my-app
new-app:
ifndef NAME
	$(error NAME is required. Usage: make new-app NAME=my-app)
endif
	@if [ -d "examples/$(NAME)" ]; then echo "Error: examples/$(NAME) already exists"; exit 1; fi
	cp -R templates/blank "examples/$(NAME)"
	@echo "Created examples/$(NAME). Edit app.json and src/App.tsx to get started."

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
