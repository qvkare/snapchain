.PHONY: build
build:
	docker compose build

.PHONY: dev
dev: build
	docker compose up --watch

.PHONY: clean
clean:
	docker compose down --remove-orphans --volumes --rmi=all
	cargo clean

.PHONY: changelog
changelog:
	#SNAPCHAIN_VERSION=$(awk -F '"' '/^version =/ {print $2}' ./Cargo.toml)
	echo "Generating changelog for version: $(SNAPCHAIN_VERSION)"
	git cliff --unreleased --tag $(SNAPCHAIN_VERSION) --prepend CHANGELOG.md

.PHONY: check-docs apply-docs docs

check-docs:
	@echo "Checking documentation requirements..."
	@if [ -z "$(GEMINI_API_KEY)" ]; then \
		echo "Error: GEMINI_API_KEY environment variable is required"; \
		exit 1; \
	fi
	@git diff HEAD~1 HEAD -- src/proto/ src/network/ src/core/ | ./scripts/analyze-docs-gemini.sh

apply-docs:
	@echo "Applying documentation updates..."
	@./scripts/apply-doc-updates.sh "$$(cat .doc-updates.json 2>/dev/null || echo '{}')"

docs: check-docs apply-docs
	@echo "Documentation check and update complete"

test-docs:
	@echo "Testing documentation analysis with latest changes..."
	@git diff HEAD~1 HEAD -- src/ > test-diff.txt || echo "No recent changes found"
	@if [ -s test-diff.txt ]; then \
		echo "Analyzing test diff..."; \
		./scripts/analyze-docs-gemini.sh "$$(cat test-diff.txt)"; \
	else \
		echo "No changes to analyze"; \
	fi
	@rm -f test-diff.txt

