.PHONY: antlr generate-ael clean-ael test test-deps dev docs docs-clean docs-serve examples bench bench-quick bench-compare

# ANTLR JAR location - download if not present
ANTLR_JAR ?= antlr-4.13.0-complete.jar
ANTLR_URL = https://www.antlr.org/download/$(ANTLR_JAR)

# AEL grammar and output directories
AEL_GRAMMAR = aerospike_sdk/ael/antlr4/Condition.g4
AEL_OUTPUT = aerospike_sdk/ael/antlr4/generated
AEL_GENERATED = $(AEL_OUTPUT)/ConditionLexer.py $(AEL_OUTPUT)/ConditionParser.py $(AEL_OUTPUT)/ConditionListener.py $(AEL_OUTPUT)/ConditionVisitor.py

antlr-download:
	@if [ ! -f $(ANTLR_JAR) ]; then \
		echo "Downloading ANTLR JAR..."; \
		curl -L -o $(ANTLR_JAR) $(ANTLR_URL); \
	fi

generate-ael: antlr-download
	@echo "Checking Java version (requires Java 11+)..."
	@java -version 2>&1 | head -1 || (echo "Error: Java is not installed or not in PATH. ANTLR requires Java 11 or higher." && exit 1)
	@echo "Generating Python parser from ANTLR grammar..."
	@mkdir -p $(AEL_OUTPUT)
	@cd aerospike_sdk/ael/antlr4 && java -jar ../../../$(ANTLR_JAR) -Dlanguage=Python3 -o generated -visitor -listener Condition.g4
	@touch $(AEL_OUTPUT)/__init__.py
	@echo "Generated parser files in $(AEL_OUTPUT)/"

clean-ael:
	@echo "Cleaning generated AEL parser files..."
	@rm -rf $(AEL_OUTPUT)
	@echo "Cleaned AEL parser files"

dev:
	pip install -e ".[dev]"

# Minimal pytest stack only (see pyproject.toml [project.optional-dependencies] test)
test-deps:
	pip install -e ".[test]"

# macOS default soft FD limit (256) is too low for the full async suite; raise when the shell allows.
test:
	bash -c 'ulimit -n 8192 2>/dev/null || true; exec pytest tests'

test-unit:
	bash -c 'ulimit -n 8192 2>/dev/null || true; exec pytest tests/unit'

test-int:
	bash -c 'ulimit -n 8192 2>/dev/null || true; exec pytest tests/integration'

examples:
	@for f in examples/*_example.py examples/operation_differences.py; do \
		echo "=== $$f ==="; \
		python "$$f" || exit 1; \
		echo; \
	done

docs-clean:
	@rm -rf docs/_build
	@echo "Cleaned docs/_build"

docs:
	sphinx-build -b html docs docs/_build/html -W

docs-serve:
	sphinx-autobuild docs docs/_build/html

bench:
	python -m benchmarks.benchmark -k 100000 -z 32 -w I -c 100000 -d 120
	python -m benchmarks.benchmark -k 100000 -z 32 -w RU,50 -d 10

bench-quick:
	python -m benchmarks.benchmark -k 1000 -z 4 -w RU,50 -d 5 --warmup 0 --cooldown 0

bench-compare:
	python -m benchmarks.compare -k 100000 -z 32 --threads 4 -w RU,50 -d 15 --warmup 3 --cooldown 3 --modes pac,async,sim-sync
