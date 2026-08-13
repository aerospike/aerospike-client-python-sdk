.PHONY: test dev docs docs-clean docs-serve examples bench bench-quick bench-compare check-pin test-sc

dev:
	pip install -e ".[dev]"

test:
	pytest tests

test-unit:
	pytest tests/unit

test-int:
	pytest tests/integration

# run the integration suite against a SC namespace instead of the AP default.
# AEROSPIKE_GENERAL_AUTH turns the default policy auth-aware
# AEROSPIKE_NAMESPACE aims general_namespace() at the SC namespace
# SC seed and AEROSPIKE_AUTH_* come from aerospike.env
# point aerospike.env's AEROSPIKE_HOST at an SC-capable cluster if it isn't already.
# Override the namespace name with `make test-sc SC_NAMESPACE=<name>`.
SC_NAMESPACE ?= test_sc
test-sc:
	AEROSPIKE_GENERAL_AUTH=1 AEROSPIKE_NAMESPACE=$(SC_NAMESPACE) pytest tests/integration

check-pin:
	pytest tests/unit/pin_drift_test.py -q

examples:
	@for f in examples/*.py; do \
		case "$$f" in examples/_env.py|examples/__init__.py) continue;; esac; \
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
	python -m benchmarks.compare -k 100000 -z 32 --threads 32 -w RU,50 -d 15 --warmup 3 --cooldown 3 --modes pac-blocking,pac-async,async,sync
