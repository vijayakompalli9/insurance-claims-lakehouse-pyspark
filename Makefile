PY ?= python
export TZ := UTC

.PHONY: install data run test lint clean

install:
	$(PY) -m pip install -r requirements-dev.txt
	$(PY) -m pip install -e . --no-deps

data:
	$(PY) -m claims_lakehouse.generate_data --out data/raw

run:
	$(PY) -m claims_lakehouse.run --batch all --reset

test:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check .

clean:
	rm -rf data .pytest_cache .ruff_cache spark-warehouse metastore_db
