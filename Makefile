TEST_COVERAGE_CUTOFF=90

clean:
	rm -rf data/*
	rm -rf docs/data_profiles/*
	rm -rf /tmp/*
run-tests:
	pipenv run mypy
	pipenv run pytest --cov --cov-fail-under=$(TEST_COVERAGE_CUTOFF)
run: run-tests
	python src/extract
ge:
	great_expectations --v2-api init -d docs --no-view
doc:	