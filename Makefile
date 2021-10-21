TEST_COVERAGE_CUTOFF=90

run:
	python src/extract
run-tests:
	pipenv run mypy
	pipenv run pytest --cov --cov-fail-under=$(TEST_COVERAGE_CUTOFF)
doc:	