TEST_COVERAGE_CUTOFF=90

clean:
	rm -rf data/raw/test.csv
run:
	python src/extract
run-tests:
	pipenv run pytest --cov --cov-fail-under=$(TEST_COVERAGE_CUTOFF)
doc:	