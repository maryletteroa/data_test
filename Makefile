TEST_COVERAGE_CUTOFF=90

clean:
	rm -rf data_testing/raw/test.csv
run:
	python data_testing
tests:
	pipenv run pytest --cov --cov-fail-under=$(TEST_COVERAGE_CUTOFF)
doc:	