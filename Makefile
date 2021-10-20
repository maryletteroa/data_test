TEST_COVERAGE_CUTOFF=90

clean:
	rm -rf data_testing/raw
run:
	python data_testing
tests:
	pipenv run pytest --cov --cov-fail-under=$(TEST_COVERAGE_CUTOFF)
test-write:
	pipenv run pytest test/test_extract_data.py -k test_write_data_table
intall-pre-commit:
	pipenv run pre-commit install -t pre-commit

intall-pre-push:
	pipenv run pre-commit install -t pre-push
doc:	