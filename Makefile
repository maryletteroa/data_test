TEST_COVERAGE_CUTOFF=100

clean:
	
test: clean
	pipenv run pytest --cov --cov-fail-under=$(TEST_COVERAGE_CUTOFF)

intall-pre-commit:
	pipenv run pre-commit install -t pre-commit

intall-pre-push:
	pipenv run pre-commit install -t pre-push