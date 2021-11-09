TEST_COVERAGE_CUTOFF=100

clean:
	rm -rf data/*
	rm -rf docs/dataprofiles/*
	rm -rf docs/great_expectations/uncommitted/*
clean-all:
	rm -rf data/*
	rm -rf docs/dataprofiles/*
	rm -rf docs/great_expectations
run-all:
	python src/extract
	python src/profile/profile_source_data.py
	python src/validate_data/validate_source_data.py 
	python src/ingest
	python src/profile/profile_raw_data.py
	python src/validate_data/validate_raw_data.py
	python src/transform
	python src/profile/profile_clean_data.py
	python src/validate_data/validate_clean_data.py
	python src/present
	python src/profile/profile_present_data.py
	python src/validate_data/validate_present_data.py
run:
	python src/extract
	python src/ingest
	python src/transform
	python src/present
run-tests:
	pipenv run mypy
	pipenv run pytest --cov --cov-fail-under=$(TEST_COVERAGE_CUTOFF)
ge-init:
	great_expectations --v2-api init -d docs --no-view --no-usage-stats
profiles:
	python src/profile/profile_source_data.py
	python src/profile/profile_raw_data.py
	python src/profile/profile_clean_data.py
	python src/profile/profile_present_data.py
expectations:
	python src/profile/profile_source_data.py --expect
	python src/profile/profile_raw_data.py --expect
	python src/profile/profile_clean_data.py --expect
	python src/profile/profile_present_data.py --expect
validations:
	python src/validate_data/validate_source_data.py
	python src/validate_data/validate_raw_data.py
	python src/validate_data/validate_clean_data.py
	python src/validate_data/validate_present_data.py