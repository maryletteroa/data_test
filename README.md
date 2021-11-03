# Introduction
This repository contains examples of code and data tests using the following tools:

- [pytest](https://docs.pytest.org/en/6.2.x/)
- [pytest-cov](https://pytest-cov.readthedocs.io/en/latest/)
- [datatest](https://datatest.readthedocs.io/en/latest/)
- [great_expectations](https://greatexpectations.io/)
- [pandas-profiling](https://pandas-profiling.github.io/pandas-profiling/docs/master/rtd/)
- [spark-df-profiling](https://github.com/julioasotodv/spark-df-profiling)


The datasets were downloaded from Kaggle ([Retail Data Analytics - Historical sales data from 45 stores](https://www.kaggle.com/manjeetsingh/retaildataset)) with some modifications. These are published in the following URLs

- [stores](https://docs.google.com/spreadsheets/d/e/2PACX-1vTuxA2NrdhAi9DDjDdOznMR1fnv1LiUhf2ztG0QqHAgc_gYK9log0XBZv0VjBB4zzFmGN0gzhD63B07/pubhtml)
- [sales](https://docs.google.com/spreadsheets/d/e/2PACX-1vRxhXER2cpZpyHf1q4Icfc7pT1WrNUR12EZvwa2FHGwuSzzgGr8uIbrtm5jyemvb6HMbfLO9JxUGgLn/pubhtml)
- [features](https://docs.google.com/spreadsheets/d/e/2PACX-1vQvWZRXlB3GMeJRnJQnylZK1G6JFH4oAg8dnNPuQITB0KHZIFO-6ku1hud6zFct3IoNpHINtY_XAiIY/pubhtml)

The data pipeline consists of the following steps:

1. Extraction of data from the web and writing them as csv files
2. Loading the data into raw spark tables and attaching tags and metadata (ingestion date and time)
3. Transforming the data: dropping columns or renaming columns, correcting data types, tagging and correcting flagged values (negative sales), and metadata tagging
4. Creating presentation data for the sales, and datascience teams which requires combining elements of these datasets

# Getting Started

Install the dependencies using `pipenv`:

```sh
pipenv install --dev
```

Activate the virtual environment:

```sh
pipenv shell
```

Scripts were developed using Python version 3.9.

# Build and Test

Command details are found in the `Makefile`.

## Quick start

Run the data pipeline

```sh
make run
```

Generate the data profiles

```sh
make profiles
```

Run data validations

```sh
make validations
```

## The long way

Rebuilding this example from scratch requires manually regenerating and editing the expectation suites. For reference, copy the `great_expectations` root directory located at `docs/great_expectations` in another folder.

Clean up:

```sh
make clean
```

Rerun the pipeline
```sh
make run
```

Run tests
```sh
make run-tests
```

Create `great_expectations` context
```sh
make ge-init
```

Add the following data sources:

```sh
cd docs
great_expectations datasource new
```

- `source_dir` located at `data/0_source` (Pandas)
- `raw_dir` located at `data/1_raw` (PySpark)
- `clean_dir` located at `data/2_clean` (PySpark)
- `present_dir` located at `data/3_present` (PySpark)

Generate the data profiles and expectation suites
```sh
make expectations
```

Edit the expectation suite JSON files located in `docs/great_expectations/expectations`by changing the `expectation_suite_name` value from `default` to more descriptive names.


Build the documentations

```sh
cd docs
great_expectations docs build
```

The great expectations (GE) documentation can be accessed in `docs/great_expectations/uncommited/data_docs/local_site/index.html`

Edit the generic expectation suits one-by one using

```sh
great_expectations suite edit <name_of_suite>
```

Run the data validations
```sh
make validations
```

# Directory Structure
- `data` - contains the extracted data, csv, and spark parquet tables
- `docs` - contains documentation generated from pytest-cov (coverage), pandas and spark dataframe profiling, and `great_expectations` root directory
- `scr` - scripts for running the data pipeline
- `tests` - tests written with pytest and datatest

```sh
├── data                                                                                     
│   ├── 0_source                                                                             
│   ├── 1_raw                                                                                
│   ├── 2_clean                                                                              
│   └── 3_present                                                                            
├── docs                                                                                     
│   ├── coverage_html_report                                                                 
│   ├── data_profiles                                                                        
│   └── great_expectations                                                                                                                               
├── Makefile                                                                                 
├── Pipfile                                                                                  
├── Pipfile.lock                                                                             
├── README.md                                                                                                                                                    
├── setup.cfg                                                                                
├── src                                                                                      
│   ├── extract                                                                              
│   ├── _includes                                                                            
│   ├── ingest                                                                               
│   ├── present                                                                              
│   ├── _profile                                                                             
│   ├── transform                                                                            
│   └── validate_data                                                                        
└── tests                                                                                    
    ├── test_extract_from_urls.py                                                            
    ├── test_ingest_data.py                                                                  
    ├── test_present_data.py                                                                 
    └── test_transform_data.py 
```

#### Attribution
[Marylette Roa](https://github.com/maryletteroa)