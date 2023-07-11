# Auto-ETL

Prerequisite

- Python 3.11
- Poetry

# Steps to configure the repo locally

- Clone the repo and cd into src directory
- Run cmd -> `poetry config virtualenvs.in-project true`
- To install the project dependencies and create the virtual env run -> `poetry install`
- To activate the env -> `poetry shell`
- Now you can run the main script by running -> python src/auto_etl.py -t query-builder -m tests/Auto_ETL_Metadata_Mapping_V1.xlsx -c tests/sample_config.json
