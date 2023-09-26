# KPIs-project-2023

This project is intendend to automate getting KPIs from some web pages.

## Setup

Install poetry (Linux)
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

Install poetry (Windows)
```bash
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
```

Install Dependencies
```bash
poetry install
```

## Execution
```bash
poetry run python KPIs/app.py
```
