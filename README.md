# BETTY PROJECT 2023

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

Execute
```bash
poetry run python KPIs/app.py
```