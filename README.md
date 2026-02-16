How to run:

1. Create virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file:

```
PGHOST=localhost
PGPORT=5432
PGDATABASE=nba
PGUSER=postgres
PGPASSWORD=your_password
```

---

## Run Single Question

```bash
python pipeline.py --mode one --model_dir ./baseline_model
```

---

## Run Evaluation

```bash
python pipeline.py --mode eval --model_dir ./baseline_model --csv test_data.csv
```
