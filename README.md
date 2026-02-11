# Toronto Blue Jays – Data Engineer Technical Exercise

This repository contains my solution to the Toronto Blue Jays Data Engineer technical exercise. The project ingests MLB game schedule and play-by-play data, loads it into a PostgreSQL database, and answers a series of analytical questions using SQL.

The solution is organized into schema definitions, a reproducible ETL pipeline written in Python, and standalone SQL query files.

## Part 1 – Table Definitions

### Where to find table definitions

All database table definitions are located in: sql/create_tables.sql

## Part 2 – Transform and Load Data (ETL)

### Where to find the code

The ETL logic for Part 2 is implemented in: scripts/run_etl.py

### How to run the ETL

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/run_etl.py --db-uri "<YOUR_DB_URI>"
```

## Part 3 – Database Queries

### Where to find the queries

All SQL queries for Part 3 are located in: sql/sql_questions.sql

### Answers to the questions in Part 3

3a) 
- Milwaukee Brewers
- Philadelphia Phillies
- Toronto Blue Jays
- New York Yankees
- Los Angeles Dodgers

3b) 
- José Caballero
- Chandler Simpson
- José Ramírez
- Bobby Witt Jr.
- Juan Soto
- Oneil Cruz
- Elly De La Cruz
- Trea Turner
- Pete Crow-Armstrong

3c)
- Elly De La Cruz
- George Springer
- Ernie Clement
- Maikel Garcia
- Mookie Betts
- Aaron Judge
- Fernando Tatis Jr.
- TJ Friedl
- Brice Turang
- CJ Abrams

3d) 
- gamepk: 776919
- team name: Colorado Rockies

3e)
- Toronto Blue Jays
- Los Angeles Dodgers
- Seattle Mariners
- Philadelphia Phillies
- Kansas City Royals

3f)
- Tyler Tolbert
- Kyren Paris
- Jakob Marsee
- José Caballero
- Nasim Nuñez
- Leody Taveras
- Chandler Simpson
- Luis Robert Jr.
- Oneil Cruz
- David Hamilton

(Reasoning explained in sql file)