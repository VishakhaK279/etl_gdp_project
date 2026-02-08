# GDP ETL Project

## Overview
This project demonstrates a complete ETL (Extract, Transform, Load) pipeline using global GDP data.  
Data is extracted from an archived Wikipedia page, cleaned and transformed, then loaded into both CSV and SQLite.  
Finally, SQL queries are run to showcase insights (e.g., economies above 100 billion USD).

## Features
- Extract GDP data from Wikipedia tables using `pandas.read_html`
- Handle messy multi-index headers and clean numeric values
- Transform into a structured DataFrame with `Country` and `GDP_USD_billion`
- Load results into:
  - CSV file (`Countries_by_GDP.csv`)
  - SQLite database (`World_Economies.db`)
- Run SQL queries to filter economies above thresholds
- Logging of each step for reproducibility

## Requirements
- Python 3.x
- Libraries: `pandas`, `sqlite3`, `logging`

Install dependencies:
```bash
pip install pandas
