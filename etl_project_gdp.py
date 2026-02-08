"""
ETL_Project: Countries by GDP
Extracts GDP data from Wikipedia (archived),
transforms into clean format, loads into CSV and SQLite,
and demonstrates query for economies > 100 billion USD.
"""

import sqlite3
import logging
import pandas as pd

# ---------------- Logging Setup ----------------
logging.basicConfig(filename="etl_project_log.txt",
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

logging.info("ETL process started.")

# ---------------- Extract ----------------


url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
logging.info(f"Fetching data from {url}")

tables = pd.read_html(url)
df = tables[3]
logging.info("Initial Table extracted")


# ---------------- Transform ----------------
df.columns = ['_'.join(col) if isinstance(col, tuple) else col for col in df.columns]

# Keep only relevant columns (Country and GDP in billions)
df = df.rename(columns={
    'Country/Territory_Country/Territory': 'Country',
    'IMF[1][13]_Estimate': 'GDP_USD_billion'
})

df = df[['Country', 'GDP_USD_billion']]
df = df[df['Country'] != 'World']


# Clean GDP column (remove commas, convert to numeric)

df["GDP_USD_billion"] = df["GDP_USD_billion"].replace(r"[^\d.]", "", regex=True)

# Convert safely to float
df["GDP_USD_billion"] = pd.to_numeric(df["GDP_USD_billion"], errors="coerce")

# Drop rows where GDP is missing
df = df.dropna(subset=["GDP_USD_billion"])

logging.info(f"Final dataset rows: {len(df)}")
logging.info("Data transformed successfully.")

# Data moved to the csv file from df
df.to_csv("Countries_by_GDP.csv", index=False)

#---------------- Load-------------------------------
conn = sqlite3.connect("World_Economies.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS Countries_by_GDP")
cursor.execute("""
    CREATE TABLE Countries_by_GDP (
        Country TEXT,
        GDP_USD_billion REAL
    )
""")

df.to_sql("Countries_by_GDP", conn, if_exists="append", index=False)
conn.commit()
logging.info("Data loaded successfully.")

#---------------------Query----------------------------------------------------

query = "SELECT * FROM Countries_by_GDP WHERE GDP_USD_billion > 100"
result = pd.read_sql_query(query, conn)
print("Economies above 100 billion USD:")
print(result.to_string(index=False))


conn.close()
logging.info("Database connection closed.")



