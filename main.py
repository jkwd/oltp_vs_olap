import os
import duckdb
import pandas as pd
import time
from sqlalchemy import create_engine, text
import numpy as np
import matplotlib.pyplot as plt


##################################################
# INITIALIZATION OF DUCKDB AND POSTGRES DATABASE #
##################################################

# Init DuckDB
conn = duckdb.connect('db.duckdb')

# Tables
tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']

# Clean up the database
print("Cleaning up the database in DuckDB")
for table in tables:
    conn.execute(f"DROP TABLE IF EXISTS {table};")

# Init TPC-H
print("Installing TPC-H extension and loading TPC-H data in DuckDB")
conn.execute("INSTALL tpch;")
conn.execute("LOAD tpch;")

# Generate the tables
# https://duckdb.org/docs/extensions/tpch.html#pre-generated-data-sets
conn.execute("CALL dbgen(sf = 1);")

# Export the tables into parquet files in data folder
print("Exporting tables in DuckDB to parquet files to load into Postgres")
# Check if data folder exists
if not os.path.exists('data'):
    os.makedirs('data')

for table in tables:
    conn.execute(f"COPY {table} TO 'data/{table}.parquet' (FORMAT PARQUET);")


# Init Postgres attachment from DuckDB
# https://duckdb.org/docs/extensions/postgres.html
conn.execute("INSTALL postgres;")
conn.execute("LOAD postgres;")

# Attach the Postgres database
conn.execute("ATTACH 'host=postgres_db user=postgres password=postgres dbname=postgres' AS postgres_db (TYPE POSTGRES, SCHEMA 'public');")

# Copy the tables from DuckDB to Postgres
print("Creating tables in Postgres. May take awhile...")
for table in tables:
    print("Copy table in Postgres", table)
    conn.execute(f"TRUNCATE postgres_db.{table};")
    conn.execute(f"COPY postgres_db.{table} FROM './data/{table}.parquet';")

# Detach Postgres from DuckDB
conn.execute("DETACH postgres_db;")

# Create connection string
conn_str = "postgresql+psycopg2://postgres:postgres@postgres_db:5432/postgres"

# # Create engine
engine = create_engine(conn_str)
postgres_conn = engine.connect()

print("Checking number of records in Duckdb vs Postgres")
for table in tables:
    print(f"Number of records in DuckDB {table}: ", conn.execute(f"SELECT COUNT(1) FROM {table}").fetchall())
    print(f"Number of records in Postgres {table}: ", postgres_conn.execute(text(f"SELECT COUNT(1) FROM {table}")).fetchall())


##################################################
# EXPERIMENT: TIME TAKEN TO EXECUTE THE QUERIES  #
##################################################
# https://docs.starrocks.io/docs/benchmarking/TPC-H_Benchmarking/#5-query-sql-and-create-table-statements

# Experiment DuckDB
print("Running experiment for DuckDB - Time taken to execute the TPC-H queries")
duckdb_times = []
for i in range(1, 6):
    with open(f'./sql/q{i}.sql', 'r') as file:
        sql_query = file.read()
        sql_query = sql_query

    # start time
    start_time = time.time()

    # Execute the query
    conn.execute(sql_query)

    # end time
    end_time = time.time()

    # Get the time difference
    time_diff = end_time - start_time
    duckdb_times.append(time_diff)
    print(f"Time taken to execute the query {i} in DuckDB:", time_diff)


# Experiment Postgres
print("Running experiment for Postgres - Time taken to execute the TPC-H queries")
postgres_times = []
for i in range(1, 6):
    with open(f'./sql/q{i}.sql', 'r') as file:
        sql_query = file.read()
        sql_query = text(sql_query)

    # start time
    start_time = time.time()

    # Execute the query
    postgres_conn.execute(sql_query)

    # end time
    end_time = time.time()

    # Get the time difference
    time_diff = end_time - start_time
    postgres_times.append(time_diff)
    print(f"Time taken to execute the query {i} in Postgres:", time_diff)

print("DuckDB times:", duckdb_times)
print("Postgres times:", postgres_times)

plt.figure()
X = ["q1", "q2", "q3", "q4", "q5"]
X_axis = np.arange(len(X))

plt.barh(X_axis - 0.2, duckdb_times, 0.4, label = 'DuckDB')
plt.barh(X_axis + 0.2, postgres_times, 0.4, label = 'Postgres')

for i in range(5):
    value = round(duckdb_times[i], 2)
    plt.text(value, i - 0.2, str(value))
    
    value = round(postgres_times[i], 2)
    plt.text(value, i + 0.2, str(value))

plt.xlim(0, max(postgres_times) + 10)
plt.xlabel("Time (seconds)") 
plt.yticks(X_axis, X)
plt.ylabel("Query") 
plt.title("Query time for DuckDB and Postgres") 
plt.legend()

plt.savefig('img/query_time.png')
plt.close()

##################################################
# EXPERIMENT: TIME TAKEN TO INSERT RECORDS      #
##################################################
print("Prep experiment - Time taken to insert records")

# Create a copy of the lineitem table in DuckDB
conn.execute("DROP TABLE IF EXISTS lineitem_copy;")
conn.execute("CREATE TABLE lineitem_copy as SELECT * FROM lineitem LIMIT 0;")
conn.execute("SELECT * FROM lineitem_copy;")

# Truncate lineitem table in Postgres
postgres_conn.execute(text("TRUNCATE lineitem_copy;"))

# Create INSERT INTO statements for each record
insert_statements = []
N = 10000
df = pd.read_parquet('data/lineitem.parquet')
for index, row in df.iterrows():
    insert_statement = f"""
        INSERT INTO lineitem_copy (
            l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
            l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
        ) VALUES (
            {row['l_orderkey']}, {row['l_partkey']}, {row['l_suppkey']}, {row['l_linenumber']}, {row['l_quantity']}, {row['l_extendedprice']}, {row['l_discount']}, {row['l_tax']},
            '{row['l_returnflag']}', '{row['l_linestatus']}', '{row['l_shipdate']}', '{row['l_commitdate']}', '{row['l_receiptdate']}', '{row['l_shipinstruct']}', '{row['l_shipmode']}', '{row['l_comment']}'
        );
    """
    insert_statements.append(insert_statement)
    
    if index == N:
        break

# Insert records into the lineitem_copy table in DuckDB
print("Running experiment for DuckDB - Time taken to insert records")
start_time = time.time()
for insert_statement in insert_statements:
    conn.execute(insert_statement)
end_time = time.time()
duckdb_insert_time = end_time - start_time
print(f"Time taken to insert {N} records in DuckDB:", duckdb_insert_time)

# Insert records into the lineitem_copy table in Postgres
print("Running experiment for Postgres - Time taken to insert records")
start_time = time.time()
for insert_statement in insert_statements:
    postgres_conn.execute(text(insert_statement))
end_time = time.time()
postgres_insert_time = end_time - start_time
print(f"Time taken to insert {N} records in Postgres:", postgres_insert_time)

X_insert = ["duckdb", "postgres"]
insert_times = [duckdb_insert_time, postgres_insert_time]

plt.figure()
plt.bar(X_insert, insert_times) 

plt.ylabel("Time (seconds)") 
plt.title(f"Insert time for {N} records")

plt.savefig('img/insert_time.png')