# Benchmarking OLTP vs OLAP

## Setup
1. git clone the repo
2. Install [Docker](https://www.docker.com/get-started/)
3. Check if docker compose comes installed with Docker by running `docker compose version`
4. Install [docker compose](https://docs.docker.com/compose/install/)
5. Run the docker componse

```
git clone https://github.com/jkwd/oltp_vs_olap.git
docker compose version
docker compose build
docker compose up
```

## Data Source
We are using the [TPC-H](https://www.tpc.org/tpch/) data which is a decision support benchmark. It consists of a suite of business-oriented ad hoc queries and concurrent data modifications.

## Experiment
We are going to benchmark the performance of an Online Transaction Processing (OLTP) database vs an Online analytical processing (OLAP) database on business-oriented query performance as well as transactional performance:
1. business-oriented ad hoc query by [TPC-H](https://docs.starrocks.io/docs/benchmarking/TPC-H_Benchmarking/#5-query-sql-and-create-table-statements)
2. INSERT N records

The OLTP database we are using for the experiment is a Postgres database. The OLAP database we are using is DuckDB.

## Hypothesis
The hypothesis is that the OLTP database will perform better for transactional operations such as INSERT/UPDATE/DELETE while the OLAP database will perform better for the business-oriented ad hoc query.

## Result
### Query performance
![](./img/query_time.png)

DuckDB executed all the queries in less than a second. Postgres on the other hand took at least 3 seconds for the query execution to complete. In fact 1 of the query took longer than 50 seconds!

### INSERT performance
![](./img/insert_time.png)
Postgres is about 4X faster when inserting records as compared to DuckDB.