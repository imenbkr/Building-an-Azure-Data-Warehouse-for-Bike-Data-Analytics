# Building an Azure Data Warehouse for Bike-Share Analytics

End-to-end data engineering project: bike-share data is loaded into an **Azure Database for PostgreSQL** (OLTP), moved to **Azure Data Lake Storage Gen2** with a **Synapse** copy pipeline, exposed as **external tables**, and transformed into a **star schema** in a **dedicated SQL pool** for analytics (OLAP).

## Architecture

```mermaid
flowchart LR
    CSV[CSV files<br/>riders, stations, trips, payments] -->|data_to_postgres.py| PG[(Azure Database<br/>for PostgreSQL)]
    PG -->|Synapse Copy pipeline| ADLS[(Azure Data Lake<br/>Storage Gen2)]
    ADLS -->|External tables| EXT[Staging<br/>external tables]
    EXT -->|CTAS / INSERT SELECT| DW[(Dedicated SQL pool<br/>star schema)]
```

**Tech stack:** Azure Database for PostgreSQL (Flexible Server), Azure Synapse Analytics, Azure Data Lake Storage Gen2, Synapse pipelines (Copy Data tool), T-SQL, Python (psycopg2), DBeaver.

## Star schema

| Table | Type | Columns |
|---|---|---|
| `fact_trip` | Fact | trip_id, trip_type, started_at, ended_at, start_station_id, end_station_id, rider_id, duration_trip |
| `fact_payment` | Fact | payment_id, payment_date, payment_amount, account_number |
| `dim_rider` | Dimension | account_number, first_name, last_name, address, birthdate, rider_age, is_member, account_start_date, account_end_date |
| `dim_station` | Dimension | station_id, station_name, station_latitude, station_longitude |
| `Calendar` | Date dimension | date, day, month, week, quarter, year, first of month/year, ... |

`duration_trip` and `rider_age` are derived columns computed during the transformation.

## Repository structure

| Path | Content |
|---|---|
| `data_to_postgres.py` | Creates the `dwh_analytics` database and loads the CSV files into PostgreSQL |
| `pipeline/`, `dataset/`, `linkedService/` | Synapse copy pipeline (PostgreSQL → Data Lake) and its connections |
| `sqlscript/` | T-SQL scripts: external tables (staging) and star-schema tables |
| `docs/images/` | Screenshots of each step |

## Steps

### 1. Create the Azure resources

All resources live in one resource group: a PostgreSQL flexible server, a Data Lake Storage account and a Synapse workspace.

![Resource group](docs/images/resource-group.png)

![PostgreSQL deployment](docs/images/postgres-deployment.png)

Register the `Microsoft.Synapse` and `Microsoft.Storage` resource providers on the subscription before creating the workspace and data lake:

![Register Synapse](docs/images/register-synapse.png)

![Register Storage](docs/images/register-storage.png)

### 2. Load the source data into PostgreSQL

Connect to the server from VS Code, then run `data_to_postgres.py`. It reads the connection settings from a local `.env` file, creates the `dwh_analytics` database and loads the four CSV files.

![Connect from VS Code](docs/images/azure-connect-vscode.png)

![VS Code PostgreSQL connection](docs/images/vscode-postgres-connection.png)

![Load data to PostgreSQL](docs/images/load-data-to-postgres.png)

Check the tables with DBeaver:

![DBeaver payment table](docs/images/dbeaver-payment-table.png)

### 3. Move the data to the Data Lake

In Synapse Studio, **Ingest → Copy Data tool**: source is Azure Database for PostgreSQL, destination is Azure Data Lake Storage Gen2.

![Copy Data tool](docs/images/copy-data-tool.png)

![Synapse PostgreSQL connection](docs/images/synapse-postgres-connection.png)

![Copy deployment complete](docs/images/copy-deployment-complete.png)

![Pipeline runs](docs/images/pipeline-runs.png)

The four tables land as CSV files in the data lake file system:

![Data lake files](docs/images/datalake-files.png)

### 4. Load the files into external tables

Create a **dedicated SQL pool**. Tables created in the serverless pool are not visible from the dedicated pool, so all tables are created in the dedicated one.

![Dedicated SQL pool](docs/images/dedicated-sql-pool.png)

Create an external table for each file (**New SQL script → Create external table**):

![Create external table menu](docs/images/create-external-table-menu.png)

![New external table](docs/images/new-external-table.png)

![External table payment](docs/images/external-table-payment.png)

![External table trip](docs/images/external-table-trip.png)

### 5. Transform into the star schema

The fact and dimension tables are created with `CREATE TABLE ... WITH (DISTRIBUTION = HASH(...))` and filled from the external tables with type conversions (`TRY_CAST`) and derived columns:
- Fact tables use a clustered columnstore index.
- Dimension tables use a heap.

![Fact payment](docs/images/fact-payment.png)

![Star schema tables](docs/images/star-schema-tables.png)

### 6. Pause the dedicated SQL pool

A dedicated SQL pool is billed per hour while online, so pause it when you're not using it.

![Pause SQL pool](docs/images/pause-sql-pool.png)

## Credits

Based on the [FaslaTech tutorial series](https://www.youtube.com/watch?v=RdiOhhHIAmI&list=PLYaPThD6DUS-5B6WXX-SwG29J4Y3xHbQK).
