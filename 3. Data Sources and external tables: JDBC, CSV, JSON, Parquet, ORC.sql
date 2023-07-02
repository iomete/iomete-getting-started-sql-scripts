-- Title: Data Sources and external tables: JDBC, CSV, JSON, Parquet, ORC


CREATE DATABASE IF NOT EXISTS data_sources_demo_db;


-- ################## JDBC Sources ##################

-- Create a table from a JDBC source (MySQL). See: https://iomete.com/docs/data-sources/jdbc-sources
CREATE TABLE IF NOT EXISTS data_sources_demo_db.employees_mysql_external
USING org.apache.spark.sql.jdbc
OPTIONS (
    url "jdbc:mysql://iomete-tutorial.cetmtjnompsh.eu-central-1.rds.amazonaws.com:3306/employees",
    dbtable "employees.employees",
    driver 'com.mysql.cj.jdbc.Driver',
    user 'tutorial_user',
    password '9tVDVEKp'
);

-- This will read data from the mysql table. Filters and other operations will be pushed down to the source.
SELECT * FROM data_sources_demo_db.employees_mysql_external LIMIT 10;

-- Similarly, you can read/write from other JDBC sources such as PostgreSQL, Oracle, SQL Server, etc.



-- ################## CSV Files ##################

-- GCP: gs://iomete-examples/sample-data/csv/employees.csv
-- AWS: s3a://iomete-examples/sample-data/csv/employees.csv

-- Read CSV file from the Cloud Storage. See: https://iomete.com/docs/data-sources/csv-files
SELECT *
FROM csv.`gs://iomete-examples/sample-data/csv/employees.csv`
LIMIT 100;

-- Create table allows to provide additional options such as header, inferSchema, etc. See: https://iomete.com/docs/data-sources/csv-files
CREATE table data_sources_demo_db.employees_csv_external
USING csv
OPTIONS (
  header "true", -- first row is header information
  inferSchema "true", -- automatically infer data types
  path "gs://iomete-examples/sample-data/csv/employees.csv"
);

-- Check the table schema and inferred data types.
DESC EXTENDED data_sources_demo_db.employees_csv_external;

-- Read data from the table (CSV file).
SELECT * FROM data_sources_demo_db.employees_csv_external LIMIT 100;


-- To export data to a CSV file, you can use the following syntax.
-- It will write employees data to the specified path in CSV format.
CREATE TABLE data_sources_demo_db.tmp_csv_external_write
    USING csv
    OPTIONS (path "gs://path/to/employees.csv")
AS SELECT * FROM data_sources_demo_db.employees;

-- You can drop temporary table after the export. It will not delete the CSV file.
DROP TABLE data_sources_demo_db.tmp_csv_external_write;



-- ################## JSON Files ##################

-- GCP: gs://iomete-examples/sample-data/json/employees.json
-- AWS: s3a://iomete-examples/sample-data/json/employees.json

-- Read JSON file from the Cloud Storage. See: https://iomete.com/docs/data-sources/json-files
SELECT  * FROM json.`gs://iomete-examples/sample-data/json/employees.json` LIMIT 100;

CREATE TABLE data_sources_demo_db.employees_json_external
    USING org.apache.spark.sql.json
    OPTIONS (
        path "gs://iomete-examples/sample-data/json/employees.json"
    );

SELECT * FROM data_sources_demo_db.employees_json_external LIMIT 100;


-- To export data to a JSON file, you can use the following syntax.
-- It will write employees data to the specified path in JSON format.
CREATE TABLE data_sources_demo_db.tmp_json_external_write
    USING org.apache.spark.sql.json
    OPTIONS (path "gs://path/to/employees.json")
    AS SELECT * FROM data_sources_demo_db.employees;

-- You can drop temporary table after the export. It will not delete the JSON file.
DROP TABLE data_sources_demo_db.tmp_json_external_write;



-- ################## Parquet Files ##################

-- GCP: gs://iomete-examples/sample-data/parquet/employees.parquet
-- AWS: s3a://iomete-examples/sample-data/parquet/employees.parquet

-- Read Parquet file from the Cloud Storage. See: https://iomete.com/docs/data-sources/parquet-files
SELECT  * FROM parquet.`gs://iomete-examples/sample-data/parquet/employees.parquet` LIMIT 100;

CREATE TABLE data_sources_demo_db.employees_parquet_external
    USING org.apache.spark.sql.parquet
    OPTIONS (
    path "gs://iomete-examples/sample-data/parquet/employees.parquet"
);

SELECT * FROM data_sources_demo_db.employees_parquet_external LIMIT 100;


-- To export data to a Parquet file, you can use the following syntax.
-- It will write employees data to the specified path in Parquet format.
CREATE TABLE data_sources_demo_db.tmp_parquet_external_write
    USING org.apache.spark.sql.parquet
    OPTIONS (path "gs://path/to/employees.parquet")
    AS SELECT * FROM data_sources_demo_db.employees;

-- You can drop temporary table after the export. It will not delete the Parquet file.
DROP TABLE data_sources_demo_db.tmp_parquet_external_write;



-- ################## ORC Files ##################

-- GCP: gs://iomete-examples/sample-data/orc/employees.orc
-- AWS: s3a://iomete-examples/sample-data/orc/employees.orc

-- Read ORC file from the Cloud Storage. See: https://iomete.com/docs/data-sources/orc-files
SELECT  * FROM orc.`gs://iomete-examples/sample-data/orc/employees.orc` LIMIT 100;

CREATE TABLE data_sources_demo_db.employees_orc_external
    USING orc
    OPTIONS (
    path "gs://iomete-examples/sample-data/orc/employees.orc"
);

SELECT * FROM data_sources_demo_db.employees_orc_external LIMIT 100;


-- To export data to a ORC file, you can use the following syntax.
-- It will write employees data to the specified path in ORC format.
CREATE TABLE data_sources_demo_db.tmp_orc_external_write
    USING orc
    OPTIONS (path "gs://path/to/employees.orc")
    AS SELECT * FROM data_sources_demo_db.employees;

-- You can drop temporary table after the export. It will not delete the ORC file.
DROP TABLE data_sources_demo_db.tmp_orc_external_write;



-- #### Clean up ####
DROP DATABASE data_sources_demo_db CASCADE;
