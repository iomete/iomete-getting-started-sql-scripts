-- Title: Common DDL Operations

-- Create database
CREATE DATABASE IF NOT EXISTS ddl_ops_demo_db;

-- Show tables in the given database
SHOW TABLES in ddl_ops_demo_db;

-- See: https://iomete.com/docs/iceberg-tables/ddl

-- Create a table. Default format is Iceberg.
CREATE TABLE ddl_ops_demo_db.sample(id bigint COMMENT 'unique id', data string, ts timestamp);

-- Both create table statements are equivalent
CREATE TABLE ddl_ops_demo_db.sample(id bigint COMMENT 'unique id', data string, ts timestamp) using iceberg;

-- Describe table
DESCRIBE EXTENDED ddl_ops_demo_db.sample;



-- Create a partitioned table
CREATE TABLE ddl_ops_demo_db.sample_partitioned (
        id bigint,
        data string,
        category string,
        ts timestamp
    ) PARTITIONED BY (days(ts), category);

DESCRIBE EXTENDED ddl_ops_demo_db.sample_partitioned;



-- Create external table. See: https://iomete.com/docs/data-sources/jdbc-sources
CREATE TABLE IF NOT EXISTS ddl_ops_demo_db.employees_mysql_external
    USING org.apache.spark.sql.jdbc
    OPTIONS (
                url "jdbc:mysql://iomete-tutorial.cetmtjnompsh.eu-central-1.rds.amazonaws.com:3306/employees",
                dbtable "employees.employees",
                driver 'com.mysql.cj.jdbc.Driver',
                user 'tutorial_user',
                password '9tVDVEKp'
);

DESCRIBE EXTENDED ddl_ops_demo_db.employees_mysql_external;


-- CTAS (Create Table As Select)
CREATE TABLE ddl_ops_demo_db.employees_iceberg_table
AS
SELECT *
FROM ddl_ops_demo_db.employees_mysql_external;

DESCRIBE EXTENDED ddl_ops_demo_db.employees_iceberg_table;

-- RTAS (Replace Table As Select)
-- Atomic table replacement creates a new snapshot with the results of the SELECT query, but keeps table history.
CREATE OR REPLACE TABLE ddl_ops_demo_db.employees_iceberg_table
AS
SELECT *
FROM ddl_ops_demo_db.employees_mysql_external;


-- Alter table name
ALTER TABLE ddl_ops_demo_db.employees_iceberg_table RENAME TO employees;

DESCRIBE EXTENDED ddl_ops_demo_db.employees;

-- Alter table properties (set/unset)
ALTER TABLE ddl_ops_demo_db.sample SET TBLPROPERTIES ('read.split.target-size'='268435456');

ALTER TABLE ddl_ops_demo_db.sample UNSET TBLPROPERTIES ('read.split.target-size');

DESCRIBE EXTENDED ddl_ops_demo_db.sample;

-- Add columns (metadata only operations)
ALTER TABLE ddl_ops_demo_db.sample ADD COLUMNS (
    new_column1 string comment 'new_column docs',
    new_column2 int
    );

-- Rename column (metadata only operations)
ALTER TABLE ddl_ops_demo_db.sample RENAME COLUMN data TO payload;

-- Change column type (metadata only operations). Allowed conversions: int -> bigint, float -> double, decimal(P,S) to decimal(P2,S) when P2 > P (scale cannot change)
ALTER TABLE ddl_ops_demo_db.sample ALTER COLUMN new_column2 TYPE bigint;

-- Drop columns (metadata only operations)
ALTER TABLE ddl_ops_demo_db.sample DROP COLUMN id;

-- Add partition field (metadata only operations)
ALTER TABLE ddl_ops_demo_db.sample ADD PARTITION FIELD years(ts);

-- Drop partition field (metadata only operations)
ALTER TABLE ddl_ops_demo_db.sample DROP PARTITION FIELD years(ts);

-- Drop Iceberg table (delete table and data from storage. Pay attention to the PURGE option)
DROP TABLE ddl_ops_demo_db.sample PURGE;

-- Drop Iceberg table (metadata only operations - doesn't delete data from storage)
DROP TABLE ddl_ops_demo_db.sample;

-- Drop Iceberg table if exists
DROP TABLE IF EXISTS ddl_ops_demo_db.sample PURGE;


-- #### Clean up ####
DROP TABLE ddl_ops_demo_db.sample PURGE;
DROP TABLE ddl_ops_demo_db.sample_partitioned PURGE;
DROP TABLE ddl_ops_demo_db.employees_mysql_external;
DROP TABLE ddl_ops_demo_db.employees PURGE;

DROP DATABASE ddl_ops_demo_db;