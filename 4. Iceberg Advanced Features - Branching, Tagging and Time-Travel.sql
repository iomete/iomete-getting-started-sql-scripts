-- Title: Iceberg Advanced Features - Branching, Tagging and Time-Travel

-- Let's create a database and some sample tables for the queries
CREATE DATABASE IF NOT EXISTS iceberg_demo_db;

CREATE TABLE IF NOT EXISTS iceberg_demo_db.employees_mysql_external
USING org.apache.spark.sql.jdbc
OPTIONS (
    url "jdbc:mysql://iomete-tutorial.cetmtjnompsh.eu-central-1.rds.amazonaws.com:3306/employees",
    dbtable "employees.employees",
    driver 'com.mysql.cj.jdbc.Driver',
    user 'tutorial_user',
    password '9tVDVEKp'
);

CREATE TABLE iceberg_demo_db.employees
AS
SELECT *
FROM iceberg_demo_db.employees_mysql_external;
-- Schema: emp_no INT, birth_date DATE, first_name STRING, last_name STRING, gender STRING, hire_date DATE





-- ################## Branching DDL ##################

-- CREATE audit branch at the latest snapshot with default retention.
ALTER TABLE iceberg_demo_db.employees CREATE BRANCH audit;

-- CREATE audit branch at snapshot 1234 with default retention.
ALTER TABLE iceberg_demo_db.employees CREATE BRANCH audit
    AS OF VERSION 1234;

-- CREATE audit branch at snapshot 1234, retain audit branch for 30 days
ALTER TABLE iceberg_demo_db.employees CREATE BRANCH audit
    AS OF VERSION 1234 RETAIN 30 DAYS;


-- CREATE historical-tag at the latest snapshot with default retention.
ALTER TABLE iceberg_demo_db.employees CREATE TAG historical_tag;

-- CREATE historical-tag at snapshot 1234 with default retention.
ALTER TABLE iceberg_demo_db.employees CREATE TAG historical_tag
    AS OF VERSION 1234;

-- CREATE historical_tag at snapshot 1234, retain historical_tag for 365 days
ALTER TABLE iceberg_demo_db.employees CREATE TAG historical_tag
    AS OF VERSION 1234 RETAIN 365 DAYS;


-- Replace branch audit's version to 1234 and retention to 60 days.
ALTER TABLE iceberg_demo_db.employees REPLACE BRANCH audit
    AS OF VERSION 1234 RETAIN 60 DAYS;

-- Replace branch audit's version to the latest and retention to 60 days.
ALTER TABLE iceberg_demo_db.employees REPLACE BRANCH audit
    RETAIN 60 DAYS;

-- Drop branch audit.
ALTER TABLE iceberg_demo_db.employees DROP BRANCH audit;

-- Drop tag historical_tag.
ALTER TABLE iceberg_demo_db.employees DROP TAG historical_tag;


-- ################## Branching Queries ##################

-- Prepare branch and tag for the queries.
ALTER TABLE iceberg_demo_db.employees CREATE BRANCH audit_branch;
ALTER TABLE iceberg_demo_db.employees CREATE TAG historical_tag;

-- Query the snapshots, their timestamps, and their IDs.
select * from iceberg_demo_db.employees.history;

-- Query branches and tags.
SELECT * FROM iceberg_demo_db.employees.refs;

-- time travel to October 26, 1986 at 01:21:00
SELECT * FROM iceberg_demo_db.employees TIMESTAMP AS OF '1986-10-26 01:21:00';

-- Timestamps can also be supplied as a Unix timestamp, in seconds:
SELECT * FROM iceberg_demo_db.employees TIMESTAMP AS OF 499162860;

-- time travel to snapshot with id 10963874102873L
SELECT * FROM iceberg_demo_db.employees VERSION AS OF 10963874102873;

-- time travel to the head snapshot of audit_branch
SELECT * FROM iceberg_demo_db.employees VERSION AS OF 'audit_branch' LIMIT 100;

-- You can use this syntax as well: <db_name>.<table_name>.branch_<branch_name>
SELECT * FROM iceberg_demo_db.employees.branch_audit_branch LIMIT 100;

-- time travel to the snapshot referenced by the tag historical_tag
SELECT * FROM iceberg_demo_db.employees VERSION AS OF 'historical_tag' LIMIT 100;

-- You can use this syntax as well: <db_name>.<table_name>.tag_<tag_name>
SELECT * FROM iceberg_demo_db.employees.tag_historical_tag LIMIT 100;





-- ################## Writing to Branches ##################

ALTER TABLE iceberg_demo_db.employees CREATE BRANCH branch_audit;

-- INSERT into `audit_branch`. The main branch stays unchanged.
INSERT INTO iceberg_demo_db.employees.branch_audit_branch (
    emp_no,
    first_name,
    last_name,
    gender,
    birth_date,
    hire_date
)
VALUES (
    1,
    'John-Branched',
    'Doe',
    'M',
    CAST('2000-10-01' AS DATE),
    CAST('2000-10-01' AS DATE)
);

-- Main branch doesn't return any row for emp_no=1
SELECT * FROM iceberg_demo_db.employees WHERE emp_no=1;

-- Branch branch_audit returns the inserted row for emp_no=1
SELECT * FROM iceberg_demo_db.employees.branch_audit_branch WHERE emp_no=1;


-- Branch write is supported for INSERT, UPDATE, DELETE, and MERGE INTO.

-- MERGE INTO iceberg_demo_db.employees.branch_audit_branch t

-- UPDATE iceberg_demo_db.employees.branch_audit_branch AS t1 ...

-- DELETE FROM iceberg_demo_db.employees.branch_audit_branch WHERE emp_no = 2;


-- #### Clean up ####
DROP TABLE iceberg_demo_db.employees PURGE;
DROP TABLE iceberg_demo_db.employees_mysql_external;

DROP DATABASE iceberg_demo_db;
