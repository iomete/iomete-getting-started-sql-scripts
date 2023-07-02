-- Title: Common queries and DML operations (IOMETE SparkSQL and Iceberg)


-- Let's create a database and some sample tables for the queries
CREATE DATABASE IF NOT EXISTS common_queries_demo_db;

CREATE TABLE IF NOT EXISTS common_queries_demo_db.employees_mysql_external
USING org.apache.spark.sql.jdbc
OPTIONS (
    url "jdbc:mysql://iomete-tutorial.cetmtjnompsh.eu-central-1.rds.amazonaws.com:3306/employees",
    dbtable "employees.employees",
    driver 'com.mysql.cj.jdbc.Driver',
    user 'tutorial_user',
    password '9tVDVEKp'
);

CREATE TABLE common_queries_demo_db.employees
AS
SELECT *
FROM common_queries_demo_db.employees_mysql_external;
-- Schema: emp_no INT, birth_date DATE, first_name STRING, last_name STRING, gender STRING, hire_date DATE


-- ################## Common queries ##################

-- Show tables in the given database
SHOW TABLES in common_queries_demo_db;

-- Describe table
DESCRIBE common_queries_demo_db.employees;
DESCRIBE EXTENDED common_queries_demo_db.employees; -- extended description

-- Show table properties
SHOW TBLPROPERTIES common_queries_demo_db.employees;

-- Show table schema
SHOW CREATE TABLE common_queries_demo_db.employees;

-- Inspect table history (Iceberg)
SELECT * FROM common_queries_demo_db.employees.history;

-- Inspect table snapshots (Iceberg)
SELECT * FROM common_queries_demo_db.employees.snapshots;

-- Show table's data files and each file's metadata (Iceberg)
SELECT * FROM common_queries_demo_db.employees.files;

-- Show table's file manifests and each file's metadata (Iceberg)
SELECT * FROM common_queries_demo_db.employees.manifests;



-- ################## INSERT INTO Operations ##################

-- INSERT INTO operation
INSERT INTO common_queries_demo_db.employees
VALUES (
    1,
    CAST('01.10.2000' AS DATE),
    'John',
    'Doe',
    'M',
    CAST('01.10.2000' AS DATE)
);

-- You can also specify the columns in any order. Note: You must specify all columns in the table.
INSERT INTO common_queries_demo_db.employees (
    emp_no, first_name, last_name, gender, birth_date, hire_date
)
VALUES (
    1,
    'John',
    'Doe',
    'M',
    CAST('2000-10-01' AS DATE),
    CAST('2000-10-01' AS DATE)
);

-- Using a SELECT query to insert data
INSERT INTO common_queries_demo_db.employees
SELECT 1 AS id,
       TO_DATE('2000-10-01', 'yyyy-MM-dd') AS birth_date,
       'John' AS first_name,
       'Doe' AS last_name,
       'M' AS gender,
       TO_DATE('2022-01-01', 'yyyy-MM-dd') AS hire_date;


-- Inserting into a partitioned table requires data to be sorted by the partition columns
CREATE TABLE common_queries_demo_db.employees_partitioned (
    emp_no int,
    birth_date date,
    first_name string,
    last_name string,
    gender string
) PARTITIONED BY (gender);

INSERT INTO common_queries_demo_db.employees_partitioned
SELECT emp_no,
       birth_date,
       first_name,
       last_name,
       gender
FROM common_queries_demo_db.employees
ORDER BY gender; -- Note: ORDER BY is required for partitioned columns. They must be sorted.


-- ################## MERGE/Update/Delete Operations ##################

-- MERGE INTO operation. See: https://iomete.com/docs/iceberg-tables/writes#merge-into-syntax
MERGE INTO spark_catalog.common_queries_demo_db.employees AS t
USING (
    SELECT
        1 AS emp_no,
        'John Doe' AS first_name,
        'Doe' AS last_name,
        'M' AS gender,
        DATE('2022-01-01') AS hire_date,
        DATE('1990-01-01') AS birth_date
) AS s
ON t.emp_no = s.emp_no
WHEN MATCHED THEN
    UPDATE SET t.first_name = s.first_name,
               t.last_name = s.last_name,
               t.gender = s.gender,
               t.hire_date = s.hire_date,
               t.birth_date = s.birth_date
WHEN NOT MATCHED THEN
    INSERT (emp_no, birth_date, first_name, last_name, gender, hire_date)
    VALUES (s.emp_no, s.birth_date, s.first_name, s.last_name, s.gender, s.hire_date);

-- UPDATE operation
UPDATE common_queries_demo_db.employees
    SET first_name = 'Max', last_name = 'Doe'
WHERE emp_no = 1;


-- DELETE FROM operation
DELETE FROM common_queries_demo_db.employees WHERE emp_no = 1;

-- #### Clean up ####
DROP TABLE common_queries_demo_db.employees PURGE;
DROP TABLE common_queries_demo_db.employees_partitioned PURGE;
DROP TABLE common_queries_demo_db.employees_mysql_external;

DROP DATABASE common_queries_demo_db;