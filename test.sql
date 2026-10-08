-- In the Trino CLI/container
DROP SCHEMA IF EXISTS iceberg.demo;

CREATE SCHEMA IF NOT EXISTS iceberg.demo WITH (location='s3://warehouse/demo/');

CREATE TABLE iceberg.demo.t (id int, note varchar);
INSERT INTO iceberg.demo.t VALUES (3,'three'),(4,'four');
SELECT * FROM iceberg.demo.t;

-- docker exec -it datalake-project-trino-1 trino

SHOW SCHEMAS FROM iceberg;
CREATE SCHEMA IF NOT EXISTS iceberg.demo;
CREATE TABLE iceberg.demo.t (id int, note varchar);

select * from iceberg.silver.sales limit 100;