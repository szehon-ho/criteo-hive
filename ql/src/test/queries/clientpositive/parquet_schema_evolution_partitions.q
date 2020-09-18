SET hive.metastore.disallow.incompatible.col.type.changes=false;

DROP TABLE parquet_schema_evolution_partitions;
DROP TABLE parquet_schema_evolution_partitions_output;
DROP TABLE parquet_schema_evolution_partitions_struct;

-- Table to test schema evolution at base level
CREATE TABLE parquet_schema_evolution_partitions (col0 int, col1 array<int>)
PARTITIONED BY (pname string)
STORED AS PARQUET;

-- Insert a first row with the original schema
INSERT INTO TABLE parquet_schema_evolution_partitions PARTITION (pname='1')
  SELECT 1, array(0, 1);

SELECT * FROM parquet_schema_evolution_partitions;

-- Reorder the columns within the schema
ALTER TABLE parquet_schema_evolution_partitions replace columns (col1 array<int>, col0 int);
INSERT INTO TABLE parquet_schema_evolution_partitions PARTITION (pname='2')
  SELECT array(2, 3), 2;

SELECT * FROM parquet_schema_evolution_partitions;

-- Add a new field in the middle of the schema
ALTER TABLE parquet_schema_evolution_partitions replace columns (col2 string, col1 array<int>, col0 int);
INSERT INTO TABLE parquet_schema_evolution_partitions PARTITION (pname='3')
  SELECT 'three', array(4, 5, 6), 3;

SELECT * FROM parquet_schema_evolution_partitions;

-- Change some types to a compatible supertype
ALTER TABLE parquet_schema_evolution_partitions replace columns (col2 string, col1 array<bigint>, col0 bigint);
INSERT INTO TABLE parquet_schema_evolution_partitions PARTITION (pname='4')
  SELECT 'four', array(cast(7 as bigint), 8, 9), 4;


-- Create a table to insert into to check there is no input/output confusion
CREATE TABLE parquet_schema_evolution_partitions_output (col0 string, col1 bigint, col2 bigint)
STORED AS PARQUET;

INSERT INTO parquet_schema_evolution_partitions_output
SELECT col2 AS col0, col1[0] AS col1, col0 as col2
FROM parquet_schema_evolution_partitions;

SELECT * FROM parquet_schema_evolution_partitions_output;

-- Table to test schema evolution at struct level
CREATE TABLE parquet_schema_evolution_partitions_struct (f struct<col0:int,col1:double>)
PARTITIONED BY (pname string)
STORED AS PARQUET;

-- Insert a first row with the original schema
INSERT INTO TABLE parquet_schema_evolution_partitions_struct PARTITION (pname='1')
  SELECT named_struct('col0', 1, 'col1', cast(1.0 as double));

-- Reorder the columns within the schema
ALTER TABLE parquet_schema_evolution_partitions_struct change column f f struct<col1:double,col0:int>;
INSERT INTO TABLE parquet_schema_evolution_partitions_struct PARTITION (pname='2')
  SELECT named_struct('col1', cast(2.0 as double), 'col0', 2);

SELECT * FROM parquet_schema_evolution_partitions_struct;

-- Add a new field in the middle of the schema
ALTER TABLE parquet_schema_evolution_partitions_struct change column f f struct<col2:string,col1:double,col0:int>;
INSERT INTO TABLE parquet_schema_evolution_partitions_struct PARTITION (pname='3')
  SELECT named_struct('col2', 'three', 'col1', cast(3.0 as double), 'col0', 3);

SELECT * FROM parquet_schema_evolution_partitions_struct;

-- Remove a field from the middle of the schema
ALTER TABLE parquet_schema_evolution_partitions_struct change column f f struct<col2:string,col0:int>;
INSERT INTO TABLE parquet_schema_evolution_partitions_struct PARTITION (pname='4')
  SELECT named_struct('col2', 'four', 'col0', 4);

SELECT * FROM parquet_schema_evolution_partitions_struct;

-- Select from both table in a single MR job to make sure mappers reading a table do not use the schema of the other
SELECT t1.*, t2.f
FROM parquet_schema_evolution_partitions t1
JOIN parquet_schema_evolution_partitions_struct t2
  ON t1.pname = t2.pname;

-- Clean up
DROP TABLE parquet_schema_evolution_partitions;
DROP TABLE parquet_schema_evolution_partitions_output;
DROP TABLE parquet_schema_evolution_partitions_struct;
