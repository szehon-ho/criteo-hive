-- When the partition schema and the table schema do not match in a parquet table, if the appropriate
￼-- config is set then the matching can be done by name
￼
￼SET hive.struct.schema.name.access=true;
￼
￼DROP TABLE parquet_schema_evolution_by_name;
￼DROP TABLE parquet_schema_evolution_by_name_struct;
￼
￼-- Table to test schema evolution at base level
￼CREATE TABLE parquet_schema_evolution_by_name (col0 int, col1 double)
￼PARTITIONED BY (pname string)
￼STORED AS PARQUET;
￼
￼-- Insert a first row with the original schema
￼INSERT INTO TABLE parquet_schema_evolution_by_name PARTITION (pname='1')
￼  SELECT 1, 1.0;
￼
￼-- Reorder the columns within the schema
￼ALTER TABLE parquet_schema_evolution_by_name replace columns (col1 double, col0 int);
￼INSERT INTO TABLE parquet_schema_evolution_by_name PARTITION (pname='2')
￼  SELECT 2.0, 2;
￼
￼SELECT * FROM parquet_schema_evolution_by_name;
￼
￼-- Add a new field in the middle of the schema
￼ALTER TABLE parquet_schema_evolution_by_name replace columns (col2 string, col1 double, col0 int);
￼INSERT INTO TABLE parquet_schema_evolution_by_name PARTITION (pname='3')
￼  SELECT 'three', 3.0, 3;
￼
￼SELECT * FROM parquet_schema_evolution_by_name;
￼
￼
￼-- Table to test schema evolution at struct level
￼CREATE TABLE parquet_schema_evolution_by_name_struct (f struct<col0:int,col1:double>)
￼PARTITIONED BY (pname string)
￼STORED AS PARQUET;
￼
￼-- Insert a first row with the original schema
￼INSERT INTO TABLE parquet_schema_evolution_by_name_struct PARTITION (pname='1')
￼  SELECT named_struct('col0', 1, 'col1', 1.0);
￼
￼-- Reorder the columns within the schema
￼ALTER TABLE parquet_schema_evolution_by_name_struct change column f f struct<col1:double,col0:int>;
￼INSERT INTO TABLE parquet_schema_evolution_by_name_struct PARTITION (pname='2')
￼  SELECT named_struct('col1', 2.0, 'col0', 2);
￼
￼SELECT * FROM parquet_schema_evolution_by_name_struct;
￼
￼-- Add a new field in the middle of the schema
￼ALTER TABLE parquet_schema_evolution_by_name_struct change column f f struct<col2:string,col1:double,col0:int>;
￼INSERT INTO TABLE parquet_schema_evolution_by_name_struct PARTITION (pname='3')
￼  SELECT named_struct('col2', 'three', 'col1', 3.0, 'col0', 3);
￼
￼SELECT * FROM parquet_schema_evolution_by_name_struct;
￼
￼-- Remove a field from the middle of the schema
￼ALTER TABLE parquet_schema_evolution_by_name_struct change column f f struct<col2:string,col0:int>;
￼INSERT INTO TABLE parquet_schema_evolution_by_name_struct PARTITION (pname='4')
￼  SELECT named_struct('col2', 'four', 'col0', 4);
￼
￼SELECT * FROM parquet_schema_evolution_by_name_struct;
￼
￼-- Clean up
￼DROP TABLE parquet_schema_evolution_by_name;
￼DROP TABLE parquet_schema_evolution_by_name_struct;