-- drop the table customer_hive if it exists
drop table if exists customer_hive;

-- create an external table customer_hive if it exists
-- load the data from the hbase table specified in hbase.table.name
-- the column mappings between the hbase table and the hive table are as 
-- mentioned in the hbase.columns.mapping property. The mappings are specified
-- in the same order in which the columns have been declared
-- 'details:' indicates this column is part of the column family named 'details'
create external table customer_hive
(
	cust_id String,
	age int,
	location string,
	name string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties
("hbase.columns.mapping"=":key,details:age,details:location,details:name")
tblproperties("hbase.table.name"="customer");

-- select all rows whose age is the same as minimum age.
-- select all rows whose age is the same as maximum age.
-- an extra column min_max in the two result sets indicate 
-- whether this row is the min value or the max value
-- union of the above two rows will give both min and max in one result set
-- store the result in a local directory
INSERT OVERWRITE LOCAL DIRECTORY '/home/arvind/hbase/acadgild/assignments/assignment_19.1/output/task_1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
select cust_id,age,location,name,'min' as min_max from customer_hive where 
age IN (select MIN(age) as min_age from customer_hive)
UNION
select cust_id,age,location,name,'max' as min_max from customer_hive where 
age IN (select MAX(age) as max_age from customer_hive);

