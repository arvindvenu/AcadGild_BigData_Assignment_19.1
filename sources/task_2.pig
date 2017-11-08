/*
Load the customer table from hbase.
The row key will be loaded to the id field and 
the columns of the details column family will be loaded 
into a map named details
*/
customers = LOAD 'hbase://customer' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('details:*', '-loadKey true') as (id:CHARARRAY, details:MAP[]);

/*
project the id and the keys of the details map into a flat relation and also 
define the types explicitly
*/
customers_projected = FOREACH customers GENERATE id as cust_id,details#'name'as name:chararray,details#'location' as location:chararray,details#'age' as age:int;

/*
group all the tuples in customers_projected bag 
into a single tuple to calculate the min and max
values
*/
cust_grouped = GROUP customers_projected ALL;

/*
calculate the min and max of the set and also 
flatten the customers_projected inner bag
*/
cust_min_max_data = FOREACH cust_grouped GENERATE MAX(customers_projected.age) as max_age:int,MIN(customers_projected.age) as min_age:int, FLATTEN(customers_projected);

-- filter out those tuple whose age = min_age
cust_min = FILTER cust_min_max_data BY age == min_age;

-- filter out those tuple whose age = max_age
cust_max = FILTER cust_min_max_data BY age == max_age;

/*
add an extra field to indicate whether this tuple is min or max.
This will be useful in the next step to identify whether a row is 
min or max because we will be doing a union of both these tuples to display 
it as a single result
*/
cust_min_with_indicator = FOREACH cust_min GENERATE cust_id,name,location,age,'min';
cust_max_with_indicator = FOREACH cust_max GENERATE cust_id,name,location,age,'max';

-- perform a union of the min tuple and the max tuple
cust_min_max = UNION cust_min_with_indicator,cust_max_with_indicator;

-- store the result in the local file system
STORE cust_min_max INTO 'file:///home/arvind/hbase/acadgild/assignments/assignment_19.1/output/task_2';
