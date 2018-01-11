# https://www.youtube.com/watch?v=a6tB07ndUgg&index=41&list=PLf0swTFhTI8pronNK7Gm-isKX7tdNb0Go

# create database tables in textfile format under hive
create database ushapotturu_retail_db_txt;
use ushapotturu_retail_db_txt;


create table orders(
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
  ) row format delimited fields terminated by ',' 
  stored as textfile ;

# to see the table structure
describe orders;

# to see the metadata about the table especially location
describe formatted orders;

# load data into table locally
load data local inpath '/home/ushapotturu/retail_db/orders' into table orders;
select * from orders limit 10;

# create order_items table
create table order_items(
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity int,
  order_item_subtotal float,
  order_item_product_price float
  ) row format delimited fields terminated by ',' 
  stored as textfile ;

# to see the table structure
describe order_items;

# load data into table locally
load data local inpath '/home/ushapotturu/retail_db/order_items' into table order_items;
select * from order_items limit 10;
#load data from hdfs
LOAD DATA INPATH ‘hdfs:/data/2012.txt’ INTO TABLE order_items;

#create customer table
create table customers (
  customer_id int,
  customer_fname varchar(45),
  customer_lname varchar(45),
  customer_email varchar(45),
  customer_password varchar(45),
  customer_street varchar(45),
  customer_city varchar(45),
  customer_state varchar(45),
  customer_zipcode varchar(45)
  ) row format delimited fields terminated by ',' 
  stored as textfile ;

load data local inpath '/home/ushapotturu/retail_db/customers' into table customers;
  
#create datatabase,tables in orc format
create database ushapotturu_retail_db_orc;
use ushapotturu_retail_db_orc;

create table orders(
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
  ) stored as orc ;
# its 2 step process (stage the data first put it in textfile format next put it in orc format)
insert into table orders select * from ushapotturu_retail_db_txt.orders;

select * from orders limit 10;
# create order_items table
create table order_items(
  order_item_id int,
  order_item_order_id int,
  order_item_product_id int,
  order_item_quantity int,
  order_item_subtotal float,
  order_item_product_price float
  ) stored as orc ;

insert into table order_items select * from ushapotturu_retail_db_txt.order_items;
 
select * from order_items limit 10;


#Running hive quries from the terminal : 
pyspark
sqlContext.sql("use ushapotturu_retail_db_txt")
sqlContext.sql("show tables").show()

# to get metadata of table
sqlContext.sql("describe formatted orders").show()

for i in sqlContext.sql("describe formatted orders").collect(): print(i)

# to run a query to see the results
sqlContext.sql("select * from orders limit 10").show()


# spark SQL Functions
# Running Hive Queries -filtering (horizontal and vertical) , functions
# to kmow the fuctions under hive  - show functions;
sqlContext.sql("show functions").show() # view only 20 records
# to know the semantic or syntax of a function in hive - decsribe function length;
sqlContext.sql("describe function length").show()
# to test a function - select length('hello again');
sqlContext.sql("select length('hello again')").show()
# to see order_status length from orders table then - select length(order_status) from orders limit 100;
sqlContext.sql("select length(order_status) from orders").show()
# to see order_status,length of order_status length from orders table then - select order_status,length(order_status) from orders limit 100;
sqlContext.sql("select order_status,length(order_status) from orders ").show()

# important string functions - substr ot substring , instr, like, rlike, length , ucase or upper , trim/ltrim/rtrim ,lpad/rpad , cast
# regular expressions - 
decsribe function substring;
select trim('  hello  world');
select trim('  hello  world'),length( trim('  hello  world'));

# padding
describe function lpad;
select lpad(2,3,'0'); # prefix string 2 with two zeros  to make 3 digit length
select lpad(12,12,'0');# prefix string 12 with ten zeros (length is 12)
select "12"; # 12 is a string
select cast("12" as int); # convert string into integer
# extract o0..................................................................................rder_date and convert into int from  ordertable 
select order_date from orders limit 10;
select substr(order_date, 6 ,2) from orders limit 10;
select cast(substr(order_date, 6 ,2) as int) from orders limit 10;
# last element in the string
select index(split("Hello world How are you?", " "),4); 

# Aggregations count,mean,sum,max,
# to get number of records in a table 
select count(1) from orders;
select sum(order_item_subtotal) from order_items;

select count(1), count(distinct order_status) from orders;

# Manipulating Dates current_date, current_timestamp,date_add,date_format,date_sub, dateddiff, day, dayofmonth, to_date, to_unix_timestamp
#to_utc_timestamp, from_unixtime, from_utc_timestamp, minute, month, months_between , next_day

select current_date;
# extract year from date
select date_format(current_date,'y');
# extract day
select date_format(current_date,'d');
# to convert datetime stamp into date
select to_date(current_timestamp);

# convert datetime into unixtimestamp # output -1512968400
select to_unix_timestamp(current_date);

#convert unixtimestamp into current date time
select from_unix_timestamp(1512968400);

#convert unixtimestamp into current date 
select to_date(from_unix_timestamp(1512968400)); 

# from order_date extract date and view from orders 
select to_date(order_date) from orders limit 10;

# to add 10 days to the day
select to_date(order_date) from orders limit 10;

# case 
describe function case;
# CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END - When a = b, returns c; when a = d, return e; else return f

select distinct order_status from orders;

# if order_status is 'CLOSED' or 'COMPLETE' then 'No Action'
select  order_status,
   case order_status
     when 'CLOSED' then 'NO Action'
     when 'COMPLETE' then  'No Action'
     when 'ON_HOLD' then  'Pending Action'
     when 'PAYMENT_REVIEW' then  'Pending Action'
     when 'PENDING_REVIEW' then  'Pending Action'
     when 'PENDING_PAYMENT' then  'Pending Action'
     else 'Risky'
   end from orders limit 10;
	
#

select order_status,
 case
    when order_status IN ('CLOSED', 'COMPLETE') THEN 'NO Action'
    when order_status IN ('ON_HOLD' ,'PAYMENT_REVIEW','PENDING','PENDING_PAYMENT','PROCESSING') then 'Pending Action'
    else 'Risky'
 end from orders limit 10;
	 
#nvl - if you want to replace null values with 'Status misssing' 
select nvl(order_status, 'Status Missing') from orders limit 100;

select case when order_status is null then 'Status Missing' else order_status end from orders limit 100;

# row level transformations
#data standardization - phone numbers, address(if data is coming from different sources then it has to be standarsized)
#data cleanzing - by removing special characters from sprcific columns
#data offcazation - want to store SSN as it is instead store last 4 digits (in other words store data what you need)

select * from orders limit 10;
#extract year and month from order_date
select concat(substr(order_date, 1, 4),' ',substr(order_date, 6, 2)) from orders limit 10;
# convert into int
select cast(concat(substr(order_date, 1, 4), ' ' ,substr(order_date, 6, 2)) as int) from orders limit 10;

select date_format(order_date, 'y'),date_format(order_date,'d') from orders limit 10;

# joining data from multiple tables
# view customers who are placing orders
select * from orders limit 10;
select o.*, c.* from orders o, customers c where o.order_customer_id = c.customer_id limit 10;

# use this instead inner join (default)
select o.*, c.* from orders o join customers c on o.order_customer_id = c.customer_id limit 10;

#left outerjoin
select o.*, c.* from customers c left outer join orders o on o.order_customer_id = c.customer_id limit 10;

# to cross verify		 
select count(1) from orders o join customers c on o.order_customer_id = c.customer_id ;

#left outerjoin
select count(1) from customers c left outer join orders o on o.order_customer_id = c.customer_id;

#
select * from customers c left outer join orders o on o.order_customer_id = c.customer_id where o.order_customer_id is null;

# not in is that efficient when we compare with left join
select * from customers where customer_id not in (select distinct order_customer_id from orders)

# Aggregations
select count(2) from orders;

select order_status ,count(1) as Sum from orders group by order_status;

select o.order_id, o.order_date, sum(oi.order_item_subtotal) order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_id
group by o.order_id;


select o.order_id,o.order_status,o.order_date, sum(oi.order_item_subtotal) order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_id
where o.order_status in('COMPLETE','CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 100;

# revenue for each day level
select o.order_date,
round(sum(order_item_subtotal),2) daily_revenue, o.order_status
from orders o join order_items oi
on o.order_id = oi.order_item_id
where o.order_status in('COMPLETE','CLOSED')
group by o.order_date, o.order_status limit 100;

#sorting to control the data use :order by
# globally sorted by order by
select o.order_id,o.order_status,o.order_date, sum(oi.order_item_subtotal) order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_id
where o.order_status in('COMPLETE','CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 100
order by o.order_date, order_revenue desc;

# not globally sorted -more efficinet in performance
select o.order_id,o.order_status,o.order_date, sum(oi.order_item_subtotal) order_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in('COMPLETE','CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 100
distribute by o.order_date sort by o.order_date, order_revenue desc;

#set operations : union and union all
# done on two datasets which are similar
# set operations require multiple queries
select 1, "Hello";
select 2, "World ";
select 1, "Hello";
select 1, "world";

select 1, "Hello"
union 
select 2, "World "
union
select 1, "hello"
union
select 1, "world";


# Analytics Functions- rank, row_number, dense_rank, cume_dist, percent_rank , ntile
select o.order_id,o.order_status,o.order_date, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal),2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal),2) Average
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in('COMPLETE','CLOSED')
group by o.order_id, o.order_date, o.order_status
having sum(oi.order_item_subtotal) >= 100
order by o.order_date,  order_revenue desc;
# error
		
select o.order_id,o.order_status,o.order_date, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id) ,2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id),2)
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in('COMPLETE','CLOSED')
order by o.order_date, order_revenue desc;

# to apply where condition use nested query because where cann't be applied to derived columns like order_revenue above(in nested queries we can use derived columns like below)
select * from (
select o.order_id,o.order_status,o.order_date, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id) ,2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id),2)
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in('COMPLETE','CLOSED') ) q
where order_revenue >= 1000 
order by order_date, order_revenue desc limit 100;

# window functions : normally used in time series analysis - lead, lag, first_value, last_value
select * from (
select o.order_id,o.order_status,o.order_date, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id) ,2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id),2)
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in('COMPLETE','CLOSED') ) q
where order_revenue >= 1000 
order by order_date, order_revenue desc;

#ranking

select * from (
select o.order_id,o.order_status,o.order_date, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id) ,2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id),2)pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in('COMPLETE','CLOSED') ) q
where order_revenuw >= 1000 
order by order_date, order_revenue desc;

select * from (
select o.order_id,o.order_status,o.order_date, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id) ,2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id),2)pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue,
rank() over (partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
dense_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
percent_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
row_number() over (partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
row_number() over (partition by o.order_id ) rn_revenue
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in('COMPLETE','CLOSED') ) q
where order_revenue >= 1000 
order by order_date, order_revenue desc,rnk_revenue limit 100;


#lead ,lag
 select * from (
select o.order_id,o.order_status,o.order_date, oi.order_item_subtotal,
round(sum(oi.order_item_subtotal) over (partition by o.order_id) ,2) order_revenue,
oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id),2)pct_revenue,
round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue,
rank() over (partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
dense_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
percent_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
row_number() over (partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
row_number() over (partition by o.order_id ) rn_revenue,
lead(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc ) 
lead_order_item_subtotal,
lag(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc )
 lag_order_item_subtotal,
first_value(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc ) 
first_order_item_subtotal,
last_value(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc ) 
last_order_item_subtotal
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in('COMPLETE','CLOSED') ) q
where order_revenue >= 1000 
order by order_date, order_revenue desc,rnk_revenue limit 100;


# create dataframe and register Temp table
#problem stmt
get daily revenue by product considering completed and closed orders.
	PRODUCTS have to be read from local file system. Dataframe need to be created
	join ORDERS, ORDER_ITEMS
	filter on ORDER_STATUS
data need to be sorted by ascending order by date and then descending order by revenue computed for each product for each day.
	sort data by order_date in ascending order and then daily revenue per product in descending order
	
# use pyspark
pyspark --master yarn --conf spark.ui.port=12562 --executor-memory 2G --num-executors 1
sqlContext.sql("use ushapotturu_retail_db_txt")

sqlContext.sql("select * from ushapotturu_retail_db_txt.orders").show()
sqlContext.sql("select * from ushapotturu_retail_db_txt.orders").printSchema()

from pyspark.sql import Row
# read data from hdfs
ordersRDD = sc.textFile("/user/ushapotturu/data/retail_db/orders")
for i in ordersRDD.take(50):print(i)

# convert RDD into DF
ordersDF = ordersRDD.map(lambda o: Row(int(o.split(",")[0]), o.split(",")[1], int(o.split(",")[2]), o.split(",")[3])).toDF()
# see the data inDF
ordersDF.show()
# give the column names
ordersDF = ordersRDD.map(lambda o: Row(order_id=int(o.split(",")[0]), order_date= o.split(",")[1], order_customer = int(o.split(",")[2]), order_status = o.split(",")[3])).toDF()
ordersDF.show()

#to create temp table
ordersDF.registerTempTable("ordersDF")
# now write queries on this temp table
sqlContext.sql("select order_status, count(1) as count from ordersDF group by order_status").show()

# to get data from Local file system
productsRaw = open("/home/ushapotturu/data/retail_db/products/part-00000").read().splitlines()

#convert into RDD
productsRDD = sc.parallelize(productsRaw)
for i in productsRDD.take(10): print(i)

# extract product_id and product_name and convert into DF
productsDF = productsRDD.map(lambda p: Row(product_id = int(p.split(",")[0]), products_name = p.split(",")[2] )).toDF()
#to create temp table
productsDF.registerTempTable("productsDF")
# now write queries on this temp table
sqlContext.sql("show tables").show()
sqlContext.sql("select * from productsDF").show()
sqlContext.sql("select * from orders").show()
sqlContext.sql("select * from order_items").show()

# to set no of tasks
sqlContext.setConf("spark.sql.shuffle.partitions", "2")
sqlContext.sql("select o.order_date, p.products_name, sum(oi.order_item_subtotal) daily_revenue_per_product \
 from orders o join order_items oi \
 ON o.order_id = oi.order_item_order_id \
 JOIN productsDF p \
 ON p.product_id = oi.order_item_product_id \
 WHERE o.order_status IN ('COMPLETE','CLOSED') \
 GROUP BY o.order_date, p.products_name \
 ORDER BY o.order_date, daily_revenue_per_product DESC ").show()

# how to save output to HIVE
sqlContext.sql("CREATE DATABASE ushapotturu_daily_revenue") 
# create table to store data
sqlContext.sql("CREATE TABLE ushapotturu_daily_revenue.daily_revenue (order_date string, product_name string, daily_revenue_per_product float) STORED AS orc")

daily_revenue_per_product_df = sqlContext.sql("select * from orders o join order_items oi\
ON o.order_id = oi.order_item_order_id \
JOIN productsDF p \
ON p.product_id = oi.order_item_product_id \
WHERE o.order_status IN ('COMPLETE','CLOSED') \
GROUP BY o.order_date, p.products_name \
ORDER BY o.order_date, daily_revenue_per_product DESC ")
# save into HIVE
daily_revenue_per_product_df.insertInto("daily_revenue")
# or 
daily_revenue_per_product_df.write.orc("daily_revenue")

# Dataframe operations in pyspark
help(daily_revenue_per_product_df)
# use api on df
daily_revenue_per_product_df.printSchema()
# save data 
# create table and load data
daily_revenue_per_product_df.saveAsTable(<table name>)
#show 100 records
daily_revenue_per_product_df.show(100);
#save file as json
daily_revenue_per_product_df.save("/user/ushapotturu/daily_revenue_save","json")
# save file json
daily_revenue_per_product_df.write.json("/user/ushapotturu/daily_revenue_write")

#check in hive
hadoop fs -ls /user/ushapotturu/daily_revenue_save
# file size
hadoop fs -ls -h /user/ushapotturu/daily_revenue_write

# to perform data operations
daily_revenue_per_product_df.select("order_date","daily_revenue_per_product").show()
help(daily_revenue_per_product_df.filter)


pyspark --master yarn\
  --conf spark.ui.port=12345 \
  --num-executors 6 \
  --executor-cores 2 \
  --executor-memory 2G
  
