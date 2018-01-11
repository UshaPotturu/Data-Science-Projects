#Aggregating data

# hive
show databases;
use retail_db;
show tables;
describe order_items;

describe region;

ls -ltr 
# to see the files under specific path
hadoop fs -ls  /user/ushapotturu

#to copy files from one location to another in HDFS use cp 
# to copy files from local file system to hdfs use -copyFromLocal  or put

hadoop fs -help copyFromLocal

# to create directory
hadoop fs -mkdir data

# cross verify hadoop 
hadoop fs -ls
whoami
# check for any files existing under data
hadoop fs -ls /user/ushapotturu/data

# put will have less properties.
hadoop fs -put /home/cloudera/data.txt  /user/ushapotturu/.

# to preserve the properties
hadoop fs -put -f -p /home/cloudera/data.txt  /user/ushapotturu/.
# check the permisiions it will be same as source


#copyToLocal copy 
hadoop fs -help copyToLocal
# trying to copy files from hadoop  to local file system
# check for files under data to copy into local system
hadoop fs -ls /user/ushapotturu/data

hadoop fs -get /user/ushapotturu/data/data* /home/cloudera/data
ls -ltr data

# if you want same properties like source then use -p. If the file is alredy exists then remove and use -p 
hadoop fs -get -p /user/ushapotturu/data/data* /home/cloudera/data

# to check hidden fiels 
ls -altr

# remove data.txt
rm data.txt

# to move file from local to hadoop
hadoop fs -moveFromLocal /home/cloudera/data.txt /user/ushapotturu

 
HiveContext
 
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
data = sqlContext.sql("select * from departments") 
for i in data.collect(): print(i)