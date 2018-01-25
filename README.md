Tested with the virtualbox provided and followed [the benchmark specification tpc-c](www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf)

## Steps to test it

`git clone https://github.com/MarcosBernal/hbase_tpc-c_benchmark.git /home/osboxes/Desktop/HBase-API`

## Start processes
In case hbase process will not start properly or you can not access the url http://osboxes:60010, check the config files in the folder conf (git repo)

`/home/osboxes/Desktop/HBaseTestBed/hadoop-2.5.0-cdh5.3.5/sbin/hadoop-daemons.sh start namenode`
`/home/osboxes/Desktop/HBaseTestBed/hadoop-2.5.0-cdh5.3.5/sbin/hadoop-daemons.sh start datanode`

`/home/osboxes/Desktop/HBaseTestBed/hbase-0.98.6-cdh5.3.5/bin/start-hbase.sh`

`cd /home/osboxes/Desktop/` 
`wget http://lsd11.ls.fi.upm.es/CSVS.7z`

Extract the files from 7z file to store them in  /home/osboxes/Desktop/CSVS



## Test queries

`cd /home/osboxes/Desktop/HBase-API` 

`mvn clean install`

`/home/osboxes/Desktop/HBase-API/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/HBaseTPCC localhost:2181 createtables`

`/home/osboxes/Desktop/HBase-API/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/HBaseTPCC localhost:2181 loadtables "/home/osboxes/Desktop/CSVS/"`

#### Query 1 List customers of a given warehouse and district that made an order between startdate and enddate

`/home/osboxes/Desktop/HBase-API/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/HBaseTPCC localhost:2181 query1 1 1 "2013-11-29 00:00:00.000" "2013-12-05 00:00:00.000"` => Returns 54 customers

`/home/osboxes/Desktop/HBase-API/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/HBaseTPCC localhost:2181 query1 1 1 "2013-12-23 09:19:40.257" "2014-12-23 09:20:40.257"`


#### Query 2 Update a list of discounts to a customer of a given warehouse and district

`/home/osboxes/Desktop/HBase-API/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/HBaseTPCC localhost:2181 query2 3 6 8 10,15,20,50,5,10` => Ok, no return type

`/home/osboxes/Desktop/HBase-API/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/HBaseTPCC localhost:2181 query2 1 1 1 0.11,0.12,0.13,0.14,0.15,0.16,0.17`


#### Query 3 Show the lastest 4 discounts for a given customer, warehouse and district

`/home/osboxes/Desktop/HBase-API/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/HBaseTPCC localhost:2181 query3 3 6 8` => Returns [10, 5, 50, 20], following the previous query we can assume it is working as expected

`/home/osboxes/Desktop/HBase-API/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/HBaseTPCC localhost:2181 query3 1 1 1`


#### Query 4 List all the customers from a given list of districts in a specified warehouse

`/home/osboxes/Desktop/HBase-API/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/HBaseTPCC localhost:2181 query4 2 3,4,5` => Returns 9000 customers (correct according specification)

`/home/osboxes/Desktop/HBase-API/target/HBase-1.0-SNAPSHOT-bin/HBase-1.0-SNAPSHOT/bin/HBaseTPCC localhost:2181 query4 1 1,2,3`
