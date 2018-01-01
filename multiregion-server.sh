#! /bin/bash

cd ~/Desktop/HBaseTestBed/

./hadoop-2.5.0-cdh5.3.5/sbin/hadoop-daemon.sh start namenode

./hadoop-2.5.0-cdh5.3.5/sbin/hadoop-daemon.sh start datanode

./zookeeper-3.4.5-cdh5.3.5/bin/zkServer.sh start 

screen -dmS hbase-master bash -c "./hbase-0.98.6-cdh5.3.5/bin/hbase --config conf master start"
screen -dmS hbase-rs1 bash -c "./hbase-0.98.6-cdh5.3.5/bin/hbase --config confRS1 regionserver start"
screen -dmS hbase-rs2 bash -c "./hbase-0.98.6-cdh5.3.5/bin/hbase --config confRS2 regionserver start"
screen -dmS hbase-rs3 bash -c "./hbase-0.98.6-cdh5.3.5/bin/hbase --config confRS3 regionserver start"