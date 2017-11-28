#!/bin/bash

if [ -f "/home/acadgild/project/logs/current-batch.txt" ]

then 

echo "Batch File Found!"

else 

echo -n "1" > "/home/acadgild/project/logs/current-batch.txt"

fi

chmod 775 /home/acadgild/project/logs/current-batch.txt
batchid=`cat /home/acadgild/project/logs/current-batch.txt`
LOGFILE=/home/acadgild/project/logs/log_batch_$batchid

echo "Starting daemons" >> $LOGFILE

sh /usr/local/hadoop-2.6.0/sbin/start-all.sh
sh /usr/local/hbase/bin/start-hbase.sh




