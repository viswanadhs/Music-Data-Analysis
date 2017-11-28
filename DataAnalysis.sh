#!/bin/bash

batchid=`cat /home/acadgild/project/logs/current-batch.txt`

LOGFILE=/home/acadgild/project/logs/log_batch_$batchid

echo "Running script for data analysis using spark..." >> $LOGFILE
chmod 775 /home/acadgild/project/Spark_Practice-1/out/artifacts/Spark_practice_1_jar/Spark_practice-1.jar

zip -d /home/acadgild/project/Spark_Practice-1/out/artifacts/Spark_practice_1_jar/Spark_practice-1.jar META-INF/*.DSA META-INF/*.RSA META-INF/*.SF

spark-submit    \
--class Spark_analysis \
--master local[2] \
--driver-class-path /usr/local/hive/lib/hive-hbase-handler-0.14.0.jar:/usr/local/hbase/lib/* \
/home/acadgild/project/Spark_Practice-1/out/artifacts/Spark_practice_1_jar/Spark_practice-1.jar $batchid

spark-submit    \
--class Spark_analysis_2 \
--master local[2] \
--driver-class-path /usr/local/hive/lib/hive-hbase-handler-0.14.0.jar:/usr/local/hbase/lib/* \
/home/acadgild/project/Spark_Practice-1/out/artifacts/Spark_practice_1_jar/Spark_practice-1.jar $batchid

echo "Exporting data to MYSQL using sqoop export..." >> $LOGFILE
sh /home/acadgild/project/scripts/data_export.sh

echo "Incrementing batchid..." >> $LOGFILE
batchid=`expr $batchid + 1`
echo -n $batchid > /home/acadgild/project/logs/current-batch.txt


