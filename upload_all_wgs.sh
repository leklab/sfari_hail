#!/bin/bash

rm ~/hail_progress.txt

for c in {1..22} X
do
    d=`date`
    echo "$d Processing: SPARK_WGS_1.chr${c}.JoinCalling.exome.ht" >> ~/hail_progress.txt
    python submit.py --run-locally hail_scripts/populate_wgs_data.py \
    --spark-home /home/ubuntu/bin/spark-2.4.3-bin-hadoop2.7 \
    --cpu-limit 4 --driver-memory 16G --executor-memory 8G \
    -i ~/data/hail_tables/hail_tables_spark_wgs/SPARK_WGS_1.chr${c}.JoinCalling.exome.ht
done

