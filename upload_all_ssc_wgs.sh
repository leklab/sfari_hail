#!/bin/bash

rm ~/hail_progress.txt

#for c in {1..14}

#for c in {10..22} X
#for c in {10..22}
#for c in {1..8}
c=X
#do
    d=`date`
    echo "$d Processing: CCDG_9000JG_B01_GRM_WGS_2019-03-21_chr${c}.exome.ht" >> ~/hail_progress.txt
    python submit.py --run-locally hail_scripts/populate_wgs_data.py \
    --spark-home /home/ubuntu/bin/spark-2.4.3-bin-hadoop2.7 \
    --cpu-limit 4 --driver-memory 16G --executor-memory 8G \
    -i ~/data/hail_tables/hail_tables_ssc_wgs2/CCDG_9000JG_B01_GRM_WGS_2019-03-21_chr${c}.exome.ht
#done

