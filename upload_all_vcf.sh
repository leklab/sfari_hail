#!/bin/bash

rm ~/hail_progress.txt

for c in {3..22} X
do
    d=`date`
    echo "$d Processing: 20190729_Merged-GATK4+GLnexus_cleaned_Dec62019_chr_${c}.ht" >> ~/hail_progress.txt
    python submit.py --run-locally hail_scripts/populate_data.py \
    --spark-home /home/ubuntu/bin/spark-2.4.3-bin-hadoop2.7 \
    --cpu-limit 4 --driver-memory 16G --executor-memory 8G \
    -i ~/data/20190729_Merged-GATK4+GLnexus_cleaned_Dec62019_chr_${c}.ht
done

