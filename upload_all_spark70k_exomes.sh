#!/bin/bash

rm ~/hail_progress.txt

#for c in {1..14}

for c in {11..22} X
#for c in {10..22}
#for c in {3..10}
#c=X
do
    d=`date`
    echo "$d Processing: spark_exome_70k_chr${c}" >> ~/hail_progress.txt
    python ./hail_scripts/populate_data.py -i /home/ubuntu/data/hail_tables/spark_exome_70k/chr${c}.ht
done

