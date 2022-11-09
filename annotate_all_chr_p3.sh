#!/bin/bash

rm ~/hail_progress_p3.txt

for c in {16..22} X
do
    d=`date`
    echo "$d Processing: wes_70487_exome.gatk.chr${c}.vcf.gz" >> ~/hail_progress_p3.txt
    python ./hail_scripts/hail_annotate_pipeline.py \
    -i /mnt/ceph/users/info/variants/snvs_indels/WES/2022/2022_01_wes_70487_exome/gatk/pvcf/pvcfs_by_chromosome/wes_70487_exome.gatk.chr${c}.vcf.gz \
    -m /mnt/home/mlek/SPARK_70K_meta/spark_70k_meta.tsv \
    -o /mnt/home/mlek/ceph/sfari_browser_data/hail_tables/spark_exomes_70k/chr${c}.ht
done

