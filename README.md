# Hail based Pipeline for SFARI Genomics projects
Hail v0.2 pipeline used to prepare VCF file from SFARI and export into elastic search database.

## Requirements
```
Python 3.6
Java 1.8
Spark-2.4
VEP v85 (with LOFETEE plugin)
```

## Installation
Example installation on a clean Ubuntu Linux VM
```
# python and pip
sudo apt-get install python
sudo apt-get install python-pip
sudo pip install --upgrade pip

# Java 8
sudo apt-get install openjdk-8-jdk

# Spark 2.4
Downloaded from https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.6.tgz

# VEP v85
Please refer to https://github.com/Ensembl/ensembl-vep/blob/release/100/README.md

# Note after installing VEP the vep hail configuration file needs to be edited
vep85-loftee-cyan.json
vep85-loftee-ruddle-b38.json
vep85-loftee-ruddle.json
```


## High level summary of Pipeline steps
```
Step 1: Break multi-allelics to bi-allelics and use minimal representation of alleles
Step 2: Count of alleles by population
Step 3: Annotate using Variant Effect Predictor (VEP) with LOFTEE plugin
Step 4: Reformat for export to Hail Table and VCF file
Step 5: Reformat and export to Elastic Search
```


## Inputs and Outputs
**Input 1:** bgzipped VCF created by GATK. Other VCF files created by other variant callers has not been tested  
**Input 2:** Tab separated meta file. See example_meta.tsv for format.  

**Output:** Hail table - individual genotypes removed, VEP functional annotation, various summary counts/metrics, flattened structure for export to Elastic Search.

## Example Usage
```
python submit.py --run-locally ./hail_scripts/hail_annotate_pipeline.py --spark-home $SPARK_HOME --driver-memory 16G --executor-memory 8G -i input_file -m meta_file
```


## Exome VCF specific issues
1. GLNexus option used for joint calling creates instances where some PL values are missing. This is problematic for splittling multi-allelic sites.  
**Current work around:**  
Set PL values to null/empty  
DP and GQ are used to consider high quality (HQ) genotype calls for adjusted counts. Only DP is used for HQ consideration as GQ cannot not be calculated in all scenarios due to missing PL.  

2. There currently is no explicit PASS filter so any downstream filter dependent features cannot be used

3. The MONOALLELIC filter and sites are problematic after multi-allelic sites are split into multiple bi-allelic sites. The biggest issue is that alleles can be represented multiple times across VCF lines  
**Current work around:**  
Variants with the `MONOALLELIC` filter are explicitly removed

4. Hail assumes GRCh38 VCF data has the `chr` prefix. Chromosomes in this VCF does not have this prefix so the `chr` was appended on VCF import.

## Genome VCF specific issues
1. HQ genotype filter and downstream counts is not behaving correctly. This may be due to the non-standard formatting of the individual genotype field.  
**Current work around:**  
To show allelic counts without considering depth (DP)  

## Wookie mistakes
Python 3.6 is not the default python  
$SPARK_HOME is set to an older version of Spark  
Uses a version of hail contained in `./hail_builds`. Imported tables must be created by this version to avoid hail file format issues between versions  



