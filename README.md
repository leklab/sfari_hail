# sfari_hail
Hail v0.2 pipeline used to VCF file from SFARI for export into elastic search.

## Requirements
```
Python 3.6
Java 1.8
Spark-2.4
VEP v85 (with LOFETEE plugin)
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
```
Input 1: bgzipped VCF created by GATK. Other VCF files created by other variant callers has not been tested
Input 2: Tab separated meta file. See example_meta.tsv for format.

Output: Hail table - individual genotypes removed, VEP functional annotation, various summary counts/metrics, flattened structure for export to Elastic Search.
```

## Example Usage
```
python submit.py --run-locally ./hail_scripts/hail_annotate_pipeline.py --spark-home $SPARK_HOME --driver-memory 16G --executor-memory 8G -i input_file -m meta_file
```


## Exome specific issues
1. GLNexus option used for joint calling creates instances where some PL values are missing. This is problematic for splittling multi-allelic sites. 
Current work around 
```
Set PL values to null/empty
DP and GQ are used to consider high quality genotype calls for adjusted counts. Only DP is used for HQ consideration as GQ cannot not be calculated in all scenarios due to missing PL.
```
2. There currently is no explicit PASS filter so any downstream filter dependent features cannot be used
3. The MONOALLELIC filter and sites are problematic after multi-allelic sites are split into multiple bi-allelic sites. The biggest issue is that alleles can be represented multiple times across VCF lines
Current work around
```
Variants with the MONOALLELIC filter are explicitly removed
```

## Wookie mistakes
Python 3.6 is not the default python 
$SPARK_HOME is set to an older version of Spark 
Uses a version of hail contained in `./hail_builds`. Imported tables must be created by this version to avoid hail file format issues between versions 



