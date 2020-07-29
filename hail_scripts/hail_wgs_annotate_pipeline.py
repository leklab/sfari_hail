import pprint
import argparse

from annotate_frequencies import *
from generate_split_alleles import *
from prepare_ht_export import *
from prepare_ht_for_es import *
from export_ht_to_es import *


def run_pipeline(args):
    hl.init(log='./hail_annotation_pipeline.log')

    mt = hl.import_vcf(args.vcf,reference_genome='GRCh38',array_elements_required=False,force_bgz=True,filter='MONOALLELIC')

    #exome_intervals = hl.import_locus_intervals('/gpfs/ycga/project/lek/shared/resources/hg38/exome_evaluation_regions.v1.interval_list', 
    #                    reference_genome='GRCh38')

    gene_intervals = hl.import_locus_intervals('/home/ml2529/gencode.v32.genes.merged.bed', reference_genome='GRCh38')

    #filter to only exome intervals
    #mt = mt.filter_rows(hl.is_defined(exome_intervals[mt.locus]))

    #filter to only gene intervals    
    mt = mt.filter_rows(hl.is_defined(gene_intervals[mt.locus]))
    
    #repartition as data set has a lot less rows
    mt = mt.repartition(hl.eval(hl.int(mt.n_partitions()/10)))

    #mt.write(args.out,overwrite=True)


    #Split alleles
    mt = generate_split_alleles(mt)

    #Annotate Population frequencies for now
    meta_ht = hl.import_table(args.meta,delimiter='\t',key='ID')
    ht = annotate_frequencies(mt,meta_ht)

    #VEP Annotate the Hail table (ie. sites-only) using GRCh38 configuration file
    ht = hl.vep(ht, 'vep85-loftee-ruddle-b38.json')

    #Reformatting for Elasticsearch export
    ht = prepare_ht_export(ht)
    ht = prepare_ht_for_es(ht)

    ht.write(args.out,overwrite=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--vcf', '--input', '-i', help='bgzipped VCF file (.vcf.bgz)', required=True)
    parser.add_argument('--out', '-o', help='Hail table output file name', required=True)
    parser.add_argument('--meta', '-m', help='Meta file containing sample population and sex', required=True)

    args = parser.parse_args()
    run_pipeline(args)



