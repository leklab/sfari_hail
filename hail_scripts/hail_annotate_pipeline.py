import pprint
import argparse

from annotate_frequencies import *
from generate_split_alleles import *
from prepare_ht_export import *
from prepare_ht_for_es import *
from export_ht_to_es import *


def run_pipeline(args):
    hl.init(log='./hail_annotation_pipeline.log')

	#Adding 'chr' prefix so to be compatible
    rg = hl.get_reference('GRCh37')
    grch37_contigs = [x for x in rg.contigs if not x.startswith('GL') and not x.startswith('M')]
    contig_dict = dict(zip(grch37_contigs, ['chr'+x for x in grch37_contigs]))

    '''
    Import VCF   
    '''

    mt = hl.import_vcf(args.vcf,reference_genome='GRCh38',contig_recoding=contig_dict,array_elements_required=False,force_bgz=True,filter='MONOALLELIC')
	
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

    #ht = hl.read_table('/home/ml2529/PCGC_dev/data/pcgc_exomes.ht')
    #export_ht_to_es(ht)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--vcf', '--input', '-i', help='bgzipped VCF file (.vcf.bgz)', required=True)
    parser.add_argument('--meta', '-m', help='Meta file containing sample population and sex', required=True)
    parser.add_argument('--out', '-o', help='Hail table output file name', required=True)

    args = parser.parse_args()
    run_pipeline(args)
