import pprint
import argparse
import hail as hl

from annotate_frequencies import *
from generate_split_alleles import *
from prepare_ht_export import *
from prepare_ht_for_es import *
from export_ht_to_es import *

from utils import get_expr_for_vep_sorted_transcript_consequences_array

def run_pipeline(args):
    hl.init(log='./hail_annotation_pipeline.log')

    '''
    mt = hl.read_matrix_table('/gpfs/ycga/project/gruber/nsd35/hail_DeNovo/PCGC11.mt')

    pprint.pprint(mt.count_cols()) # output: 1100
    pprint.pprint(mt.count_rows()) # output: 1493771

    mt = generate_split_alleles(mt)

    pprint.pprint(mt.count_cols()) 
    pprint.pprint(mt.count_rows()) 

    table = (hl.import_table('/gpfs/ycga/project/gruber/nsd35/hail_DeNovo/IDs_keep.txt', impute=True).key_by('Sample'))
    mt = mt.annotate_cols(should_retain = table[mt.s].should_retain)
    mt = mt.filter_cols(mt.should_retain == 'yes', keep=True)

    pprint.pprint(mt.count_cols()) 
    pprint.pprint(mt.count_rows()) 

    mt = mt.annotate_rows(gt_stats = hl.agg.call_stats(mt.GT, mt.alleles))
    mt = mt.filter_rows(mt.row.gt_stats.AC[1] == 0, keep=False)

    pprint.pprint(mt.count_cols()) 
    pprint.pprint(mt.count_rows()) 

    mt.write('pcgc11_subset.mt',overwrite=True)    
    '''

    '''
    mt = hl.read_matrix_table('pcgc11_subset.mt')
    mt = hl.vep(mt, 'vep85-loftee-ruddle.json')    
    mt.write('pcgc11_subset_vep.mt',overwrite=True)
    '''


    
    #mt = hl.read_matrix_table('pcgc11_subset.mt')
    #pprint.pprint(mt.count_cols()) 
    #pprint.pprint(mt.count_rows()) 
    #pprint.pprint(mt.describe())
    #pprint.pprint(mt.show(include_row_fields=True))
    #pprint.pprint(mt.row_key)

    mt = hl.read_matrix_table('pcgc11_subset_vep.mt')
    #pprint.pprint(mt_vep.count_cols()) 
    #pprint.pprint(mt_vep.count_rows()) 
    #pprint.pprint(mt_vep.describe())

    
    mt = mt.annotate_rows(sortedTranscriptConsequences=get_expr_for_vep_sorted_transcript_consequences_array(vep_root=mt.vep))
    pprint.pprint(mt.describe())

    mt = mt.annotate_rows(
        gene_symbol=hl.cond(mt.sortedTranscriptConsequences.size() > 0, mt.sortedTranscriptConsequences[0].gene_symbol, hl.null(hl.tstr)),
        major_consequence=hl.cond(mt.sortedTranscriptConsequences.size() > 0, mt.sortedTranscriptConsequences[0].major_consequence, hl.null(hl.tstr)),
        hgvs=hl.cond(mt.sortedTranscriptConsequences.size() > 0, mt.sortedTranscriptConsequences[0].hgvs, hl.null(hl.tstr)),
        category=hl.cond(mt.sortedTranscriptConsequences.size() > 0, mt.sortedTranscriptConsequences[0].category, hl.null(hl.tstr)),
        canonical=hl.cond(mt.sortedTranscriptConsequences.size() > 0, mt.sortedTranscriptConsequences[0].canonical, -1)
        )


    #pprint.pprint(mt.describe())
    #pprint.pprint(mt.show(include_row_fields=True))


    
    gnomad_exomes_ht = hl.read_table('/gpfs/ycga/project/lek/shared/data/gnomad/gnomad.exomes.r2.1.1.sites.ht')
    #pprint.pprint(gnomad_exomes_ht.describe())
    #pprint.pprint(gnomad_exomes_ht.show())
    #global_meta = hl.eval(gnomad_exomes_ht.globals.freq_index_dict)
    #pprint.pprint(global_meta)

    #mt = mt.annotate_rows(gnomad_af = gnomad_exomes_ht[mt.row_key].freq[0].AF)
    mt = mt.annotate_rows(gnomad_af = hl.cond(hl.is_defined(gnomad_exomes_ht[mt.row_key]), gnomad_exomes_ht[mt.row_key].freq[0].AF, 0.0))


    #pprint.pprint(mt.describe())
    #pprint.pprint(mt.show(include_row_fields=True))
    #pprint.pprint(mt.count_cols()) 
    #pprint.pprint(mt.count_rows()) 
    #pprint.pprint(mt.major_consequence.show())

    #mt = mt.filter_rows(mt.gnomad_af > 0.0004, keep=False)
    #pprint.pprint(mt.count_cols()) 
    #pprint.pprint(mt.count_rows()) #552,067 114,297
    #pprint.pprint(mt.show(include_row_fields=True))
    #pprint.pprint(mt.major_consequence.show())

    
    pedigree = hl.Pedigree.read('/gpfs/ycga/project/gruber/nsd35/hail_DeNovo/PCGC11.fam')
    de_novo_results = (hl.de_novo(mt, pedigree, pop_frequency_prior=mt.gnomad_af)).key_by('locus','alleles')
    #pprint.pprint(de_novo_results.describe())


    #de_novo_results = hl.de_novo(mt, pedigree, pop_frequency_prior=gnomad_exomes_ht[mt.row_key].freq[0].AF)

    #de_novo_results = de_novo_results.filter(de_novo_results.confidence == "HIGH")
    #pprint.pprint(de_novo_results.key)

    #ht = mt.rows()
    #pprint.pprint(ht.describe())
    #pprint.pprint(ht.show())
    '''
    de_novo_results = de_novo_results.annotate(
                        gene=ht[de_novo_results.key].gene_symbol,
                        major_consequence=ht[de_novo_results.key].major_consequence,
                        hgvs=ht[de_novo_results.key].hgvs,
                        category=ht[de_novo_results.key].category,
                        canonical=ht[de_novo_results.key].canonical,                    
                        )

    '''
    #pprint.pprint(de_novo_results.show())
    de_novo_results.export('denovo_results.tsv')
    
    '''
    #mt = hl.import_vcf('vcf_files/pcgc_chr20_slice.vcf.bgz',reference_genome='GRCh37')
    mt = hl.import_vcf(args.vcf,reference_genome='GRCh37')

    #Split alleles
    mt = generate_split_alleles(mt)
    #pprint.pprint(mt.describe())
    #pprint.pprint(mt.show(include_row_fields=True))

    #Annotate Population frequencies for now
    meta_ht = hl.import_table(args.meta,delimiter='\t',key='ID')
    ht = annotate_frequencies(mt,meta_ht)
    #pprint.pprint(ht.describe())
    #pprint.pprint(ht.show())

    #VEP Annotate the Hail table (ie. sites-only)
    ht = hl.vep(ht, 'vep85-loftee-ruddle.json')
    #pprint.pprint(ht.describe())
    #pprint.pprint(ht.show())

    ht = prepare_ht_export(ht)
    #pprint.pprint(ht.describe()) 
    #pprint.pprint(ht.show())

    ht = prepare_ht_for_es(ht)
    #pprint.pprint(ht.describe())
    #pprint.pprint(ht.show())

    ht.write('pcgc_exomes_v2.ht',overwrite=True)
    #export_ht_to_es(ht)

    #ht = hl.read_table('/home/ml2529/PCGC_dev/data/pcgc_chr20_100samples.ht')
    #ht = hl.read_table('/home/ml2529/PCGC_dev/data/pcgc_exomes.ht')
    #export_ht_to_es(ht)
    '''

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    #parser.add_argument('--vcf', '--input', '-i', help='bgzipped VCF file (.vcf.bgz)', required=True)
    #parser.add_argument('--meta', '-m', help='Meta file containing sample population and sex', required=True)

    args = parser.parse_args()
    run_pipeline(args)
