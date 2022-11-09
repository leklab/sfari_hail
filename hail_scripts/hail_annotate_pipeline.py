import pprint
import argparse

from annotate_frequencies import *
from generate_split_alleles import *
from prepare_ht_export import *
from prepare_ht_for_es import *
from export_ht_to_es import *

def qual_hist_expr(
    gt_expr: Optional[hl.expr.CallExpression] = None,
    gq_expr: Optional[hl.expr.NumericExpression] = None,
    dp_expr: Optional[hl.expr.NumericExpression] = None,
    ad_expr: Optional[hl.expr.ArrayNumericExpression] = None,
    adj_expr: Optional[hl.expr.BooleanExpression] = None,
) -> hl.expr.StructExpression:
    """
    Return a struct expression with genotype quality histograms based on the arguments given (dp, gq, ad).
    .. note::
        - If `gt_expr` is provided, will return histograms for non-reference samples only as well as all samples.
        - `gt_expr` is required for the allele-balance histogram, as it is only computed on het samples.
        - If `adj_expr` is provided, additional histograms are computed using only adj samples.
    :param gt_expr: Entry expression containing genotype
    :param gq_expr: Entry expression containing genotype quality
    :param dp_expr: Entry expression containing depth
    :param ad_expr: Entry expression containing allelic depth (bi-allelic here)
    :param adj_expr: Entry expression containing adj (high quality) genotype status
    :return: Genotype quality histograms expression
    """
    qual_hists = {}
    if gq_expr is not None:
        qual_hists["gq_hist"] = hl.agg.hist(gq_expr, 0, 100, 20)
    if dp_expr is not None:
        qual_hists["dp_hist"] = hl.agg.hist(dp_expr, 0, 100, 20)

    if gt_expr is not None:
        qual_hists = {
            **{
                f"{qual_hist_name}_all": qual_hist_expr
                for qual_hist_name, qual_hist_expr in qual_hists.items()
            },
            **{
                f"{qual_hist_name}_alt": hl.agg.filter(
                    gt_expr.is_non_ref(), qual_hist_expr
                )
                for qual_hist_name, qual_hist_expr in qual_hists.items()
            },
        }
        if ad_expr is not None:
            qual_hists["ab_hist_alt"] = hl.agg.filter(
                gt_expr.is_het(), hl.agg.hist(ad_expr[1] / hl.sum(ad_expr), 0, 1, 20)
            )

    else:
        qual_hists = {
            f"{qual_hist_name}_all": qual_hist_expr
            for qual_hist_name, qual_hist_expr in qual_hists.items()
        }

    if adj_expr is not None:
        qual_hists.update(
            {
                f"{qual_hist_name}_adj": hl.agg.filter(adj_expr, qual_hist_expr)
                for qual_hist_name, qual_hist_expr in qual_hists.items()
            }
        )

    return hl.struct(**qual_hists)


def run_pipeline(args):
    #hl.init(log='./hail_annotation_pipeline.log',master='spark://worker5090.cm.cluster:7077')
    hl.init(log='./hail_annotation_pipeline.log',master='local[20]',spark_conf={'spark.driver.memory': '8g', 'spark.executor.memory': '8g'})

	#Adding 'chr' prefix so to be compatible
    rg = hl.get_reference('GRCh37')
    grch37_contigs = [x for x in rg.contigs if not x.startswith('GL') and not x.startswith('M')]
    contig_dict = dict(zip(grch37_contigs, ['chr'+x for x in grch37_contigs]))

    '''
    Import VCF   
    '''

    mt = hl.import_vcf(args.vcf,reference_genome='GRCh38',contig_recoding=contig_dict,array_elements_required=False,force_bgz=True,filter='MONOALLELIC')

    #mt = hl.filter_intervals(mt, [hl.parse_locus_interval('chr21:1-46709983',reference_genome='GRCh38')])
	
    #Split alleles
    mt = generate_split_alleles(mt)

    mt = mt.annotate_rows(raw_qual_hists=qual_hist_expr(mt.GT,mt.GQ,mt.DP,mt.AD))

    #Annotate Population frequencies for now
    meta_ht = hl.import_table(args.meta,delimiter='\t',key='ID')
    ht = annotate_frequencies(mt,meta_ht)

    res_ht = hl.read_table('/mnt/home/mlek/ceph/resources/combined_reference_data_grch38.ht')
    dbsnp_ht = hl.read_table('/mnt/home/mlek/ceph/resources/dbsnp_b151_grch38_all_20180418.ht')
    #clinvar_ht = hl.read_table('/mnt/home/mlek/ceph/resources/clinvar_20190923.ht')

    
    ht = ht.annotate(rsid = dbsnp_ht[ht.key].rsid,
        in_silico_predictors=hl.struct(
            splice_ai = res_ht[ht.key].splice_ai.delta_score, 
            primate_ai= res_ht[ht.key].primate_ai.score,
            cadd= res_ht[ht.key].cadd.PHRED,
            revel= res_ht[ht.key].dbnsfp.REVEL_score
        ),

        allele_balance=hl.struct(
            alt_raw=ht.raw_qual_hists.ab_hist_alt.annotate(
                bin_edges=ht.raw_qual_hists.ab_hist_alt.bin_edges.map(lambda n: hl.float(hl.format("%.3f", n)))
            )
        ),
        genotype_depth=hl.struct(
            all_raw=ht.raw_qual_hists.dp_hist_all,
            alt_raw=ht.raw_qual_hists.dp_hist_alt
        ),
        genotype_quality=hl.struct(
            all_raw=ht.raw_qual_hists.gq_hist_all,
            alt_raw=ht.raw_qual_hists.gq_hist_alt
        )
    )
    

    #ht = mt.rows()
    #pprint.pprint(ht.describe())
    #pprint.pprint(ht.show())

    #VEP Annotate the Hail table (ie. sites-only) using GRCh38 configuration file
    ht = hl.vep(ht, '/mnt/home/mlek/ceph/vep-GRCh38.json')

	#Reformatting for Elasticsearch export
    ht = prepare_ht_export(ht)
    ht = prepare_ht_for_es(ht)

    #ht = mt.rows()





    pprint.pprint(ht.describe())
    pprint.pprint(ht.show())


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
