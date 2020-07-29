import pprint
import argparse
import hail as hl

def run_pipeline(args):
	hl.init(log='./hail_annotation_pipeline.log')

	'''
	rg = hl.get_reference('GRCh37')
	grch37_contigs = [x for x in rg.contigs if not x.startswith('GL') and not x.startswith('M')]
	contig_dict = dict(zip(grch37_contigs, ['chr'+x for x in grch37_contigs]))
	'''


	exome_intervals = hl.import_locus_intervals('/gpfs/ycga/project/lek/shared/resources/hg38/exome_evaluation_regions.v1.interval_list', 
						reference_genome='GRCh38')

	#mt = hl.import_vcf(args.vcf,reference_genome='GRCh38',contig_recoding=contig_dict,array_elements_required=False,force_bgz=True,filter='MONOALLELIC')
	mt = hl.import_vcf(args.vcf,reference_genome='GRCh38',array_elements_required=False,force_bgz=True,filter='MONOALLELIC')


	mt = mt.filter_rows(hl.is_defined(exome_intervals[mt.locus]))

	pprint.pprint(mt.describe())
	pprint.pprint(mt.show())

	mt = mt.repartition(hl.eval(hl.int(mt.n_partitions()/10)))
	

	mt.write(args.out,overwrite=True)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()

	parser.add_argument('--vcf', '--input', '-i', help='bgzipped VCF file (.vcf.bgz)', required=True)
	parser.add_argument('--out', '-o', help='Hail table output file name', required=True)

	args = parser.parse_args()
	run_pipeline(args)
