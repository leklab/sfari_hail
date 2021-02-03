import pprint
import argparse

from annotate_frequencies import *
from generate_split_alleles import *
from prepare_ht_export import *
from prepare_ht_for_es import *
from export_ht_to_es import *


def run_pipeline(args):
    hl.init(log='./hail_annotation_pipeline.log')
    ht = hl.read_table(args.input)
    export_ht_to_es(ht,index_name='gnomad_structural_variants')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--input', '-i', help='Hail table', required=True)

    args = parser.parse_args()
    run_pipeline(args)

