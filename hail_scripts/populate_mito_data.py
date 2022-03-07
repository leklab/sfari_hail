#!/usr/bin/env python

import pprint
import argparse

from export_ht_to_es import *


def run_pipeline(args):
    hl.init(log='./hail_annotation_pipeline.log')
    ht = hl.read_table(args.input)
    export_ht_to_es(ht,index_name='mito_test3')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--input', '-i', help='Hail table', required=True)

    args = parser.parse_args()
    run_pipeline(args)

