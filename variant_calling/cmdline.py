'''
Created on Feb 19, 2018

@author: dgrewal
'''
import argparse
import pypeliner
import os
import sys
import json

from single_cell.utils import helpers


from single_cell.config import generate_batch_config
from single_cell.config import generate_pipeline_config
from single_cell import __version__

def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--version', action='version',
                        version='{version}'.format(version=__version__))

    subparsers = parser.add_subparsers()

    #================
    # variant calling
    #================
    variant_calling = subparsers.add_parser("run")
    variant_calling.set_defaults(which='run')

    variant_calling.add_argument("--input_yaml",
                                 required=True,
                                 help='''yaml file with tumour, normal and sampleids''')

    variant_calling.add_argument("--out_dir",
                                required=True,
                                help='''Path to output directory.''')

    variant_calling.add_argument("--config_file",
                                help='''Path to the config file.''')

    variant_calling.add_argument("--config_override",
                                type=json.loads,
                                help='''json string to override the defaults in config''')


    #======================================
    # generates pipeline and batch configs
    #======================================
    generate_config = subparsers.add_parser("generate_config")
    generate_config.set_defaults(which='generate_config')

    generate_config.add_argument("--pipeline_config",
                                 help='''output yaml file''')

    generate_config.add_argument("--batch_config",
                                 help='''output yaml file''')

    generate_config.add_argument("--config_override",
                                 type=json.loads,
                                 help='''json string to override the defaults in config''')


    args = vars(parser.parse_args())

    # add config paths to global args if needed.
    args = generate_pipeline_config.generate_pipeline_config_in_temp(args)

    args = generate_batch_config.generate_submit_config_in_temp(args)

    return args
