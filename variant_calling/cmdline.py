'''
Created on Feb 19, 2018

@author: dgrewal
'''
import argparse
import json
import pypeliner
from variant_calling import __version__

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

    pypeliner.app.add_arguments(variant_calling)


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

    return args
