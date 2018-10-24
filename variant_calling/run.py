"""
Created on Feb 19, 2018

@author: dgrewal
"""

import pypeliner
from cmdline import parse_args
from single_cell.config import pipeline_config
from single_cell.config import batch_config


def generate_config(args):
    config_yaml = args.get("pipeline_config")
    batch_yaml = args.get("batch_config")

    if config_yaml:
        pipeline_config.generate_pipeline_config(args)

    if batch_yaml:
        batch_config.generate_pipeline_config(args)

def variant_calling_workflow(workflow, args):

    raise NotImplementedError()


if __name__ == "__main__":

    args = parse_args()

    if args["which"] == "generate_config":
        generate_config(args)

    if args["which"] == "run":
        pyp = pypeliner.app.Pypeline(config=args)
        workflow = pypeliner.workflow.Workflow()
        workflow = variant_calling_workflow(workflow, args)
        pyp.run(workflow)

