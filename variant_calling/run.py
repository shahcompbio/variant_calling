"""
Created on Feb 19, 2018

@author: dgrewal
"""
import os
import pypeliner
import pypeliner.managed as mgd
from cmdline import parse_args
from variant_calling.config import pipeline_config
from variant_calling.config import batch_config
from variant_calling.utils import helpers
from variant_calling.workflows import mutationseq
from variant_calling.workflows import strelka
from variant_calling.workflows import vcf_annotation
from variant_calling.workflows import consensus_calling

def generate_config(args):
    if args['which'] == 'generate_config':
        if args.get("pipeline_config"):
            args = pipeline_config.generate_pipeline_config(args)
        if args.get("batch_config"):
            args = batch_config.generate_pipeline_config(args)
    else:
        if not args.get("config_file"):
            args = pipeline_config.generate_pipeline_config(args)
        if not args.get("submit_config"):
            args = batch_config.generate_batch_config(args)
    return args


def variant_calling_workflow(args):
    pyp = pypeliner.app.Pypeline(config=args)
    workflow = pypeliner.workflow.Workflow()

    args = generate_config(args)
    config = helpers.load_yaml(args['config_file'])
    inputs = helpers.load_yaml(args['input_yaml'])
    output_dir = os.path.join(args['out_dir'], '{sample_id}')

    ## TODO: for now assume both normals and tumours exist
    samples = inputs.keys()
    tumours = {sample: inputs[sample]['tumour'] for sample in samples}
    normals = {sample: inputs[sample]['normal'] for sample in samples}

    museq_vcf = os.path.join(output_dir, '{sample_id}', 'museq_paired_annotated.vcf')
    museqss_vcf = os.path.join(output_dir, '{sample_id}', 'museq_single_annotated.vcf')
    strelka_vcf = os.path.join(output_dir, '{sample_id}', 'strelka_annotated.vcf')


    workflow.setobj(
        obj=mgd.OutputChunks('sample_id'),
        value=samples)


    museqportrait_pdf = os.path.join(output_dir, 'paired_museqportrait.pdf')
    museqportrait_txt = os.path.join(output_dir, 'paired_museqportrait.txt')
    workflow.subworkflow(
        name="mutationseq_paired",
        func=mutationseq.create_museq_workflow,
        axes=('sample_id',),
        args=(
            mgd.TempOutputFile("museq_snv.vcf.gz", 'sample_id'),
            mgd.OutputFile(museqportrait_pdf, 'sample_id'),
            mgd.OutputFile(museqportrait_txt, 'sample_id'),
            config,
            mgd.InputInstance('sample_id')
        ),
        kwargs={
            'tumour_bam': mgd.InputFile("tumour.bam", 'sample_id', fnames=tumours,
                                        extensions=['.bai'], axes_origin=[]),
            'normal_bam': mgd.InputFile("normal.bam", 'sample_id', fnames=normals,
                                        extensions=['.bai'], axes_origin=[]),
        }
    )

    museqportrait_pdf = os.path.join(output_dir, 'single_museqportrait.pdf')
    museqportrait_txt = os.path.join(output_dir, 'single_museqportrait.txt')
    workflow.subworkflow(
        name="mutationseq_single",
        func=mutationseq.create_museq_workflow,
        axes=('sample_id',),
        args=(
            mgd.TempOutputFile("museq_germlines.vcf.gz", 'sample_id'),
            mgd.OutputFile(museqportrait_pdf, 'sample_id'),
            mgd.OutputFile(museqportrait_txt, 'sample_id'),
            config,
            mgd.InputInstance('sample_id')
        ),
        kwargs={
            'tumour_bam': None,
            'normal_bam': mgd.InputFile("normal.bam", 'sample_id', fnames=normals,
                                        extensions=['.bai'], axes_origin=[]),
        }
    )

    workflow.subworkflow(
        name="strelka",
        func=strelka.create_strelka_workflow,
        axes=('sample_id',),
        args=(
            mgd.InputFile('normal_bam', 'sample_id', fnames=normals, extensions=['.bai']),
            mgd.InputFile('tumour_bam', 'sample_id', fnames=tumours, extensions=['.bai']),
            config['reference'],
            mgd.TempOutputFile('strelka_indel.vcf.gz', 'sample_id'),
            mgd.TempOutputFile('strelka_snv.vcf.gz', 'sample_id'),
            config,
        ),
    )

    workflow.subworkflow(
        name="annotate_paired_museq",
        func=vcf_annotation.create_annotation_workflow,
        axes=('sample_id',),
        args=(
            mgd.TempInputFile("museq_snv.vcf.gz", 'sample_id'),
            mgd.TempOutputFile('museq_snv_ann.vcf.gz', 'sample_id'),#, template=museq_vcf),
            config,
        ),
    )

    workflow.subworkflow(
        name="annotate_germline_museq",
        func=vcf_annotation.create_annotation_workflow,
        axes=('sample_id',),
        args=(
            mgd.TempInputFile("museq_germlines.vcf.gz", 'sample_id'),
            mgd.TempOutputFile('museq_germlines_ann.vcf.gz', 'sample_id'),#, template=museq_vcf),
            config,
        ),
    )

    workflow.subworkflow(
        name="annotate_strelka",
        func=vcf_annotation.create_annotation_workflow,
        axes=('sample_id',),
        args=(
            mgd.TempInputFile("strelka_snv.vcf.gz", 'sample_id'),
            mgd.TempOutputFile('strelka_snv_ann.vcf.gz', 'sample_id'),#, template=museq_vcf),
            config,
        ),
    )

    workflow.subworkflow(
        name="annotate_strelka_indel",
        func=vcf_annotation.create_annotation_workflow,
        axes=('sample_id',),
        args=(
            mgd.TempInputFile("strelka_indel.vcf.gz", 'sample_id'),
            mgd.TempOutputFile('strelka_indel_ann.vcf.gz', 'sample_id'),#, template=museq_vcf),
            config,
        ),
    )


    workflow.subworkflow(
        name="consensus_calling",
        func=consensus_calling.create_consensus_workflow,
        axes=('sample_id',),
        args=(
            mgd.TempInputFile("museq_germlines_ann.vcf.gz", 'sample_id'),
            mgd.TempInputFile("museq_snv_ann.vcf.gz", 'sample_id'),
            mgd.TempInputFile("strelka_snv_ann.vcf.gz", 'sample_id'),
            mgd.TempInputFile("strelka_indel_ann.vcf.gz", 'sample_id'),
            mgd.OutputFile("temp.csv"),
            config,
        ),
    )



    pyp.run(workflow)



def main():
    args = parse_args()

    if args["which"] == "generate_config":
        generate_config(args)

    if args["which"] == "run":
        variant_calling_workflow(args)


if __name__ == "__main__":
    main()
