'''
Created on Feb 21, 2018

@author: pwalters
'''
import pypeliner
import pypeliner.managed as mgd
import tasks
from variant_calling.utils import vcf_tasks


def create_annotation_workflow(
        input_vcf,
        annotated_vcf,
        config):

    workflow = pypeliner.workflow.Workflow()

    workflow.transform(
        name='run_snpeff',
        ctx={'num_retry': 3, 'mem_retry_increment': 2,
            'mem': config['memory']['high'],
            'pool_id': config['pools']['highmem'],
            'ncpus': 1,'walltime': '08:00'},
        func=tasks.run_snpeff,
        args=(
            mgd.InputFile(input_vcf),
            mgd.TempOutputFile('museq_annotSnpEff.vcf'),
            config,
        ),
    )

    workflow.transform(
        name='run_mutation_assessor',
        ctx={'num_retry': 3, 'mem_retry_increment': 2,
            'mem': config['memory']['low'],
            'pool_id': config['pools']['standard'],
            'ncpus': 1, 'walltime': '08:00'},
        func=tasks.run_mutation_assessor,
        args=(
            mgd.TempInputFile('museq_annotSnpEff.vcf'),
            mgd.TempOutputFile('museq_annotMA.vcf'),
            config,
        ),
    )

    workflow.transform(
        name='run_DBSNP',
        ctx={'num_retry': 3, 'mem_retry_increment': 2,
            'mem': config['memory']['high'],
            'pool_id': config['pools']['highmem'],
            'ncpus': 1, 'walltime': '08:00'},
        func=tasks.run_DBSNP,
        args=(
            mgd.TempInputFile('museq_annotMA.vcf'),
            mgd.TempOutputFile('museq_flagDBsnp.vcf'),
            config,
        ),
    )

    workflow.transform(
        name='run_1000gen',
        ctx={'num_retry': 3, 'mem_retry_increment': 2,
            'mem': config['memory']['high'],
            'pool_id': config['pools']['highmem'],
            'ncpus': 1, 'walltime': '08:00'},
        func=tasks.run_1000gen,
        args=(
            mgd.TempInputFile('museq_flagDBsnp.vcf'),
            mgd.TempOutputFile('museq_flag1000gen.vcf'),
            config,
        ),
    )

    workflow.transform(
        name='run_cosmic',
        ctx={'num_retry': 3, 'mem_retry_increment': 2,
            'mem': config['memory']['high'],
            'pool_id': config['pools']['highmem'],
            'ncpus': 1, 'walltime': '08:00'},
        func=tasks.run_cosmic,
        args=(
            mgd.TempInputFile('museq_flag1000gen.vcf'),
            mgd.TempOutputFile('museq_cosmic.vcf'),
            config,
        ),
    )

    workflow.transform(
        name='finalize',
        ctx={'num_retry': 3, 'mem_retry_increment': 2,
            'mem': config['memory']['high'],
            'pool_id': config['pools']['highmem'],
            'ncpus': 1, 'walltime': '08:00'},
        func=vcf_tasks.finalise_vcf,
        args=(
            mgd.TempInputFile('museq_cosmic.vcf'),
            mgd.OutputFile(annotated_vcf, extensions=['.csi','.tbi']),
        ),
    )


    return workflow
