'''
Created on Feb 21, 2018

@author: pwalters
'''
import pypeliner
import pypeliner.managed as mgd
import tasks


def create_museq_workflow(
        snv_vcf,
        snv_filtered_vcf,
        snv_maf,
        museqportrait_pdf,
        museqportrait_txt,
        config,
        tumour_bam=None,
        normal_bam=None):

    workflow = pypeliner.workflow.Workflow()

    workflow.transform(
        name='generate_intervals',
        func=tasks.generate_intervals,
        ctx={'mem': config['memory']['low'], 'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        ret=mgd.OutputChunks('interval'),
        args=(
            config['reference'],
            config['chromosomes']
        )
    )

    if tumour_bam and not normal_bam:
        # Unpaired with only tumour
        workflow.transform(
            name='run_museq_unpaired_tumour',
            ctx={'num_retry': 3, 'mem_retry_increment': 2,
                'mem': config['memory']['high'],
                'pool_id': config['pools']['multicore'],
                'ncpus': config['threads'],
                'walltime': '24:00'},
            axes=('interval',),
            func=tasks.run_museq,
            args=(
                mgd.TempOutputFile('museq.vcf','interval'),
                mgd.TempOutputFile('museq.log', 'interval'),
                config,
                mgd.InputInstance('interval')
            ),
            kwargs={
                'tumour_bam': mgd.InputFile(tumour_bam, extensions=['.bai']),
            }
        )
    elif not tumour_bam and normal_bam:
        # Unpaired with only normal
        workflow.transform(
            name='run_museq_unpaired_normal',
            ctx={'num_retry': 3, 'mem_retry_increment': 2,
                'mem': config['memory']['high'],
                'pool_id': config['pools']['multicore'],
                'ncpus': config['threads'],
                'walltime': '24:00'},
            axes=('interval',),
            func=tasks.run_museq,
            args=(
                mgd.TempOutputFile('museq.vcf', 'interval'),
                mgd.TempOutputFile('museq.log', 'interval'),
                config,
                mgd.InputInstance('interval')
            ),
            kwargs={
                'normal_bam': mgd.InputFile(normal_bam, extensions=['.bai']),
            }
        )
    else:
        # Paired
        workflow.transform(
            name='run_museq_paired',
            ctx={'num_retry': 3, 'mem_retry_increment': 2,
                'mem': config['memory']['high'],
                'pool_id': config['pools']['multicore'],
                'ncpus': config['threads'],
                'walltime': '24:00'},
            axes=('interval',),
            func=tasks.run_museq,
            args=(
                mgd.TempOutputFile('museq.vcf', 'interval'),
                mgd.TempOutputFile('museq.log', 'interval'),
                config,
                mgd.InputInstance('interval')
            ),
            kwargs={
                'tumour_bam': mgd.InputFile(tumour_bam, extensions=['.bai']),
                'normal_bam': mgd.InputFile(normal_bam, extensions=['.bai']),
            }
        )

    workflow.transform(
        name='merge_vcfs',
        ctx={'num_retry': 3, 'mem_retry_increment': 2,
            'mem': config['memory']['high'],
            'pool_id': config['pools']['multicore'],
            'ncpus': config['threads'],
            'walltime': '08:00'},
        func=tasks.merge_vcfs,
        args=(
            mgd.TempInputFile('museq.vcf', 'interval'),
            mgd.TempOutputFile(snv_vcf),
        )
    )

    workflow.transform(
        name='run_museqportrait',
        ctx={'num_retry': 3, 'mem_retry_increment': 2,
            'mem': config['memory']['low'],
            'pool_id': config['pools']['standard'],
            'ncpus': 1, 'walltime': '08:00'},
        func=tasks.run_museqportrait,
        args=(
            mgd.InputFile(snv_vcf),
            mgd.OutputFile(museqportrait_pdf),
            mgd.OutputFile(museqportrait_txt),
            mgd.TempOutputFile('museqportrait.log'),
        ),
    )

    workflow.transform(
        name='filter_museq_vcf', 
        ctx={
            'mem': config['memory']['low'], 
            'pool_id': config['pools']['standard'], 
            'ncpus': 1},
        func=tasks.filter_museq_vcf, 
        args=(
            mgd.InputFile(snv_vcf),  
            mgd.OutputFile(snv_filtered_vcf), 
            config,
        ),
    )

    workflow.transform(
        name='convert_vcf_to_maf', 
        ctx={
            'mem': config['memory']['low'], 
            'pool_id': config['pools']['standard'], 
            'ncpus': 1},
        func=tasks.convert_vcf_to_maf, 
        args=(
            mgd.InputFile(snv_filtered_vcf), 
            mgd.OutputFile(snv_maf), 
            config,
        ),
    )

    return workflow
