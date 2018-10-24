from pypeliner.workflow import Workflow

import csv
import pypeliner
import pypeliner.managed as mgd
import pysam

from strelkautils import default_chromosomes

import strelkautils as utils
import vcf_tasks
import tasks
import os


def create_strelka_workflow(
        normal_bam,
        normal_bai,
        tumour_bam,
        tumour_bai,
        ref_genome_fasta_file,
        indel_vcf_file,
        snv_vcf_file,
        config,
        chromosomes=default_chromosomes,
        use_depth_thresholds=False):

    workflow = Workflow()


    workflow.transform(
        name='generate_intervals',
        func=tasks.generate_intervals,
        ctx={'mem': config['memory']['low'], 'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        ret=mgd.OutputChunks('interval'),
        args=(
            config['reference_genome'],
            config['chromosomes']
        )
    )

    workflow.setobj(
        obj=mgd.OutputChunks('chrom'),
        value=chromosomes,
    )

    workflow.transform(
        name='count_fasta_bases',
        ctx={'mem': config['memory']['low'], 'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        func=tasks.count_fasta_bases,
        args=(
            ref_genome_fasta_file,
            mgd.TempOutputFile('ref_base_counts.tsv'),
        ),
    )

    workflow.transform(
        name='get_chrom_sizes',
        ctx={'mem': config['memory']['low'], 'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        func=tasks.get_known_chromosome_sizes,
        ret=mgd.TempOutputObj('known_sizes'),
        args=(
              mgd.TempInputFile('ref_base_counts.tsv'),
              chromosomes,
        ),
    )

    workflow.transform(
        name='call_somatic_variants',
        ctx={'mem': config['memory']['med'], 'pool_id': config['pools']['multicore'], 'ncpus': config['threads'], 'walltime': '08:00'},
        axes=('interval',),
        func=tasks.call_somatic_variants,
        args=(
            mgd.InputFile(normal_bam),
            mgd.InputFile(normal_bai),
            mgd.InputFile(tumour_bam),
            mgd.InputFile(tumour_bai),
            mgd.TempInputObj('known_sizes'),
            ref_genome_fasta_file,
            mgd.TempOutputFile('somatic.indels.unfiltered.vcf', 'interval'),
            mgd.TempOutputFile('somatic.indels.unfiltered.vcf.window', 'interval'),
            mgd.TempOutputFile('somatic.snvs.unfiltered.vcf', 'interval'),
            mgd.TempOutputFile('strelka.stats', 'interval'),
            mgd.InputInstance('interval'),
            config,
        ),
        kwargs={'ncores': config['max_cores']},
    )

    workflow.transform(
        name='add_indel_filters',
        axes=('chrom',),
        ctx={'mem': config['memory']['med'], 'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        func=tasks.filter_indel_file_list,
        args=(
            mgd.TempInputFile('somatic.indels.unfiltered.vcf', 'interval', axes_origin=[]),
            mgd.TempInputFile('strelka.stats', 'interval', axes_origin=[]),
            mgd.TempInputFile('somatic.indels.unfiltered.vcf.window', 'interval', axes_origin=[]),
            mgd.TempOutputFile('somatic.indels.filtered.vcf', 'chrom'),
            mgd.InputInstance('chrom'),
            mgd.TempInputObj('known_sizes'),
            mgd.InputChunks('interval'),
        ),
        kwargs={'use_depth_filter': use_depth_thresholds}
    )

    workflow.transform(
        name='add_snv_filters',
        axes=('chrom',),
        ctx={'mem': config['memory']['med'], 'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        func=tasks.filter_snv_file_list,
        args=(
            mgd.TempInputFile('somatic.snvs.unfiltered.vcf', 'interval', axes_origin=[]),
            mgd.TempInputFile('strelka.stats', 'interval', axes_origin=[]),
            mgd.TempOutputFile('somatic.snvs.filtered.vcf', 'chrom'),
            mgd.InputInstance('chrom'),
            mgd.TempInputObj('known_sizes'),
            mgd.InputChunks('interval'),
        ),
        kwargs={'use_depth_filter': use_depth_thresholds}
    )

    workflow.transform(
        name='merge_indels',
        ctx={'mem': config['memory']['med'], 'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        func=vcf_tasks.concatenate_vcf,
        args=(
            mgd.TempInputFile('somatic.indels.filtered.vcf', 'chrom'),
            mgd.TempOutputFile('somatic.indels.filtered.vcf.gz'),
        ),
    )

    workflow.transform(
        name='merge_snvs',
        ctx={'mem': config['memory']['med'], 'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        func=vcf_tasks.concatenate_vcf,
        args=(
            mgd.TempInputFile('somatic.snvs.filtered.vcf', 'chrom'),
            mgd.TempOutputFile('somatic.snvs.filtered.vcf.gz'),
        ),
    )

    workflow.transform(
        name='filter_indels',
        ctx={'mem': config['memory']['med'], 'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        func=vcf_tasks.filter_vcf,
        args=(
            mgd.TempInputFile('somatic.indels.filtered.vcf.gz'),
            mgd.TempOutputFile('somatic.indels.passed.vcf'),
        ),
    )

    workflow.transform(
        name='filter_snvs',
        ctx={'mem': config['memory']['med'], 'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        func=vcf_tasks.filter_vcf,
        args=(
            mgd.TempInputFile('somatic.snvs.filtered.vcf.gz'),
            mgd.TempOutputFile('somatic.snvs.passed.vcf'),
        ),
    )

    workflow.transform(
        name='finalise_indels',
        ctx={'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        func=vcf_tasks.finalise_vcf,
        args=(
            mgd.TempInputFile('somatic.indels.passed.vcf'),
            mgd.OutputFile(indel_vcf_file),
        ),
    )

    workflow.transform(
        name='finalise_snvs',
        ctx={'pool_id': config['pools']['standard'], 'ncpus': 1, 'walltime': '01:00'},
        func=vcf_tasks.finalise_vcf,
        args=(
            mgd.TempInputFile('somatic.snvs.passed.vcf'),
            mgd.OutputFile(snv_vcf_file),
        ),
    )

    return workflow


def get_chromosomes(bam_file, chromosomes=None):

    chromosomes = _get_chromosomes(bam_file, chromosomes)

    return dict(zip(chromosomes, chromosomes))


def _get_chromosomes(bam_file, chromosomes=None):
    bam = pysam.Samfile(bam_file, 'rb')

    if chromosomes is None:
        chromosomes = bam.references

    else:
        chromosomes = chromosomes

    return [str(x) for x in chromosomes]


def get_coords(bam_file, chrom, split_size):

    coords = {}

    bam = pysam.Samfile(bam_file, 'rb')

    chrom_lengths = dict(zip(bam.references, bam.lengths))

    length = chrom_lengths[chrom]

    lside_interval = range(1, length + 1, split_size)

    rside_interval = range(split_size, length + split_size, split_size)

    for coord_index, (beg, end) in enumerate(zip(lside_interval, rside_interval)):
        coords[coord_index] = (beg, end)

    return coords


def get_known_chromosome_sizes(bam_file, bai_file, size_file, chromosomes):
    chromosomes = _get_chromosomes(bam_file, chromosomes)

    sizes = {}

    with open(size_file, 'r') as fh:
        reader = csv.DictReader(fh, ['path', 'chrom', 'known_size', 'size'], delimiter='\t')

        for row in reader:
            if row['chrom'] not in chromosomes:
                continue

            sizes[row['chrom']] = int(row['known_size'])

    return sizes
