'''
Created on Feb 21, 2018

@author: pwalters
'''
import os
import pysam
import pypeliner
import multiprocessing
from pypeliner_utils import helpers, vcfutils


scripts_directory = os.path.join(
    os.path.realpath(os.path.dirname(__file__)), 'scripts')


def generate_intervals(ref, chromosomes, size=1000000):
    fasta = pysam.FastaFile(ref)
    lengths = fasta.lengths
    names = fasta.references

    intervals = []

    for name, length in zip(names, lengths):
        if name not in chromosomes:
            continue
        for i in range(int((length/size)+1)):
            intervals.append( name+ "_" + str(i*size) +"_"+ str((i+1)*size))

    return intervals

def run_museq(out, log, config, interval, tumour_bam=None, tumour_bai=None,
              normal_bam=None, normal_bai=None):
    '''
    Run museq script for all chromosomes and merge VCF files

    :param tumour: path to tumour bam
    :param normal: path to normal bam
    :param out: path to the temporary output VCF file for the merged VCF files
    :param log: path to the log file
    :param config: path to the config YAML file
    '''
    script = os.path.join(config['museq_params']['mutationseq'], 'classify.py')
    conf = os.path.join(
        config['museq_params']['mutationseq'], 'metadata.config')
    model = config['museq_params']['mutationseq_model']
    reference = config['museq_params']['reference_genome']

    cmd = ['python', script]

    if tumour_bam:
        cmd.append('tumour:' + tumour_bam)
    if normal_bam:
        cmd.append('normal:' + normal_bam)

    interval = interval.split('_')
    interval = interval[0] +':'+ interval[1] + '-' + interval[2]

    cmd.extend(['reference:' + reference, 'model:' + model, '--out', out,
                '--log', log, '--config', conf, '--interval', interval, '-v'])

    pypeliner.commandline.execute(*cmd)

def merge_vcfs(inputs, output):
    vcfutils.concatenate_vcf(inputs, output)


def run_museqportrait(infile, out_pdf, out_txt, museqportrait_log):
    '''
    Run museqportrait script on the input VCF file

    :param infile: temporary input VCF file
    :param out_dir: temporary output VCF file
    :param museqportrait_log: path to the log file
    '''

    cmd = ['museqportrait', '--log', museqportrait_log, '--output-pdf',
           out_pdf, '--output-txt', out_txt, infile]

    pypeliner.commandline.execute(*cmd)
