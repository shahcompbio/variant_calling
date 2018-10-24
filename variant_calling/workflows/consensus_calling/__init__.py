'''
Created on Feb 21, 2018

@author: pwalters
'''
import pypeliner
import pypeliner.managed as mgd
import tasks


def create_museq_workflow(
        museq_vcf,
        strelka_snv,
        strelka_indel,
        config):

    workflow = pypeliner.workflow.Workflow()

    #1: parse museq vcf
    #2 parse strelka vcf
    #3 parse strelka indel
    #get overlapping calls

    return workflow
