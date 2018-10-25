import os
import warnings
import yaml
from variant_calling.utils import helpers


def luna_config(reference):

    pools = {}

    memory = {
        'low': 5,
        'med': 10,
        'high': 15,
    }

    chromosomes = map(str, range(1, 23)) + ['X', 'Y']
    chromosomes = ['22']


    threads = 1

    if reference == 'grch37':
        reference = "/ifs/work/leukgen/ref/homo_sapiens/GRCh37d5/genome/gr37.fasta"
    else:
        reference = None

    snpeff_params = {'snpeff_config': '//ifs/work/leukgen/home/grewald/reference/snpEff.config'}

    mutation_assessor_params = {'db': '/ifs/work/leukgen/home/grewald/reference/MA.hg19_v2/'}

    dbsnp_params = {'db': '/ifs/work/leukgen/home/grewald/reference/dbsnp_142.human_9606.all.vcf.gz'}

    thousandgen_params = {'db': '/ifs/work/leukgen/home/grewald/reference/1000G_release_20130502_genotypes.vcf.gz'}

    cosmic_params = {'db': '/ifs/work/leukgen/home/grewald/reference/CosmicMutantExport.sorted.vcf.gz'}

    plot_params = {'threshold': 0.5,
                   'refdata_single_sample':'/refdata/single_sample_plot_data.txt'
                   }

    config = locals()

    return config


def azure_config(reference):
    pools = {'standard': None, 'highmem': None, 'multicore': None}

    memory = {
        'low': 5,
        'med': 10,
        'high': 15,
    }

    chromosomes = map(str, range(1, 23)) + ['X', 'Y']
    chromosomes = ['22']

    threads = 1

    if reference == 'grch37':
        reference = "/datadrive/refdata/GRCh37-lite.fa"
    else:
        reference = None

    snpeff_params = {'snpeff_config': '/datadrive/refdata/snpEff.config'}

    mutation_assessor_params = {'db': '/datadrive/refdata/MA.hg19_v2/'}

    dbsnp_params = {'db': '/datadrive/refdata/dbsnp_142.human_9606.all.vcf.gz'}

    thousandgen_params = {'db': '/datadrive/refdata/1000G_release_20130502_genotypes.vcf.gz'}

    cosmic_params = {'db': '/datadrive/refdata/CosmicMutantExport.sorted.vcf.gz'}

    plot_params = {'threshold': 0.5,
                   'refdata_single_sample': '/datadrive/refdata/single_sample_plot_data.txt'
                   }

    mappability_ref = '/datadrive/refdata/mask_regions_blacklist_crg_align36_table.txt'

    parse_strelka = {
        'label_mapping': '/datadrive/refdata/annotations.csv',
        'keep_1000gen': True,
        'keep_cosmic': True,
        'remove_duplicates': False,
        'keep_dbsnp': True,
        'chromosomes': map(str, range(23)) + ['X'],
        'mappability_ref': mappability_ref,
     }

    parse_museq = {
        'label_mapping': '/datadrive/refdata/annotations.csv',
        'keep_1000gen': True,
        'keep_cosmic': True,
        'remove_duplicates': False,
        'keep_dbsnp': True,
        'chromosomes': map(str, range(23)) + ['X'],
        'mappability_ref': mappability_ref,
        'pr_threshold': 0.85
     }

    config = locals()

    return config


def shahlab_config(reference):

    pools = {}

    memory = {
        'low': 5,
        'med': 10,
        'high': 15,
    }

    chromosomes = map(str, range(1,23)) + ['X', 'Y']
    chromosomes = ['22']

    threads = 1

    if reference == 'grch37':
        reference = "/shahlab/pipelines/reference/GRCh37-lite.fa"
    else:
        reference = None

    snpeff_params = {'snpeff_config': '/shahlab/pipelines/apps_centos6/snpEff_4_3/snpEff.config'}

    mutation_assessor_params = {'db': '/shahlab/pipelines/reference/MA.hg19_v2/'}

    dbsnp_params = {'db': '/shahlab/pipelines/reference/dbsnp_142.human_9606.all.vcf.gz'}

    thousandgen_params = {'db': '/shahlab/pipelines/reference/1000G_release_20130502_genotypes.vcf.gz'}

    cosmic_params = {'db': '/shahlab/dgrewal/cosmic/CosmicMutantExport.sorted.vcf.gz'}

    plot_params = {'threshold': 0.5,
                   'refdata_single_sample':'/refdata/single_sample_plot_data.txt'
                   }


    config = locals()

    return config

def get_config(override):

    if override["cluster"] == "shahlab":
        config = shahlab_config(override["reference"])
    elif override["cluster"] == "luna":
        config = luna_config(override["reference"])
    elif override["cluster"] == "azure":
        config = azure_config(override["reference"])

    return config


def write_config(params, filepath):
    with open(filepath, 'w') as outputfile:
        yaml.safe_dump(params, outputfile, default_flow_style=False)


def generate_pipeline_config(args):

    if args['which'] == 'generate_config':
        config_yaml = args['pipeline_config']
        config_yaml = os.path.abspath(config_yaml)
    else:
        config_yaml = "config.yaml"
        tmpdir = args.get("tmpdir", None)
        pipelinedir = args.get("pipelinedir", None)

        # use pypeliner tmpdir to store yaml
        if pipelinedir:
            config_yaml = os.path.join(pipelinedir, config_yaml)
        elif tmpdir:
            config_yaml = os.path.join(tmpdir, config_yaml)
        else:
            warnings.warn("no tmpdir specified, generating configs in working dir")
            config_yaml = os.path.join(os.getcwd(), config_yaml)

        config_yaml = helpers.get_incrementing_filename(config_yaml)
    print config_yaml

    params_override = {'cluster': 'azure', 'reference': 'grch37'}
    if args['config_override']:
        params_override.update(args["config_override"])

    helpers.makedirs(config_yaml, isfile=True)

    config = get_config(params_override)
    write_config(config, config_yaml)

    args["config_file"] = config_yaml

    print config_yaml
    return args

