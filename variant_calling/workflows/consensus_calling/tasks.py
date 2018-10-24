import pypeliner


def parse_museq(infile, output, low_map_filt_output):
    '''
    Parse the input VCF file into a TSV file

    :param infile: temporary input VCF file
    :param output: path to the output TSV file
    '''

    cmd = ['vizutils_parse_museq', '--infile', infile,
           '--pre_mappability_output', output,
           '--output', low_map_filt_output,
           '--keep_dbsnp', '--keep_1000gen', '--remove_duplicates']
    pypeliner.commandline.execute(*cmd)



def parse_strelka(infile, output):
    parser = ParseStrelka(infile=infile, tid='NA', nid='NA', output=output,
                        keep_dbsnp=True,keep_1000gen=True,
                        remove_duplicates=True)

    parser.main()