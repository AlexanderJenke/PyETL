from optparse import OptionParser

def get_default_opts():
    parser = OptionParser()
    parser.add_option("--db_host", dest="db_host", default="localhost")
    parser.add_option("--db_port", dest="db_port", default="5432")
    parser.add_option("--interval", dest="interval", default="* * * * *")
    parser.add_option("--csv_dir", dest="csv_dir", default="Daten")
    return parser.parse_args()[0]
