from optparse import OptionParser

def get_default_opts():
    parser = OptionParser()
    parser.add_option("--dummy_data", dest="dummy_data", default=True)

    parser.add_option("--user", dest="user", default="med")
    parser.add_option("--pw", dest="pw", default="1")

    parser.add_option("--db_user", dest="db_user", default="ohdsi_admin_user")
    parser.add_option("--db_pw", dest="db_pw", default="omop")
    parser.add_option("--db_host", dest="db_host", default="localhost")
    parser.add_option("--db_port", dest="db_port", default="5432")
    parser.add_option("--db_name", dest="db_name", default="ml_results")
    return parser.parse_args()[0]
