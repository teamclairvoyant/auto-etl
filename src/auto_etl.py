import logging
import sys
from argparse import ArgumentParser

from app.etl_exceptions import AutoETLException
from app.logger import setup_logging
from app.services.query_builder import QueryBuilder

if __name__ == "__main__":
    setup_logging('logs')
    logger = logging.getLogger(__name__)

    parser = ArgumentParser(description="Auto ETL")

    parser.add_argument("-t", "--tool", dest="tool", required=True,
                        help="tool that needs to be used", choices=["query-builder", "pipeline-builder"])
    parser.add_argument("-m", "--meta-file", dest="meta_file", required='query-builder' in sys.argv,
                        help="absolute path of the meta file", metavar="<path to meta file>")
    parser.add_argument("-c", "--config-file", dest="config_file", required=True,
                        help="absolute path of the config file", metavar="<path to config file>")

    args = parser.parse_args()

    try:
        if args.tool == 'query-builder':
            if args.meta_file and args.config_file:
                QueryBuilder(args.meta_file, args.config_file).run()
        else:
            pass
    except AutoETLException as excep:
        logger.error("Auto ETL exception - %s", excep.args)
        sys.exit(1)
