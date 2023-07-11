import dataclasses
import logging
import os

from pydantic.dataclasses import dataclass

from app.etl_exceptions import AutoETLException
from app.services.config_parser import ConfigParser, MetadataParser
from app.services.query_builder import RedshiftDialect
from app.utils import excel_to_json, validate_joins_mapping

logger = logging.getLogger(__name__)


class Config:
    arbitrary_types_allowed = True


@dataclass(config=Config)
class QueryBuilder:
    metadata_file_path: str
    config_file_path: str
    metadata_parser: MetadataParser = dataclasses.field(init=False)
    config_parser: ConfigParser = dataclasses.field(init=False)

    def __post_init__(self):
        self.metadata_parser: MetadataParser = MetadataParser.get_metadata_parser(
            self.metadata_file_path)
        self.config_parser: ConfigParser = ConfigParser.get_config_parser(
            self.config_file_path)

    def run(self) -> None:
        """ Method to trigger the build
        """
        logger.info("Initialising Auto ETL")
        logger.info("Found metadata file - %s",
                    os.path.basename(self.metadata_file_path))
        logger.info("Found config file - %s",
                    os.path.basename(self.config_file_path))

        meta_xls = self.metadata_parser.validate_file()
        _config = self.config_parser.validate_file()

        logger.info("building query from meta_file -> %s",
                    {self.metadata_file_path})

        target_table_json = excel_to_json(meta_xls, 'target_table', 'records')
        joins_and_filters = excel_to_json(
            meta_xls, 'joins_and_filters', 'index')
        select_sources = excel_to_json(meta_xls, 'select_sources', 'records')

        validate_joins_mapping(joins_and_filters)
        match _config['target']:
            case 'redshift':
                RedshiftDialect(target_table_json,
                                joins_and_filters, select_sources).get_sql()

            case _:
                logger.error(
                    "Target system - %s not supported yet.", _config.get('target', 'None'))
                raise AutoETLException(
                    f"Target system - {_config['target']} not supported yet.")
