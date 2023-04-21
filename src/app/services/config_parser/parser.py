import json
import logging
from typing import Any

import pandas as pd
from pydantic.dataclasses import dataclass

from app.etl_exceptions import AutoETLException
from app.utils import is_valid_file

logger = logging.getLogger(__name__)

sheet_names = ["target_table", "joins_and_filters"]


@dataclass
class MetadataParser:
    metadata_file_path: str

    def validate_file(self) -> pd.ExcelFile:
        """method to validate the metadata excel file

        Raises:
            AutoETLException: Exception if sheet names not found in the file

        Returns:
            ExcelFile
        """
        logger.info("Validating the metadata file")
        if not is_valid_file(self.metadata_file_path):
            logger.error("File path not correct - %s", self.metadata_file_path)
            raise AutoETLException(
                f"File path not correct - {self.metadata_file_path}")
        meta_xls = pd.ExcelFile(self.metadata_file_path)
        current_sheets = meta_xls.sheet_names

        if not all(sheet in current_sheets for sheet in sheet_names):
            logger.error(
                "Sheet names not set properly in metadata excel file")
            raise AutoETLException(
                "Sheet names not set properly in metadata excel file")
        return meta_xls

    @classmethod
    def get_metadata_parser(cls,
                            file_path: str) -> 'MetadataParser':
        """method to get the metadata parser object

        Args:
            file_path (str): path of the metadata file

        Returns:
            MetadataParser
        """
        return MetadataParser(file_path)


@dataclass
class ConfigParser:
    config_file_path: str

    def validate_file(self) -> Any:
        """method to validate the config Json file

        Raises:
            AutoETLException: Exception if required configs are missing

        Returns:
            Dictionary
        """
        logger.info("Validating the config file")
        if not is_valid_file(self.config_file_path):
            logger.error("File path not correct - %s", self.config_file_path)
            raise AutoETLException(
                f"File path not correct - {self.config_file_path}")

        with open(self.config_file_path) as fp:
            config_json = json.load(fp)

        if config_json.get('target', None) is None:
            logger.error(
                "Target system not set properly in config file")
            raise AutoETLException(
                "Target system not set properly in config file")
        return config_json

    @classmethod
    def get_config_parser(cls,
                          file_path: str) -> 'ConfigParser':
        """method to get the config parser object

        Args:
            file_path (str): path of the config file

        Returns:
            ConfigParser
        """
        return ConfigParser(file_path)
