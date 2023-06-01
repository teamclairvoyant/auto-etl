import json
import logging
import os
from typing import Dict, List

import pandas as pd
from pydantic import Json

from app.etl_exceptions import AutoETLException

logger = logging.getLogger(__name__)

JOIN_TYPES = ('inner join', 'left join', 'right join',
              'cross join', 'full outer join')

transform_format = '{func}({exp})'


def is_valid_file(file_path: str) -> bool:
    """ method to check if a file path is valid or not

    Args:
        file_path (str): path to file
    """

    if not os.path.exists(file_path):
        return False
    return True


def excel_to_json(excel_file: pd.ExcelFile, sheet_name: str, _orient) -> Json:
    """method to parse excel to json

    Args:
        excel_file (ExcelFile): name of the excel file
        sheet_name (str): name of the sheet

    Returns:
        json: returns json object of the excel file
    """
    logger.debug('parsing excel to json')
    if sheet_name == "select_sources":
        select_conf = json.loads(pd.read_excel(
            excel_file, sheet_name).to_json(orient=_orient))
        return construct_select_transform(select_conf)
    return json.loads(pd.read_excel(excel_file, sheet_name).to_json(orient=_orient))


def construct_select_transform(select_conf: List[Dict]) -> List[Dict]:
    """method to construct the select transformation conf

    Args:
        select_conf (List[Dict]): select transformations from excel mapping

    Returns:
        List[Dict]
    """
    _conf = []
    for _select in select_conf:
        _select['transformation'] = transform_format.format(
            func=_select['transformation'], exp=_select['arguments'])
        _conf.append(_select)
    return _conf


def validate_joins_mapping(joins_and_filters_conf: Dict[str, Dict]) -> None:
    """Method to validate the joins mapping conf

    Args:
        joins_and_filters_conf (List): Joins configurations loaded from metadata excel file
    """

    logger.info('validating and processing joins mapping conf')
    num_joins = len(joins_and_filters_conf)
    logger.info(
        'found %d mappings in the configurations', num_joins)
    for _index in joins_and_filters_conf:
        _map = joins_and_filters_conf[_index]
        if _index == 0:
            if _map['driving_table'] is None or \
                    (_map['reference_table'] is None and _map['reference_subquery'] is None):
                logger.error(
                    'joins and filters not provided properly. Please validate the mappings file')
                raise AutoETLException(
                    'joins and filters not provided properly. Please validate the mappings file')

        else:
            if _map['reference_table'] is None and _map['reference_subquery'] is None:
                logger.error(
                    'joins and filters not provided properly. Please validate the mappings file')
                raise AutoETLException(
                    'joins and filters not provided properly. Please validate the mappings file')
        if _map['join_type'] is None or _map['join_type'] not in JOIN_TYPES:
            logger.error(
                'Either Join type is null or not supported. Please check the mappings file')
            raise AutoETLException(
                'Either Join type is null or not supported. Please check the mappings file')
