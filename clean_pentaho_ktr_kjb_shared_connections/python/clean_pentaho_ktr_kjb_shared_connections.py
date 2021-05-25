import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from os import walk

import mpmapas_commons as commons
import mpmapas_logger
from mpmapas_exceptions import MPMapasException


os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)


def clean_pentaho_shared_connections():
    logger.info('Starting clean_pentaho_shared_connections')
    for (dirpath, dirnames, filenames) in walk(configs.settings.PENTAHO_FILES_DIR):
        for filename in filenames:
            for dict_ext in configs.settings.PENTAHO_FILES_EXTENSIONS:
                for ext in dict_ext:
                    if filename.endswith(ext):
                        full_file_name = dirpath + '\\' + filename
                        logger.info(full_file_name)
                        tree = ET.parse(full_file_name)
                        root = tree.getroot()
                        for dict_tag in dict_ext[ext]:
                            for tag in dict_tag:
                                for target_tag in root.iter(tag):
                                    remove_tag = dict_tag[tag]
                                    for remove in target_tag.findall(remove_tag):
                                        target_tag.remove(remove)
                        tree.write(full_file_name, encoding='utf-8', xml_declaration=True, method='xml')
    logger.info('Finishing clean_pentaho_shared_connections')


def main():
    try:
        logger.info('Starting %s.' % configs.settings.ETL_JOB)
        clean_pentaho_shared_connections()
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)
    finally:
        logger.info('Finishing %s.' % configs.settings.ETL_JOB)


global configs, logger

if __name__ == '__main__':
    try:
        configs = commons.read_config('../etc/settings.yml')
        mpmapas_logger.Logger.config_logger(configs, logghandler_file=True)
        logger = logging.getLogger(configs.settings.ETL_JOB)
        main()
    except Exception as excpt:
        logging.exception('Fatal error in __main__')
        exit(excpt)
