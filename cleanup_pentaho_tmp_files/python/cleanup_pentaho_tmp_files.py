import logging
import os
import time
from os import listdir
from os.path import isfile, join

import mpmapas_commons as commons
import mpmapas_logger
from mpmapas_exceptions import MPMapasException

os.environ["NLS_LANG"] = ".UTF8"


def main():
    try:
        logger.info('Starting %s.' % configs.settings.ETL_JOB)
        limit_in_seconds = time.time() - (configs.settings.DAYS_LIMIT * 24 * 60 * 60)
        extensions = configs.settings.PENTAHO_TMP_EXTENSIONS
        total_size: int = 0
        total_files: int = 0
        for tmp_dir in configs.settings.PENTAHO_TMP_DIRS:
            file_path = join(configs.settings.PENTAHO_DIR, tmp_dir)
            if os.path.exists(file_path):
                files = [join(file_path, file) for file in listdir(file_path) if isfile(join(file_path, file)) and
                         os.path.splitext(join(file_path, file))[1] in extensions]
                logger.debug('files: %s.' % files)
                for file in files:
                    if os.stat(file).st_mtime <= limit_in_seconds:
                        size = os.stat(file).st_size
                        total_size += size
                        total_files += 1
                        os.remove(file)
                        logger.info('file removed: %s. size: %s' % (file, size))
        logger.info('Finishing %s. %s files removed, cleared %f MBs' % (
            configs.settings.ETL_JOB, total_files, (total_size / 1024 / 1024)))
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)


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
