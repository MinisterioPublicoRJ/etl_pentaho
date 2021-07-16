import logging
import os

import mpmapas_commons as commons
from mpmapas_exceptions import MPMapasException
import mpmapas_logger

os.environ["NLS_LANG"] = ".UTF8"


def main():
    try:
        logger.info('Starting %s.' % configs.settings.ETL_JOB)

        find_my_packages = """
import site
site.addsitedir(r'%s')
        """ % configs.settings.MPMAPAS_PYTHON_UTILS

        filename = os.path.join(configs.settings.PYTHON_SITE_PACKAGES, configs.settings.SITE_CUSTOM_FILE)

        with open(filename, 'w') as outfile:
            print(find_my_packages, file=outfile)

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
        configs = commons.read_config('./etc/settings.yml')
        mpmapas_logger.Logger.config_logger(configs, logghandler_file=True)
        logger = logging.getLogger(configs.settings.ETL_JOB)

        main()
    except Exception as excpt:
        logging.exception('Fatal error in %s' % __name__)
        exit(excpt)
