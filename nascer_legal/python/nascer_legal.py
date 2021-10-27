import logging
import os
from datetime import datetime, timezone

import mpmapas_commons as commons
import mpmapas_logger
from mpmapas_exceptions import MPMapasException

import nascer_legal_get_data_arcgis
import nascer_legal_send_emails

os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)


def main():
    try:
        logger.info('Starting %s.' % configs.settings.ETL_JOB)
        nascer_legal_get_data_arcgis.main()
        nascer_legal_send_emails.main()

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
        logging.exception('Fatal error in %s' % __name__)
        exit(excpt)
