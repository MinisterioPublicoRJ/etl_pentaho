import logging
import os

import mpmapas_commons as commons
import mpmapas_logger

os.environ["NLS_LANG"] = ".UTF8"

configs: commons.Configs = commons.read_config('./etc/settings.yml')  # commons.Configs
mpmapas_logger.Logger.config_logger(configs, logghandler_file=True)
# logger = logging.getLogger(__name__)
logger: logging.Logger = logging.getLogger('add_lib')  # logging.Logger
logger.warning('Starting add_lib.')

try:
    FIND_MY_PACKAGES = """
    import site
    site.addsitedir(r'%s')
    """ % configs.settings.MPMAPAS_PYTHON_UTILS

    filename = os.path.join(configs.settings.PYTHON_SITE_PACKAGES, configs.settings.SITE_CUSTOM_FILE)

    with open(filename, 'w') as outfile:
        print(FIND_MY_PACKAGES, file=outfile)
except Exception as err:
    logger.exception('Error in add_lib')
    exit(err)

logger.warning('Finishing add_lib.')
