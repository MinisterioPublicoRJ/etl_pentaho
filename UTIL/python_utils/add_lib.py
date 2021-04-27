import os

import mpmapas_commons as commons

os.environ["NLS_LANG"] = ".UTF8"


configs = commons.read_config('./etc/settings.yml')

FIND_MY_PACKAGES = """
import site
site.addsitedir(r'%s')
""" % configs.settings.PYTHON_UTILS

filename = os.path.join(configs.settings.PYTHON_SITE_PACKAGES, configs.settings.SITE_CUSTOM_FILE)

with open(filename, 'w') as outfile:
	print(FIND_MY_PACKAGES, file=outfile)
