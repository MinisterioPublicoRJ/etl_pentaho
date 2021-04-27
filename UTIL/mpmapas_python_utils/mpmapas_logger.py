# -*- coding: utf-8 -*-
import logging


class Logger:
    @staticmethod
    def config_logger(configs, loggfmt_filename=False, loggfmt_modulename=False, loggfmt_name=True,
                      logghandler_file=False, logghandler_http=False):
        try:
            log_format = '%(asctime)s,%(msecs)d - %(levelname)s - '
            if loggfmt_filename:
                log_format += '%(filename)s.'
            if loggfmt_modulename:
                log_format += '%(module)s-'
            if loggfmt_name:
                log_format += '%(name)s - '
            log_format += '%(message)s'
            default_time_format = '%Y-%m-%d %H:%M:%S'
            default_msec_format = '%s,%03d'
            log_formatter = logging.Formatter(log_format)
            log_formatter.datefmt = default_time_format
            log_formatter.default_msec_format = default_msec_format
            loglevel = configs.settings.LOG_LEVEL
            logging.basicConfig(format=log_format, level=loglevel, datefmt=default_time_format)
            root_logger = logging.getLogger()

            if logghandler_file:
                file_handler = logging.FileHandler(
                    filename="{0}/{1}".format(configs.folders.LOG_DIR, configs.settings.LOG_FILE_NAME),
                    mode=configs.settings.LOG_MODE, encoding='utf-8')
                file_handler.setFormatter(log_formatter)
                file_handler.setLevel(loglevel)
                root_logger.addHandler(file_handler)

            # TODO: HTTPHandler
        except AttributeError:
            logging.error("Required attribute for config_logger not found.", exc_info=True)
            exit()
        except Exception:
            logging.exception("Fatal error in config_logger")
            exit()
