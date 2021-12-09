import logging
import os
import re
import unicodedata
from datetime import datetime, timezone
from os import walk

import mpmapas_commons as commons
import mpmapas_logger
import unidecode
from db_utils import mpmapas_db_commons as dbcommons
from mpmapas_exceptions import MPMapasException

os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)


def normalize_text(text, isdir=False, isfile=False, isalphanum=True):
    if text and type(text) == str:
        remove_chars = ',;:!?@#$%&*\\<>(){}[]~^´`¨-+°ºª¹²³£¢¬\'\"'
        if not isdir:
            remove_chars += '/'
        if not isfile:
            remove_chars += '.'
        if not isalphanum:
            text = re.sub(r'\d', '', text)
        text = text.translate(str.maketrans('', '', remove_chars)).replace('.csv', '')
        text = unicodedata.normalize(u'NFKD', text).encode('ascii', 'ignore').decode('utf8')
        text = ''.join(ch for ch in unicodedata.normalize('NFKD', text) if not unicodedata.combining(ch))
        text = unidecode.unidecode(text.strip().strip(remove_chars).strip())
    return text


def normalize_column_name(text, isdir=False, isfile=False, isalphanum=True):
    if text and type(text) == str:
        text = str.lower(text).strip().replace(' - ', '_').replace(' ', '_').replace('___', '_').replace('__',
                                                                                                         '_').strip().strip(
            "-_").strip()
        text = normalize_text(text=text, isdir=isdir, isfile=isfile, isalphanum=isalphanum)
    return text


def copy_rename_files():
    dir_csv = configs.settings.DIR_CSVS
    for (dirpath, dirnames, filenames) in walk(dir_csv):
        for filename in filenames:
            if '.csv' in filename and filename in configs.settings.FILES_LIST and 'lock' not in filename:
                new_file_name = normalize_column_name(text=filename, isdir=True, isfile=True, isalphanum=True)
                df_result = commons.read_csv(dirpath + '/' + filename, decimal='.', dayfirst=True, encoding='UTF-8',
                                             delimiter=';').rename(normalize_column_name, axis='columns')
                commons.gravar_saida(df_result, configs.folders.ENTRADA_DIR + new_file_name + '.csv', sep=';',
                                     encoding='utf-8')


def exec_r_for_rh():
    logger.info('inicio exec_r_for_rh: vai executar o Rscript pro painel RH.')
    commons.execute_r_script(logger=logger, configs=configs, file_script_r=configs.settings.FILE_SCRIPT_R,
                             folder_name=configs.folders.R_SCRIPT_DIR)
    logger.info('fim exec_r_for_rh.')


def update_bd_for_rh():
    logger.info('inicio update_bd_for_rh: vai atualizar o banco pro painel RH.')
    db_opengeo_alchemy = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME])
    dict_carga_trabalho_rh = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='rh', table_name='carga_trabalho_rh',
                                                  csv_source=configs.folders.SAIDA_DIR + 'carga_trabalho_rh.csv',
                                                  csvDecimal=',')
    result_df_carga_trabalho_rh = dict_carga_trabalho_rh['table']
    db_opengeo_alchemy.df_insert(df=result_df_carga_trabalho_rh, schema_name='rh', table_name='carga_trabalho_rh',
                                 if_exists='append', index=False, index_label=None, echo=False,
                                 dtype=result_df_carga_trabalho_rh.dtypes)
    logger.info('fim update_bd_for_rh.')


def main():
    try:
        logger.info('Starting %s.' % configs.settings.ETL_JOB)
        logger.info('Exec.: %s.' % configs.settings.AMB_EXEC)
        copy_rename_files()
        exec_r_for_rh()
        update_bd_for_rh()

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
