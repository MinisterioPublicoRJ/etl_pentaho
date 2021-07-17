import logging
import os
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import mpmapas_commons as commons
import mpmapas_logger
import pandas as pd
import requests
import unidecode
from mpmapas_exceptions import MPMapasException

os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)
dict_last_modified_date: dict = {}  # dict


def ascii_normalizer(text):
    import re
    import unicodedata
    text = str(text).lower()
    text = text.strip()
    text = re.sub("\s\-\s", "_", text)
    text = re.sub("\s", "_", text)
    text = ''.join(ch for ch in unicodedata.normalize('NFKD', text) if not unicodedata.combining(ch))
    return text


def normalize_text(text):
    if text and type(text) == str:
        remove_chars = '.,;:!?@#$%&*/\\<>(){}[]~^´`¨-+°ºª¹²³£¢¬\'\"'
        text = text.translate(str.maketrans('', '', remove_chars)).replace('.csv', '')
        text = unicodedata.normalize(u'NFKD', text).encode('ascii', 'ignore').decode('utf8')
        text = ''.join(ch for ch in unicodedata.normalize('NFKD', text) if not unicodedata.combining(ch))
        text = unidecode.unidecode(text.strip().strip(remove_chars).strip())
    return text


def normalize_table_name(text):
    if text and type(text) == str:
        text = str.lower(text)
        text = normalize_text(text)
    return text


def normalize_column_name(text):
    if text and type(text) == str:
        text = str.lower(text)
        text = normalize_text(text)
        text = text.replace(' ', '_')
    return text


def erase_dir(file_path):
    logger.info('erasing dir: %s !' % file_path)
    [file.unlink() for file in Path(file_path).glob("*") if file.is_file()]


def download_file(urlstr, file_path, file_nam, last_date_to_check, mode='wb'):
    logger.info('downloading url: %s !' % urlstr)
    erase_dir(file_path)
    req = requests.get(urlstr)
    with open(file_path + file_nam, mode) as file:
        file.write(req.content)
        return_new_file = True
    logger.info('file saved at %s !' % file_nam)
    return return_new_file


def truncate_table(schema, table):
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    trunc_table_sql = "TRUNCATE TABLE %s.%s CONTINUE IDENTITY CASCADE" % (schema, table)
    db_opengeo.execute_select(trunc_table_sql, result_mode=None)


def read_file(file_dir, file_nam, file_extension, header=0, encoding=None, delimiter=None):
    print('Lendo aquivo: ' + file_nam + file_extension + ' ....')
    # delimiter = '\t'
    df_result = pd.DataFrame
    if str.lower(file_extension) == 'xlsx':
        df_result = pd.read_excel(os.path.abspath(file_dir + file_nam + '.' + file_extension), na_values='-',
                                  keep_default_na=True, sheet_name=0, header=header).rename(ascii_normalizer,
                                                                                            axis='columns')
        for col in df_result.columns.values:
            if 'unnamed' in col:
                df_result = df_result.drop([col], axis='columns')
    elif str.lower(file_extension) == 'csv':
        file_pat = os.path.abspath(file_dir + file_nam + '.' + file_extension)
        if not encoding:
            encoding = commons.detect_encoding(file_pat)
        if not delimiter:
            delimiter = commons.detect_delimiter(file_pat, encoding)
        df_result = pd.read_csv(filepath_or_buffer=file_pat, header=0, delimiter=delimiter, encoding=encoding,
                                na_values='-', keep_default_na=True, dayfirst=True, decimal=',').rename(
            ascii_normalizer, axis='columns')
    return df_result


def anp_carga(df_carga, file_name):
    logger.warning('Start ANP carga file: %s...', file_name)
    """
        Import csv da ANP:
        importar os csvs baixados da ANP para as suas respectivas tabelas 
    """
    if isinstance(df_carga, pd.DataFrame) and not df_carga.empty:
        df_carga['dt_ult_atualiz'] = dt_now
        db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME],
                                          api='sqlalchemy')
        db_opengeo.df_insert(df=df_carga, schema_name='anp', table_name=file_name, if_exists='append', chunksize=1000,
                             method='multi', echo=False, dtype=True, datestyle='dmy')
        logger.warning('Finish ANP carga...')


def anp_download():
    logger.warning('Start ANP downloads...')
    result_new_data = False
    max_dt_extracao = None
    anp_files = configs.settings.ANP_FILES
    for anp_file_name in anp_files:
        Path(configs.folders.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
        for url in anp_files[anp_file_name]['url']:
            new_file = download_file(urlstr=url, file_path=configs.folders.DOWNLOAD_DIR,
                                 file_nam=anp_file_name + '.' + anp_files[anp_file_name]['tipo'],
                                 last_date_to_check=max_dt_extracao, mode='wb')
            if new_file:
                result_new_data = new_file
                df = read_file(configs.folders.DOWNLOAD_DIR, anp_file_name, anp_files[anp_file_name]['tipo'],
                               header=anp_files[anp_file_name]['header'], encoding=anp_files[anp_file_name]['encoding'],
                               delimiter=anp_files[anp_file_name]['delimiter'])
                if anp_files[anp_file_name]['filtro']:
                    for filtro_key in anp_files[anp_file_name]['filtro']:
                        df = df[df[filtro_key].isin(anp_files[anp_file_name]['filtro'][filtro_key])]
                anp_carga(df_carga=df, file_name=anp_file_name)
            erase_dir(file_path=configs.folders.DOWNLOAD_DIR)
    logger.warning('Finish ANP downloads...')
    return result_new_data


def main():
    try:
        logger.info('Starting %s.' % configs.settings.ETL_JOB)
        anp_download()

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
