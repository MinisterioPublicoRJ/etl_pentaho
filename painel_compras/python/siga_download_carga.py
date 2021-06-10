import logging
import os
import unicodedata
import zipfile
from datetime import datetime, timezone
from os import walk
from pathlib import Path

import mpmapas_commons as commons
import mpmapas_logger
import pandas as pd
import requests
import unidecode
from dateutil import tz
from db_utils import mpmapas_db_commons as dbcommons
from mpmapas_exceptions import MPMapasException

os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)
dict_last_modified_date: dict = {}  # dict


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


def download_file(urlstr, file_path, file_nam, last_date_to_check, mode='wb'):
    logger.info('erasing dir: %s !' % file_path)
    return_new_file = False
    [file.unlink() for file in Path(file_path).glob("*") if file.is_file()]
    logger.info('downloading url: %s !' % urlstr)
    # with urllib.request.urlopen(urlstr) as response, open(file=file_path + file_nam, mode=mode) as out_file:
    #     shutil.copyfileobj(response, out_file)
    # urllib.request.urlretrieve(urlstr, file_path + file_nam)
    req = requests.get(urlstr)
    with open(file_path + file_nam, mode) as file:
        last_modified_date = datetime.strptime(req.headers['last-modified'], '%a, %d %b %Y %H:%M:%S %Z').replace(
            tzinfo=tz.gettz('UTC')).astimezone(tz.tzlocal())
        if (not last_date_to_check) or (not last_modified_date) or (last_modified_date > last_date_to_check):
            file.write(req.content)
            dict_last_modified_date[file_nam] = last_modified_date
            return_new_file = True
    logger.info('file saved at %s !' % file_nam)
    return return_new_file


def unzip_file(file_path, file_name):
    logger.info('unziping file: %s !' % file_name)
    with zipfile.ZipFile(file_path + file_name, 'r') as zip_ref:
        zip_ref.extractall(file_path)
    os.remove(file_path + file_name)
    logger.info('unziped file to %s !' % file_path)


def get_max_dt_extracao(table):
    result = None
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    max_dt_extracao_sql = "select max(c.dt_extracao) as dt_extracao from comprasrj_stage.%s c" % table
    max_dt_return = db_opengeo.execute_select(max_dt_extracao_sql, result_mode='all')
    if max_dt_return and max_dt_return[0] and max_dt_return[0][0]:
        result = max_dt_return[0][0].astimezone(tz.tzlocal())
    return result


def siga_download():
    logger.warning('Start SIGA downloads...')
    siga_urls = configs.settings.SIGA_URLS
    siga_zips = configs.settings.SIGA_FILES
    for siga_zip in siga_zips:
        siga_url = siga_urls[str.lower(siga_zip)]
        siga_csvs = siga_zips[str.upper(siga_zip)]
        Path(configs.folders.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
        max_dt_extracao = None
        for csv in siga_csvs:
            dt_extracao = get_max_dt_extracao(str.lower(csv))
            if dt_extracao and ((not max_dt_extracao) or (dt_extracao > max_dt_extracao)):
                max_dt_extracao = dt_extracao
        new_file = download_file(urlstr=siga_url, file_path=configs.folders.DOWNLOAD_DIR,
                                 file_nam=siga_zip + '.zip', last_date_to_check=max_dt_extracao, mode='wb')
        if new_file:
            unzip_file(file_path=configs.folders.DOWNLOAD_DIR, file_name=siga_zip + '.zip')
            last_modified_date = dict_last_modified_date[siga_zip + '.zip']
            for (dirpath, dirnames, filenames) in walk(configs.folders.DOWNLOAD_DIR):
                for filename in filenames:
                    if '.CSV' in filename and filename.split('.')[0] in siga_csvs:
                        siga_carga(filepath=dirpath, filename=filename.replace('.CSV', ''), ext='.CSV',
                                   dt_extracao=last_modified_date)
    logger.warning('Finish SIGA downloads...')


def siga_carga(filepath, filename, ext, dt_extracao):
    logger.warning('Start SIGA carga file: %s...', filename)
    """
        Import csv do SIGA:
        importar os csvs baixados do SIGA para as suas respectivas tabelas 
    :param configs: configuracoes com a conexao de banco
    """
    dict_siga = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj_stage', table_name=str.lower(filename),
                                     csv_source=filepath + filename + ext, csvEncoding='ISO-8859-1', csvDelimiter=';')

    result_df_siga = dict_siga['table'].rename(normalize_column_name, axis='columns')
    result_df_siga = pd.DataFrame(data=result_df_siga, columns=dict_siga['table_info']['column_name'])
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name)
    if isinstance(result_df_siga, pd.DataFrame) and not result_df_siga.empty:
        result_df_siga['dt_ult_atualiz'] = dt_now
        result_df_siga['dt_extracao'] = dt_extracao
        result_df_siga = result_df_siga.drop(['id'], axis='columns')
        list_flds_siga = result_df_siga.columns.values
        trunc_siga_sql = 'TRUNCATE TABLE %s.%s CONTINUE IDENTITY CASCADE' % (
            'comprasrj_stage', normalize_table_name(filename))
        db_opengeo.execute_select(trunc_siga_sql, result_mode=None)
        insert_sql_siga, insert_template_siga = db_opengeo.insert_values_sql(schema_name='comprasrj_stage',
                                                                             table_name=normalize_table_name(filename),
                                                                             list_flds=list_flds_siga,
                                                                             unique_field='id', pk_field='id')
        db_opengeo.execute_values_insert(sql=insert_sql_siga,
                                         template=insert_template_siga,
                                         df_values_to_execute=result_df_siga,
                                         fetch=False, server_encoding=server_encoding)
        logger.warning('Finish SIGA carga...')


def main():
    try:
        logger.info('Starting SIGA download e carga para o %s.' % configs.settings.ETL_JOB)
        siga_download()
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)
    finally:
        logger.info('Finishing SIGA download e carga para o %s.' % configs.settings.ETL_JOB)


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
