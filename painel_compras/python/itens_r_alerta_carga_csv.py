import logging
import os, subprocess
import time
from datetime import datetime, timezone

import mpmapas_commons as commons
import mpmapas_logger
import numpy
import pandas as pd
import urllib3
from db_utils import mpmapas_db_commons as dbcommons
from mpmapas_exceptions import MPMapasErrorAccessingTable, MPMapasDataBaseException, MPMapasErrorFileNotFound, MPMapasException
from psycopg2.extensions import register_adapter, AsIs

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
pd.options.mode.chained_assignment = None  # default='warn'


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)


def export_table_to_csv(obj_jdbc, table_name,
                        file_name, folder_name, schema_name='comprasrj'):
    df_result = None
    folder_name = os.path.abspath(folder_name)
    complete_file_name = folder_name + os.sep + file_name
    logger.info('Starting [%s] - export_table_to_csv.' % complete_file_name)
    dict_table = dbcommons.load_table(configs=configs, jndi_name=obj_jdbc.jndi_name, schema_name=schema_name,
                                      table_name=table_name, csvEncoding='UTF-8',
                                      csvDelimiter=';', csvDecimal=',')
    df_result = dict_table['table']
    logger.info('Load table [%s] from schema [%s].' % (table_name, schema_name))
    commons.gravar_saida(df_result, complete_file_name)
    if os.path.isfile(complete_file_name):
        print("Finishing OK output file [%s] - export_table_to_csv." % complete_file_name)
    else:
        print("Finishing NOK output file [%s] - export_table_to_csv." % complete_file_name)
    return df_result


def import_csv_to_table(obj_jdbc, table_name, file_name, folder_name, df_chk_already_loaded,
                        schema_name='comprasrj', checksum_column='checksumid',
                        checksum_drop_column_list=['id', 'checksumid', 'dt_ult_atualiz'],
                        fillna_values_column_list={'alerta': '', 'mensagem': ''},
                        datetime_field='dt_ult_atualiz',
                        unique_field='id', pk_field='id', raise_error_if_file_does_not_exist=True):
    folder_name = os.path.abspath(folder_name)
    complete_file_name = folder_name + os.sep + file_name

    logger.info('Starting [%s] - import_csv_to_table.' % complete_file_name)

    db = commons.get_database(obj_jdbc, api=None)
    server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name)

    if os.path.isfile(complete_file_name):
        dict_1 = dbcommons.load_table(configs=configs, jndi_name=obj_jdbc.jndi_name, schema_name=schema_name,
                                      table_name=table_name, csv_source=complete_file_name, csvEncoding='UTF-8',
                                      csvDelimiter=';', csvDecimal=',')
        df = pd.DataFrame(data=dict_1['table'], columns=dict_1['table_info']['column_name'])
        if len(df) > 0:
            for f in checksum_drop_column_list:
                df = df.drop([f], axis='columns')
            df = df.fillna(fillna_values_column_list)
            df[checksum_column] = commons.generate_checksum(df)
            dt_now = datetime.now()
            df[datetime_field] = dt_now
            list_fields = df.columns.values
            # vamos separar registros que não foram enviados ainda
            df_insert = df.loc[~df[checksum_column].isin(df_chk_already_loaded[checksum_column])]
            if len(df_insert) > 0:
                insert_sql_statement, insert_template_statement = db.insert_values_sql(schema_name=schema_name,
                                                                                       table_name=table_name,
                                                                                       list_flds=list_fields,
                                                                                       unique_field=unique_field,
                                                                                       pk_field=pk_field)
                result = db.execute_values_insert(sql=insert_sql_statement,
                                                  template=insert_template_statement,
                                                  df_values_to_execute=df_insert, fetch=False,
                                                  server_encoding=server_encoding)
        logger.info('Finishing import [%s] to [%s.%s] - import_csv_to_table.' % (file_name, schema_name, table_name))
    else:
        logger.info('Finishing NOK [%s] - import_csv_to_table.' % complete_file_name)
        if raise_error_if_file_does_not_exist:
            raise MPMapasErrorFileNotFound(etl_name=configs.settings.ETL_JOB, error_name='Import CSV file not found',
                                           abs_path=folder_name, file_name=file_name)


def main():
    try:
        jdbc_opengeo = configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME]
        folder_name = configs.folders.SAIDA_DIR

        df_out_icf = export_table_to_csv(obj_jdbc=jdbc_opengeo, table_name='itens_contratos_filtrados',
                                         file_name='itens_contratos_filtrados.csv', folder_name=folder_name)

        df_out_ah = export_table_to_csv(obj_jdbc=jdbc_opengeo, table_name='alertas_historico',
                                        file_name='alertas_historico.csv', folder_name=folder_name)

        file_script_r = 'script_alerta_precos_contratos.R'
        folder_name = configs.folders.R_SCRIPT_DIR
        commons.execute_r_script(logger=logger, configs=configs, file_script_r=file_script_r,
                                 folder_name=folder_name)

        folder_name = configs.folders.SAIDA_DIR

        import_csv_to_table(obj_jdbc=jdbc_opengeo, table_name='alertas_contratos_produtos',
                            file_name='alertas_contratos_produtos.csv', folder_name=folder_name,
                            df_chk_already_loaded=df_out_ah, unique_field='id',
                            raise_error_if_file_does_not_exist=False)

        import_csv_to_table(obj_jdbc=jdbc_opengeo, table_name='alertas_historico',
                            file_name='alertas_contratos_produtos.csv', folder_name=folder_name,
                            df_chk_already_loaded=df_out_ah, unique_field='id',
                            raise_error_if_file_does_not_exist=False)

        import_csv_to_table(obj_jdbc=jdbc_opengeo, table_name='alertas_contratos_avaliacao',
                            file_name='alertas_contratos_avaliacao.csv', folder_name=folder_name,
                            df_chk_already_loaded=df_out_ah, unique_field='id',
                            raise_error_if_file_does_not_exist=False)

    except MPMapasDataBaseException as c_err:
        logger.exception(c_err)
        exit(c_err.error_code)
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)
    finally:
        logger.info('Finishing item exports to CSV in %s folder.' % configs.folders.SAIDA_DIR)


global configs, logger

if __name__ == '__main__' or __name__ == 'itens_r_alerta_carga_csv':
    try:
        configs = commons.read_config('../etc/settings.yml')
        if __name__ == '__main__':
            mpmapas_logger.Logger.config_logger(configs, logghandler_file=True)
        logger = logging.getLogger(configs.settings.ETL_JOB)
        if __name__ == '__main__':
            main()
    except Exception as excpt:
        logging.exception('Fatal error in %s' % __name__)
        exit(excpt)
