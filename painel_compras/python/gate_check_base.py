import hashlib
import logging
import os
from datetime import datetime, timezone

import mpmapas_commons as commons
import mpmapas_logger
import pandas as pd
from db_utils import mpmapas_db_commons as dbcommons
from mpmapas_exceptions import MPMapasException

os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)


def count_table(configs, jndi_name, schema_name, table_name):
    database = commons.get_database(configs.settings.JDBC_PROPERTIES[jndi_name], api=None)
    count_sql = "SELECT count(*) as count FROM \"" + schema_name + "\".\"" + table_name + "\";"
    df_fields = pd.DataFrame({'table_schema': [schema_name], 'table_name': [table_name],
                              'row_count': [database.execute_select(count_sql, result_mode='all')[0][0]],
                              'dt_count': [datetime.now(timezone.utc)]})

    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    last_count_sql = "SELECT rc.row_count from comprasrj.table_row_count rc where rc.table_schema = \'" + schema_name \
                     + "\' and rc.table_name = \'" + table_name + "\' order by rc.dt_count desc limit 1"
    last_count = db_opengeo.execute_select(last_count_sql, result_mode='all')
    if len(last_count) > 0:
        row_count_changed = True if last_count[0][0] != df_fields['row_count'][0] else False
    else:
        row_count_changed = True
    df_fields['row_count_changed'] = row_count_changed
    return df_fields


def check_table_row_count(configs, jndi_name, schema_name, table_name):
    logger.info('Checking for table %s different rows count.' % (schema_name + '.' + table_name))
    count_table_rows = count_table(configs=configs, jndi_name=jndi_name, schema_name=schema_name, table_name=table_name)
    count_table_rows['dt_ult_atualiz'] = datetime.now(timezone.utc)

    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    list_flds = count_table_rows.columns.values
    insert_sql, insert_template = db_opengeo.insert_values_sql(schema_name='comprasrj',
                                                               table_name='table_row_count',
                                                               list_flds=list_flds,
                                                               pk_field='id')
    db_opengeo.execute_values_insert(sql=insert_sql,
                                     template=insert_template,
                                     df_values_to_execute=count_table_rows,
                                     fetch=True, server_encoding='utf-8')
    return count_table_rows['row_count_changed']


def table_to_str(row, text):
    text = text.join(row.astype(str).values.sum())
    return text


def check_table_checksum(configs, jndi_name, schema_name, table_name):
    logger.info('Checking for table %s different data.' % (schema_name + '.' + table_name))
    df_table = \
        dbcommons.load_table(configs=configs, jndi_name=jndi_name, schema_name=schema_name, table_name=table_name)[
            'table']

    checksum = ''
    text_check = ''
    text_check = text_check.join(df_table.apply(lambda x: table_to_str(x, checksum), axis=1))
    checksum = hashlib.md5(text_check.encode('utf-8')).hexdigest()

    dt_check = datetime.now(timezone.utc)
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    last_check_sql = "SELECT tc.checksumid from comprasrj.table_check tc where tc.table_schema = \'" + schema_name + \
                     "\' and tc.table_name = \'" + table_name + "\' order by tc.dt_check desc limit 1"
    last_check = db_opengeo.execute_select(last_check_sql, result_mode='all')
    if len(last_check) > 0:
        checksumid_changed = True if last_check[0][0] != checksum else False
    else:
        checksumid_changed = True
    dt_ult_atualiz = datetime.now(timezone.utc)
    df_table_checksum = pd.DataFrame({'table_schema': schema_name, 'table_name': table_name, 'checksumid': checksum,
                                      'checksumid_changed': checksumid_changed, 'dt_check': dt_check,
                                      'dt_ult_atualiz': dt_ult_atualiz},
                                     columns=['table_schema', 'table_name', 'checksumid', 'checksumid_changed',
                                              'dt_check', 'dt_ult_atualiz'], index=[0])
    list_flds = df_table_checksum.columns.values
    insert_sql, insert_template = db_opengeo.insert_values_sql(schema_name='comprasrj',
                                                               table_name='table_check',
                                                               list_flds=list_flds,
                                                               pk_field='id')
    db_opengeo.execute_values_insert(sql=insert_sql,
                                     template=insert_template,
                                     df_values_to_execute=df_table_checksum,
                                     fetch=True, server_encoding='utf-8')
    return checksumid_changed


def check_atualizacao_bases_gate(run_painel_compras, diff_check_table, diff_count_table):
    logger.info('Starting check_atualizacao_bases_gate')
    jdbc_gate = configs.settings.JDBC_PROPERTIES[configs.settings.DB_GATE_DS_NAME]
    schema_gate = 'stage'
    table_compra_gate = 'ComprasRJ_Compra'
    table_contrato_gate = 'ComprasRJ_Contrato'
    table_item_gate = 'ComprasRJ_ItemContrato'
    if not run_painel_compras and diff_check_table:
        diff_check_compra = check_table_checksum(configs=configs, jndi_name=jdbc_gate.jndi_name,
                                                 schema_name=schema_gate, table_name=table_compra_gate)
        diff_check_contrato = check_table_checksum(configs=configs, jndi_name=jdbc_gate.jndi_name,
                                                   schema_name=schema_gate, table_name=table_contrato_gate)
        diff_check_item = check_table_checksum(configs=configs, jndi_name=jdbc_gate.jndi_name,
                                               schema_name=schema_gate, table_name=table_item_gate)
        if diff_check_contrato or diff_check_item or diff_check_compra:
            logger.info('Different data found.')
            run_painel_compras = True
        else:
            logger.info('No different data found.')
            run_painel_compras = False
    elif not run_painel_compras and diff_count_table:
        diff_count_compra = check_table_row_count(configs=configs, jndi_name=jdbc_gate.jndi_name,
                                                  schema_name=schema_gate, table_name=table_compra_gate)
        diff_count_contrato = check_table_row_count(configs=configs, jndi_name=jdbc_gate.jndi_name,
                                                    schema_name=schema_gate, table_name=table_contrato_gate)
        diff_count_item = check_table_row_count(configs=configs, jndi_name=jdbc_gate.jndi_name,
                                                schema_name=schema_gate, table_name=table_item_gate)
        if diff_count_contrato[0] or diff_count_item[0] or diff_count_compra[0]:
            logger.info('Different rows count found.')
            run_painel_compras = True
        else:
            logger.info('No different rows count found.')
            run_painel_compras = False
    return run_painel_compras


def main(run_painel_compras=True, diff_check_table=False, diff_count_table=False):
    try:
        logger.info('Starting gate_check_base para o %s.' % configs.settings.ETL_JOB)
        return check_atualizacao_bases_gate(run_painel_compras, diff_check_table, diff_count_table)
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)
    finally:
        logger.info('Finishing gate_check_base para o %s.' % configs.settings.ETL_JOB)


global configs, logger

if __name__ == '__main__' or __name__ == 'gate_check_base':
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
