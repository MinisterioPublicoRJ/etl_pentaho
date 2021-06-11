import logging
import os
import time
from datetime import datetime, timezone

import mpmapas_commons as commons
import mpmapas_logger
import numpy
import pandas as pd
import pandasql as ps
import urllib3
from db_utils import mpmapas_db_commons as dbcommons
from mpmapas_exceptions import MPMapasDataBaseException, MPMapasException
from psycopg2.extensions import register_adapter, AsIs

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
pd.options.mode.chained_assignment = None  # default='warn'


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)


def build_file_rules_classify_items(file_name_input='sql_1_rules_tested.sql',
                                    file_name_output='sql_2_insert_rules.sql'):
    if os.path.isfile(file_name_input):
        print("let's try to build sql commands for item classification rules")
        print("File input: file_name_input='%s'" % file_name_input)
        print("File output: file_name_output='%s'" % file_name_output)
        with open(file_name_input, 'r') as fr:
            file_content = fr.read()
        if len(file_content) > 0:
            c1 = '_P3RC3NT_'
            c2 = '_QU0T3_'
            c3 = '_AP0STR0PH3_'
            new_file_content = file_content.replace("'", c3).replace('"', c2).replace('%', c1)
            all_lines = new_file_content.splitlines()
            new_file_content = 'delete from comprasrj.regras_classificacao_itens;\n'
            id_ordem = 100
            value_1 = 'ordem'
            value_2 = 'produto'
            value_3 = 'descricao'
            value_4 = 'sql_declaracao'
            for l in all_lines:
                if len(l) > 0 and '--' not in l:
                    value_1 = id_ordem
                    if ('set tp_item = %sproduto%s,' % (c3, c3)) in l:
                        value_2 = 'produto'
                        value_3 = 'produto'
                    elif ('set tp_item = %sservico%s,' % (c3, c3)) in l:
                        value_2 = 'servico'
                        value_3 = 'servico'
                    if ('set tp_item = %sdespesa%s,' % (c3, c3)) in l:
                        value_2 = 'despesa'
                        value_3 = 'despesa'
                    value_4 = l.replace(';', '').replace('__AUTO_NUMERAR_ORDEM__', '%s' % id_ordem)
                    sql_1 = "insert into comprasrj.regras_classificacao_itens (ordem, tp_item, descricao, sql_declaracao) values (%s, '%s', '%s', '%s');"
                    sql_cmd = sql_1 % (value_1, value_2, value_3, value_4)
                    new_file_content = new_file_content + sql_cmd + '\n'
                    id_ordem = id_ordem + 100
            if os.path.isfile(file_name_output):
                print("The output file already exists so it will be overwritten!")
            with open(file_name_output, 'w') as fw:
                fw.write(new_file_content)
            if os.path.isfile(file_name_output):
                print("Output file updated successfully.")
    else:
        print('there is no %s sql command file with item classification rules' % file_name_input)


def itens_classificar():
    logger.info('Starting %s - itens_classificar.' % configs.settings.ETL_JOB)
    dt_now = datetime.now(timezone.utc)
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    logger.info('Load table - df.')
    df_item_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj', table_name='item_contrato')[
        'table']
    logger.info('count item_contrato rows: %s' % len(df_item_contrato))
    df_itens_classificados = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj', table_name='itens_classificados')[
        'table']
    logger.info('count df_itens_classificados rows: %s' % len(df_itens_classificados))

    df_itens_classificar_tipo = df_item_contrato[['ID', 'un_item']].rename(columns={'ID': 'id'})

    sql_select = "select ict.id as id_item, ict.un_item as un_item, ic.fk_tp_item as id_tipo " \
                 "from df_itens_classificar_tipo ict inner join df_itens_classificados ic on ic.un_item = ict.un_item"
    df_itens_class_to_insert = pd.DataFrame(ps.sqldf(sql_select), columns=['id_item', 'un_item', 'id_tipo'])
    df_itens_class_to_insert['dt_ult_atualiz'] = dt_now
    df_itens_class_to_insert = df_itens_class_to_insert.rename(
        columns={'id_item': 'id_item_contrato', 'id_tipo': 'id_tp_item'}).drop(
        ['un_item'], axis='columns')

    list_flds_itens_class_to_insert = df_itens_class_to_insert.columns.values
    trunc_item_contrato_tipo_sql = "TRUNCATE TABLE comprasrj.item_contrato_tipo CONTINUE IDENTITY RESTRICT"
    db_opengeo.execute_select(trunc_item_contrato_tipo_sql, result_mode=None)
    insert_sql_itens_class_to_insert, insert_template_itens_class_to_insert = db_opengeo.insert_values_sql(
        schema_name='comprasrj', table_name='item_contrato_tipo',
        list_flds=list_flds_itens_class_to_insert, unique_field='id', pk_field='id')
    db_opengeo.execute_values_insert(
        sql=insert_sql_itens_class_to_insert, template=insert_template_itens_class_to_insert,
        df_values_to_execute=df_itens_class_to_insert, fetch=True)
    logger.info('Starting REFRESH MATERIALIZED VIEW comprasrj.itens_a_classificar.')
    refresh_mview_sql = "REFRESH MATERIALIZED VIEW comprasrj.itens_a_classificar;"
    db_opengeo.execute_select(refresh_mview_sql, result_mode=None)
    logger.info('Finishing REFRESH MATERIALIZED VIEW comprasrj.itens_a_classificar.')
    logger.info('Finish %s - itens_classificar.' % configs.settings.ETL_JOB)


def classificar():
    logger.info('Starting %s - classificar.' % configs.settings.ETL_JOB)
    dt_now = datetime.now(timezone.utc)
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    logger.info('Load table - df_rules_sql.')
    rule_sqls = 'SELECT r.id as id, r.ordem as ordem, r.descricao as descricao, r.tp_item as tp_item, ' \
                'r.sql_declaracao as sql_declaracao, r.cod_autorizacao as cod_autorizacao ' \
                'FROM comprasrj.regras_classificacao_itens r order by r.ordem;'
    df_rules_sql = pd.DataFrame(db_opengeo.execute_select(rule_sqls, result_mode='all'),
                                columns=['id', 'ordem', 'descricao', 'tp_item', 'sql_declaracao', 'cod_autorizacao'])
    for index, row in df_rules_sql.iterrows():
        if row.cod_autorizacao is None:  # TODO:
            logger.info('Invalid authorization code for executing SQL statement [rule id = %s, ordem = %s].' % (
                row.id, row.ordem))
        else:
            sql_cmd = row.sql_declaracao.replace('_P3RC3NT_', '%').replace('_QU0T3_', '\"').replace('_AP0STR0PH3_',
                                                                                                    '\'')
            if len(sql_cmd) > 0:
                t0 = time.time()
                logger.info('Starting execution SQL statement for [rule id = %s, ordem = %s].' % (row.id, row.ordem))
                db_opengeo.execute_select(sql_cmd, result_mode=None)
                t1 = time.time()
                logger.info('Finishing execution SQL statement for [rule id = %s, ordem = %s]. It took %s sec' % (
                    row.id, row.ordem, int(t1 - t0)))
            else:
                logger.info('Error: empty SQL statement for [rule id = %s, ordem = %s].' % (row.id, row.ordem))
    logger.info('Finish %s - classificar.' % configs.settings.ETL_JOB)


def main():
    try:
        logger.info('Starting classificacao de itens para o %s.' % configs.settings.ETL_JOB)
        classificar()
        itens_classificar()
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
        logger.info('Finishing classificacao de itens para o %s.' % configs.settings.ETL_JOB)


global configs, logger

if __name__ == '__main__' or __name__ == 'itens_classificar':
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
