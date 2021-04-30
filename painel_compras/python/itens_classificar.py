import logging
from datetime import datetime, timezone

import mpmapas_commons as commons
import mpmapas_logger
import numpy
import pandas as pd
import pandasql as ps
import unidecode
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


def unaccent_df(df):
    return df.apply(lambda x: unidecode.unidecode(str.upper(x.un_item)), axis=1)


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

    df_itens_a_classificar = df_item_contrato[['ID', 'ITEM']].rename(columns={'ITEM': 'un_item'})
    df_itens_a_classificar['un_item'] = unaccent_df(df_itens_a_classificar)

    sql_select = "select iac.\"ID\" as id_item, iac.un_item as un_item, ic.tp_item as id_tipo " \
                 "from df_itens_a_classificar iac inner join df_itens_classificados ic on ic.un_item = iac.un_item"
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


def classificar():
    logger.info('Starting %s - classificar.' % configs.settings.ETL_JOB)
    dt_now = datetime.now(timezone.utc)
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    logger.info('Load table - df.')
    df_itens_a_classificar = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj', table_name='itens_a_classificar')[
        'table']
    logger.info('count itens_a_classificar rows: %s' % len(df_itens_a_classificar))

    # in selection rules we will start with rules that are not composed and have no exceptions,
    # these items will be classified with 1 sql command
    simple_rules_sql = 'select r.tp_item as tp_item, r.operador as operador, ' \
                       'r.palavras_expressao as palavras_expressao ' + \
                       'from comprasrj.regras_classificacao_itens r ' + \
                       'where r.composta = false order by r.ordem'
    sql_list_cmds: list[dict[str, str]] = []
    df_simple_rules = pd.DataFrame(db_opengeo.execute_select(simple_rules_sql, result_mode='all'),
                                   columns=['tp_item', 'operador', 'palavras_expressao'])

    for index, row in df_simple_rules.iterrows():
        sql_list_cmds.append({'tp_item': row.tp_item,
                              'sql': "un_item %s '_P3RC3NT_%s_P3RC3NT_' " % (row.operador, row.palavras_expressao)})

    # now let's get other rules composed
    composed_rules_sql = 'select r.tp_item as tp_item, r.operador as operador, ' \
                         'r.palavras_expressao as palavras_expressao, ' \
                         'r.proximo_operador as proximo_operador, ' + \
                         'r.proxima_ordem as proxima_ordem, r.id as id ' + \
                         'from comprasrj.regras_classificacao_itens r ' + \
                         'where r.composta = true ' + \
                         'order by r.ordem '
    df_composed_rules = pd.DataFrame(db_opengeo.execute_select(composed_rules_sql, result_mode='all'),
                                     columns=['tp_item', 'operador', 'palavras_expressao', 'proximo_operador',
                                              'proxima_ordem', 'id'])
    sql_dict_cmds_composed = {}

    if len(df_composed_rules) > 0:
        tp_item = -1
        composed_cmd = ''
        for index, row in df_composed_rules.iterrows():
            if not tp_item == row.tp_item:
                tp_item = row.tp_item
                sql_dict_cmds_composed[tp_item] = []
                composed_cmd = ''
            if int(row.proxima_ordem) == 0:
                composed_cmd += "un_item %s '_P3RC3NT_%s_P3RC3NT_' " % (row.operador, row.palavras_expressao)
                sql_dict_cmds_composed[tp_item].append(composed_cmd)
            else:
                composed_cmd += "un_item %s '_P3RC3NT_%s_P3RC3NT_' %s " % (
                    row.operador, row.palavras_expressao, row.proximo_operador)

    for tp_iten in sql_dict_cmds_composed:
        sql_list_cmds.append({'tp_item': tp_iten, 'sql': sql_dict_cmds_composed[tp_iten][0]})

    if len(sql_list_cmds) > 0:
        sql_select = "select un_item from comprasrj.itens_a_classificar where "
        for cmds in sql_list_cmds:
            sql_cmds = sql_select + cmds['sql'].replace('_P3RC3NT_', '%')
            # let's check if there are items to classify with rules previously defined
            df_itens_to_classify = pd.DataFrame(db_opengeo.execute_select(sql_cmds, result_mode='all'),
                                                columns=['un_item'])
            df_itens_to_classify['tp_item'] = cmds['tp_item']
            df_itens_to_classify['dt_ult_atualiz'] = dt_now
            if len(df_itens_to_classify) > 0:  # now we will update ITEM type classification
                list_flds_itens_to_classify = df_itens_to_classify.columns.values
                insert_sql_itens_to_classify, insert_template_itens_to_classify = db_opengeo.insert_values_sql(
                    schema_name='comprasrj', table_name='itens_classificados',
                    list_flds=list_flds_itens_to_classify, unique_field='un_item', pk_field='id')
                db_opengeo.execute_values_insert(
                    sql=insert_sql_itens_to_classify, template=insert_template_itens_to_classify,
                    df_values_to_execute=df_itens_to_classify, fetch=True)


def main():
    try:
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
