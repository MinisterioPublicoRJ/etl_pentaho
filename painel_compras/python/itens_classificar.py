import logging

import mpmapas_commons as commons
import mpmapas_logger
import numpy
import pandas as pd
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


def classificar():
    logger.info('Starting %s - classificar.' % configs.settings.ETL_JOB)
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    logger.info('Load table - df.')
    df_itens_a_classificar = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj', table_name='itens_a_classificar')[
        'table']
    logger.info('count itens_a_classificar rows: %s' % len(df_itens_a_classificar))

    # in selection rules we will start with rules that are not composed and have no exceptions,
    # these items will be classified with 1 sql command
    simple_rules_sql = 'select r.tp_item as tp_item, r.operador as operador, r.palavras_expressao as palavras_expressao ' + \
                       'from comprasrj.regras_classificacao_itens r ' + \
                       'where r.composta = false order by r.ordem'
    sql_list_cmds: list[dict[str, str]] = []
    df_simple_rules = pd.DataFrame(db_opengeo.execute_select(simple_rules_sql, result_mode='all'),
                                   columns=['tp_item', 'operador', 'palavras_expressao'])

    for index, row in df_simple_rules.iterrows():
        sql_list_cmds.append({'tp_item': row.tp_item,
                              'sql': "un_item %s '_P3RC3NT_%s_P3RC3NT_' " % (row.operador, row.palavras_expressao)})

    # now let's get other rules composed
    composed_rules_sql = 'select r.tp_item as tp_item, r.operador as operador, r.palavras_expressao as palavras_expressao, r.proximo_operador as proximo_operador, ' + \
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
