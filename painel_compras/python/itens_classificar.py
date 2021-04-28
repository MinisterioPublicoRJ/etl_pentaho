import logging
import time
from datetime import datetime, timezone

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
    operador = ''
    palavras_expressao = ''
    tp_item = ''

    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)

    # in selection rules we will start with rules that are not composed and have no exceptions,
    # these items will be classified with 1 sql command
    sql_1 = 'select r.tp_item, r.operador, r.palavras_expressao ' + \
            'from comprasrj.regras_classificacao_itens r ' + \
            'where r.composta = false order by r.ordem'
    sql_2 = "select un_item from comprasrj.itens_a_classificar where "
    sql_list_cmds = []
    result_1 = db_opengeo.execute_select(sql_1, result_mode='all')
    dt_now = datetime.now(timezone.utc)

    print('dt_now =', dt_now)
    print('sql_1 =', sql_1)

    if result_1 is not None:
        print('len(result_1) =', len(result_1))
        for r in result_1:
            tp_item = r[0]
            operador = r[1]
            palavras_expressao = r[2]
            sql_3 = "un_item %s '_P3RC3NT_%s_P3RC3NT_' " % (operador, palavras_expressao)
            sql_list_cmds.append({'tp_item': tp_item, 'sql': sql_3})

    print('sql_list_cmds =', sql_list_cmds)

    # now let's get other rules composed
    sql_1 = 'select r.tp_item, r.operador, r.palavras_expressao, r.proximo_operador, ' + \
            'r.proxima_ordem, r.id ' + \
            'from comprasrj.regras_classificacao_itens r ' + \
            'where r.composta = true ' + \
            'order by r.ordem '
    result_1 = db_opengeo.execute_select(sql_1, result_mode='all')
    sql_list_cmds_composed = {}
    if result_1 is not None:
        sql_4 = ''
        tp_item = result_1[0][0]
        sql_list_cmds_composed[tp_item] = []
        for r in result_1:
            operador = r[1]
            palavras_expressao = r[2]
            proximo_operador = r[3]
            proxima_ordem = r[4]
            if tp_item == r[0]:
                if int(proxima_ordem) == 0:
                    sql_3 = "un_item %s '_P3RC3NT_%s_P3RC3NT_' " % (operador, palavras_expressao)
                    sql_4 = sql_4 + sql_3
                    sql_list_cmds_composed[tp_item].append(sql_4)
                else:
                    sql_3 = "un_item %s '_P3RC3NT_%s_P3RC3NT_' %s " % (operador, palavras_expressao, proximo_operador)
                    sql_4 = sql_4 + sql_3
            else:
                tp_item = r[0]
                sql_list_cmds_composed[tp_item] = []
                sql_4 = ''
                if int(proxima_ordem) == 0:
                    sql_3 = "un_item %s '_P3RC3NT_%s_P3RC3NT_' " % (operador, palavras_expressao)
                    sql_4 = sql_4 + sql_3
                    sql_list_cmds_composed[tp_item].append(sql_4)
                else:
                    sql_3 = "un_item %s '_P3RC3NT_%s_P3RC3NT_' %s " % (operador, palavras_expressao, proximo_operador)
                    sql_4 = sql_4 + sql_3
    print('sql_list_cmds_composed =', sql_list_cmds_composed)
    for c in sql_list_cmds_composed:
        sql_list_cmds.append({'tp_item': c, 'sql': sql_list_cmds_composed[c][0]})
    print('sql_list_cmds =', sql_list_cmds)
    if len(sql_list_cmds) > 0:
        start_time = time.time()
        warning_interval = 180
        number_items_updated = 0
        last_time_checked = time.time() + warning_interval
        for c in sql_list_cmds:
            sql_4 = sql_2 + c['sql'].replace('_P3RC3NT_', '%')
            print('sql_4 ="', sql_4, '"')
            # let's check if there are items to classify with rules previously defined
            result_2 = db_opengeo.execute_select(sql_4, result_mode='all')
            if result_2 is not None:  # now we will update ITEM type classification
                print("Classified items counter...")
                print('SQL = "', sql_4, '"')
                print(len(result_2), 'registration number that meet the search criteria')
                sql_3 = "insert into comprasrj.itens_classificados " + \
                        "( tp_item, un_item, dt_ult_atualiz ) values ( '%s', '%s', '%s' )"
                for i in result_2:  # we will insert the classified item
                    number_items_updated = number_items_updated + 1
                    # itens_classificados table will not accept insert if ITEM already exists
                    if (time.time() > last_time_checked):
                        last_time_checked = time.time() + warning_interval
                        print(datetime.now())
                        print(number_items_updated, 'number of items updated')
                    un_item = i[0]
                    sql_4 = sql_3 % (c['tp_item'], un_item, dt_now)
                    db_opengeo.execute_select(sql_4, result_mode=None)
                    # if number_items_updated > 10:
                    #    break


def classificar_pandas():
    logger.info('Starting %s - classificar.' % configs.settings.ETL_JOB)
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    logger.info('Load table - df.')
    result_df = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj', table_name='itens_a_classificar')[
        'table']
    logger.info('count df.')
    count_df = len(result_df)
    print(count_df)
    logger.info('df rows: %s' % count_df)


def main():
    try:
        # classificar_pandas()
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
