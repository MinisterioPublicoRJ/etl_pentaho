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


def unaccent_df(df, col):
    return df.apply(lambda x: unidecode.unidecode(str.upper(x[col])), axis=1)


def build_file_rules_classify_items(file_name_input = 'sql_1_rules_tested.sql', file_name_output = 'sql_2_insert_rules.sql'):
    if os.path.isfile(file_name_input):
        print("let's try to build sql commands for item classification rules")
        print("File input:", file_name_input)
        print("File output:", file_name_output)
        with open(file_name_input, 'r') as fr:
            file_content = fr.read()
        if len(file_content) > 0:
            c1 = '_P3RC3NT_'
            c2 = '_QU0T3_'
            c3 = '_AP0STR0PH3_'
            new_file_content = file_content.replace("'", c3).replace('"', c2).replace('%', c1)
            all_lines = new_file_content.splitlines()
            new_file_content = ''
            id_ordem = 100
            value_1 = 'ordem'
            value_2 = 'tp_item'
            value_3 = 'descricao'
            value_4 = 'sql_declaracao'
            for l in all_lines:
                if len(l) > 0 and '--' not in l:
                    value_1 = id_ordem
                    if 'select 1 as tp_item,' in l:
                        value_2 = 1
                        value_3 = 'produto'
                    elif 'select 2 as tp_item,' in l:
                        value_2 = 2
                        value_3 = 'servico'
                    if 'select 3 as tp_item,' in l:
                        value_2 = 3
                        value_3 = 'despesa'
                    value_4 = l.replace(';', '')
                    sql_1 = "insert into comprasrj.regras_classificacao_itens (ordem, tp_item, descricao, sql_declaracao) values (%s, %s, '%s', '%s');"
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
    # df_itens_a_classificar['un_item'] = unaccent_df(df=df_itens_a_classificar, col='un_item')

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
    logger.info('Load table - df.')
    df_itens_a_classificar = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj', table_name='itens_a_classificar')[
        'table']
    logger.info('count itens_a_classificar rows: %s' % len(df_itens_a_classificar))

    select_tipo_itens = 'SELECT ti.id_tipo as id_tipo, ti.fk_tipo_primario as fk_tipo_primario, ti.tp_item as tp_item ' \
                        'from comprasrj.tipo_item ti'
    df_tipo_itens = pd.DataFrame(db_opengeo.execute_select(select_tipo_itens, result_mode='all'),
                                 columns=['id_tipo', 'fk_tipo_primario', 'tp_item'])

    select_tipo_classificacao = 'SELECT tc.id_tipo as id_tipo, tc.fk_tipo_primario as fk_tipo_primario, ' \
                                'tc.tp_classificacao as tp_classificacao FROM comprasrj.tipo_classificacao tc ' \
                                'where tc.tp_classificacao = \'regras\''
    df_tipo_classificacao = pd.DataFrame(db_opengeo.execute_select(select_tipo_classificacao, result_mode='all'),
                                         columns=['id_tipo', 'fk_tipo_primario', 'tp_classificacao'])

    rule_sqls = 'SELECT r.ordem as ordem, r.descricao as descricao, r.tp_item as tp_item, r.sql_declaracao as sql_declaracao ' \
                'FROM comprasrj.regras_classificacao_itens r order by r.ordem;'
    sql_list_cmds: list[dict[str, str]] = []
    df_rules_sql = pd.DataFrame(db_opengeo.execute_select(rule_sqls, result_mode='all'),
                                columns=['ordem', 'descricao', 'tp_item', 'sql_declaracao'])

    # in selection rules we will start with rules that are not composed and have no exceptions,
    # these items will be classified with 1 sql command
    # simple_rules_sql = 'select r.tp_item as tp_item, r.operador as operador, ' \
    #                    'r.palavras_expressao as palavras_expressao ' + \
    #                    'from comprasrj.regras_classificacao_itens r ' + \
    #                    'where r.composta = false order by r.ordem'
    # sql_list_cmds: list[dict[str, str]] = []
    # df_simple_rules = pd.DataFrame(db_opengeo.execute_select(simple_rules_sql, result_mode='all'),
    #                                columns=['tp_item', 'operador', 'palavras_expressao'])

    # for index, row in df_simple_rules.iterrows():
    #     sql_list_cmds.append({'tp_item': row.tp_item,
    #                           'sql': "un_item %s '_P3RC3NT_%s_P3RC3NT_' " % (row.operador, row.palavras_expressao)})

    for index, row in df_rules_sql.iterrows():
        sql_list_cmds.append({'ordem': row.ordem, 'descricao': row.descricao,
                              'tp_item': row.tp_item, 'sql': row.sql_declaracao})

    # now let's get other rules composed
    # composed_rules_sql = 'select r.tp_item as tp_item, r.operador as operador, ' \
    #                      'r.palavras_expressao as palavras_expressao, ' \
    #                      'r.proximo_operador as proximo_operador, ' + \
    #                      'r.proxima_ordem as proxima_ordem, r.id as id ' + \
    #                      'from comprasrj.regras_classificacao_itens r ' + \
    #                      'where r.composta = true ' + \
    #                      'order by r.ordem '
    # df_composed_rules = pd.DataFrame(db_opengeo.execute_select(composed_rules_sql, result_mode='all'),
    #                                  columns=['tp_item', 'operador', 'palavras_expressao', 'proximo_operador',
    #                                           'proxima_ordem', 'id'])
    # sql_dict_cmds_composed = {}
    #
    # if len(df_composed_rules) > 0:
    #     tp_item = -1
    #     composed_cmd = ''
    #     for index, row in df_composed_rules.iterrows():
    #         if not tp_item == row.tp_item:
    #             tp_item = row.tp_item
    #             sql_dict_cmds_composed[tp_item] = []
    #             composed_cmd = ''
    #         if int(row.proxima_ordem) == 0:
    #             composed_cmd += "un_item %s '_P3RC3NT_%s_P3RC3NT_' " % (row.operador, row.palavras_expressao)
    #             sql_dict_cmds_composed[tp_item].append(composed_cmd)
    #         else:
    #             composed_cmd += "un_item %s '_P3RC3NT_%s_P3RC3NT_' %s " % (
    #                 row.operador, row.palavras_expressao, row.proximo_operador)
    #
    # for tp_iten in sql_dict_cmds_composed:
    #     sql_list_cmds.append({'tp_item': tp_iten, 'sql': sql_dict_cmds_composed[tp_iten][0]})

    if len(sql_list_cmds) > 0:
        sql_select = "select un_item from comprasrj.itens_a_classificar "
        for cmds in sql_list_cmds:
            sql_cmds = cmds['sql'].replace('_P3RC3NT_', '%').replace('_QU0T3_', '\"').replace('_AP0STR0PH3_', '\'')
            # let's check if there are items to classify with rules previously defined
            df_itens_to_classify = pd.DataFrame(db_opengeo.execute_select(sql_cmds, result_mode='all'),
                                                columns=['tp_item', 'un_item'])
            tipo_item = df_tipo_itens.loc[df_tipo_itens['id_tipo'] == int(cmds['tp_item'])]
            if not tipo_item['fk_tipo_primario'].empty and tipo_item['fk_tipo_primario'].iloc[0] > 0:
                df_itens_to_classify['fk_tp_item'] = tipo_item['fk_tipo_primario'].iloc[0]
            else:
                df_itens_to_classify['fk_tp_item'] = tipo_item['id_tipo'].iloc[0]
            df_itens_to_classify['fk_tp_classificacao'] = df_tipo_classificacao['id_tipo'].iloc[0]
            df_itens_to_classify['descricao'] = cmds['descricao']

            df_itens_to_classify['dt_ult_atualiz'] = dt_now
            df_itens_to_classify = df_itens_to_classify.drop(['tp_item'], axis='columns')
            if len(df_itens_to_classify) > 0:  # now we will update ITEM type classification
                list_flds_itens_to_classify = df_itens_to_classify.columns.values
                insert_sql_itens_to_classify, insert_template_itens_to_classify = db_opengeo.insert_values_sql(
                    schema_name='comprasrj', table_name='itens_classificados',
                    list_flds=list_flds_itens_to_classify, unique_field='un_item', pk_field='id')
                db_opengeo.execute_values_insert(
                    sql=insert_sql_itens_to_classify, template=insert_template_itens_to_classify,
                    df_values_to_execute=df_itens_to_classify, fetch=True)
                # refresh_mview_sql = "REFRESH MATERIALIZED VIEW comprasrj.itens_a_classificar;"
                # db_opengeo.execute_select(refresh_mview_sql, result_mode=None)
        logger.info('Starting REFRESH MATERIALIZED VIEW comprasrj.itens_a_classificar.')
        refresh_mview_sql = "REFRESH MATERIALIZED VIEW comprasrj.itens_a_classificar;"
        db_opengeo.execute_select(refresh_mview_sql, result_mode=None)
        logger.info('Finishing REFRESH MATERIALIZED VIEW comprasrj.itens_a_classificar.')
    logger.info('Finish %s - classificar.' % configs.settings.ETL_JOB)


def main():
    try:
        logger.info('Starting %s - Main.' % configs.settings.ETL_JOB)
        classificar()
        itens_classificar()
        logger.info('Finish %s - Main.' % configs.settings.ETL_JOB)
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
