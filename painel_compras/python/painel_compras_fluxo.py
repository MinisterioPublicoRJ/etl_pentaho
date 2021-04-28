import getopt
import hashlib
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone

import mpmapas_commons as commons
import mpmapas_logger
import numpy as np
import pandas as pd
from db_utils import mpmapas_db_commons as dbcommons
from mpmapas_exceptions import MPMapasErrorAccessingTable, MPMapasErrorCheckingChangesTableStructure, \
    MPMapasErrorGettingTableStructure, MPMapasErrorThereAreNoRecordsInTable, MPMapasDataBaseException, MPMapasException

os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)


def get_info_table_structure(obj_jdbc, schema_name, table_name, raise_error_if_changes=True):
    logger.debug('get_info_table_structure')
    ds_name = obj_jdbc.jndi_name
    json_table_info = {'dt': '%s' % int(time.time()), 'ds_name': ds_name, 'schema_name': schema_name,
                       'table_name': table_name, 'fields': {}}
    db = commons.get_database(obj_jdbc, api='postgresql')
    list_fields = ['table_name', 'column_name', 'data_type', 'ordinal_position', 'column_default', 'is_nullable',
                   'numeric_precision', 'numeric_scale']
    select_sql = "SELECT %s FROM information_schema.columns WHERE table_name = '%s';" % (
        ", ".join(i for i in list_fields), table_name)
    df_result = pd.DataFrame(db.execute_select(select_sql, result_mode='all'), columns=list_fields)
    # TODO: psycopg2.OperationalError: could not connect to server: Operation timed out
    if len(df_result) == 0:
        if raise_error_if_changes:
            print(ds_name, schema_name, table_name)
            raise MPMapasErrorAccessingTable(connection=ds_name, schema=schema_name, table=table_name)
    else:
        for recno, row in df_result.iterrows():
            json_table_info['fields'][recno] = {}
            for c in df_result.columns:
                if 'table_name' not in c:
                    str_tmp = "%s" % row[c]
                    if 'nan' in str_tmp.lower():
                        json_table_info['fields'][recno][c] = None
                    else:
                        json_table_info['fields'][recno][c] = row[c]
    return json_table_info, df_result


def checking_changes_tables_structure_0(obj_jdbc, ds_name, schema_name, table_name, raise_error_if_changes=True):
    logger.debug('checking_changes_tables_structure_0')
    json_table_info_0 = {'dt': '%s' % int(time.time()), 'table_name': '', 'fields': {}}
    json_table_info_1, df_result = get_info_table_structure(obj_jdbc, schema_name, table_name)
    if len(df_result) == 0:
        if raise_error_if_changes:
            raise MPMapasErrorThereAreNoRecordsInTable(connection=ds_name, schema=schema_name, table=table_name)

    else:
        for recno, row in df_result.iterrows():
            json_table_info_1['fields'][recno] = {}
            for c in df_result.columns:
                if 'table_name' not in c:
                    str_tmp = "%s" % row[c]
                    if 'nan' in str_tmp.lower():
                        json_table_info_1['fields'][recno][c] = None
                    else:
                        json_table_info_1['fields'][recno][c] = row[c]
    # let's save structure for future verification
    file_name = "%s%s_%s-%s.txt" % (configs.folders.ENTRADA_DIR, ds_name, schema_name, table_name)
    logger.debug("file_name='%s'" % file_name)
    if os.path.isfile(file_name):
        with open(file_name, 'r') as fr:
            json_table_info_0 = eval(fr.read())
        sorted_0 = []
        sorted_1 = []
        for f in json_table_info_0['fields']:
            # let's disregard added fields for ETL process
            if json_table_info_0['fields'][f]['column_name'] not in ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ',
                                                                     'DT_EXTRACAO']:
                # it will convert them to strings for classification
                sorted_0.append("%s" % json_table_info_0['fields'][f])
        for f in json_table_info_1['fields']:
            # let's disregard added fields for ETL process
            if json_table_info_1['fields'][f]['column_name'] not in ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ',
                                                                     'DT_EXTRACAO']:
                # it will convert them to strings for classification
                sorted_1.append("%s" % json_table_info_1['fields'][f])
        sorted_0 = sorted([repr(x) for x in sorted_0])
        sorted_1 = sorted([repr(x) for x in sorted_1])
        # If it changed then we will stop extraction
        if sorted_0 != sorted_1:
            logger.debug("sorted_0 = %s" % sorted_0)
            logger.debug("sorted_1 = %s" % sorted_1)
            if raise_error_if_changes:
                raise MPMapasErrorGettingTableStructure(connection=ds_name, schema=schema_name, table=table_name)
    else:
        with open(file_name, 'a') as fw:  # TODO: when will we update this file? When to delete?
            fw.write("%s\n" % json_table_info_1)  # na primeira vez vamos salvar o que foi lido do banco
        # returning df_result was useful for debugging but is no longer used
        # return json_table_info_1, df_result
    return json_table_info_1


def checking_changes_tables_structure_1(jt_1, jt_2, raise_error_if_changes=True):
    logger.warning('checking_changes_tables_structure_1')
    # Check if all fields in table 1 are in table 2 and have the same type
    list_to_check_fields = ['data_type', 'numeric_precision', 'numeric_scale']
    # TODO: Should we check these other columns?
    # ['column_name', 'data_type', 'ordinal_position', 'column_default', 
    # 'is_nullable', 'numeric_precision', 'numeric_scale']
    list_columns_to_check_1 = []
    list_recno_1 = []
    for f in jt_1['fields']:
        # let's disregard added fields for ETL process
        if jt_1['fields'][f]['column_name'] not in ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO']:
            # it will convert them to strings for classification
            list_columns_to_check_1.append(jt_1['fields'][f]['column_name'])
            list_recno_1.append(f)

    list_columns_to_check_2 = []
    list_recno_2 = []
    for f in jt_2['fields']:
        # let's disregard added fields for ETL process
        if jt_2['fields'][f]['column_name'] not in ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO']:
            # it will convert them to strings for classification
            list_columns_to_check_2.append(jt_2['fields'][f]['column_name'])
            list_recno_2.append(f)

    can_i_proceed = True
    for recno in jt_1['fields']:
        if jt_1['fields'][recno]['column_name'] in list_columns_to_check_2:
            recno2 = list_columns_to_check_2.index(jt_1['fields'][recno]['column_name'])
            if jt_1['fields'][recno]['column_name'] == jt_2['fields'][recno2]['column_name']:
                for properties_to_check in list_to_check_fields:  # checking properties
                    if jt_1['fields'][recno][properties_to_check] == jt_2['fields'][recno2][properties_to_check]:
                        can_i_proceed = True
                    else:
                        can_i_proceed = False
                        # TODO: rever essa mensagem ou adicinar uma MPMapasErrorDefinitionForAttributeIsDifferent
                        logger.error(
                            'Definition for attribute "%s" in column "%s" is not same at source and destination: "%s" != "%s"' % \
                            (properties_to_check, jt_1['fields'][recno]['column_name'],
                             jt_1['fields'][recno][properties_to_check], jt_2['fields'][recno2][properties_to_check]))
                        raise MPMapasErrorCheckingChangesTableStructure(connection=jt_2['ds_name'],
                                                                        schema=jt_2['schema_name'],
                                                                        table=jt_2['table_name'])
        else:
            can_i_proceed = False
            # TODO: rever essa mensagem ou adicinar uma MPMapasErrorColumnDoesNotExistInTable
            logger.warning('Column "%s" does not exist in table "%s" schema "%s" connection "%s"' % \
                           (jt_1['fields'][recno]['column_name'], jt_2['table_name'], jt_2['schema_name'],
                            jt_2['ds_name']))
            raise MPMapasErrorCheckingChangesTableStructure(connection=jt_2['ds_name'], schema=jt_2['schema_name'],
                                                            table=jt_2['table_name'])
    return can_i_proceed


def count_table(configs, jndi_name, schema_name, table_name):
    database = commons.get_database(configs.settings.JDBC_PROPERTIES[jndi_name], api=None)
    count_sql = "SELECT count(*) as count FROM \"" + schema_name + "\".\"" + table_name + "\";"
    df_fields = pd.DataFrame({'table_schema': [schema_name], 'table_name': [table_name],
                              'row_count': [database.execute_select(count_sql, result_mode='all')[0][0]],
                              'dt_count': [datetime.now(timezone.utc)]})

    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    last_count_sql = "SELECT rc.row_count from comprasrj.table_row_count rc where rc.table_schema = \'" + schema_name + "\' and rc.table_name = \'" + table_name + "\' order by rc.dt_count desc limit 1"
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
    last_check_sql = "SELECT tc.checksumid from comprasrj.table_check tc where tc.table_schema = \'" + schema_name + "\' and tc.table_name = \'" + table_name + "\' order by tc.dt_check desc limit 1"
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


def carga_gate():
    logger.info('carga_gate...')
    """
        Import do GATE:
        importar as tabelas stage.ComprasRJ_Contrato e stage.ComprasRJ_ItemContrato do GATE para atualizar 
        as novas tabelas comprasrj.item_contrato e comprasrj.contrato do opengeo com novos registros;
    :param configs: configuracoes com a conexao de banco
    :return: retorna os ids inseridos nas bases comprasrj.contrato e comprasrj.item_contrato
    """
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    result_df_item_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GATE_DS_NAME].jndi_name, schema_name='stage', table_name='ComprasRJ_ItemContrato')['table']
    result_df_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GATE_DS_NAME].jndi_name, schema_name='stage', table_name='ComprasRJ_Contrato')['table']
    server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name)

    if isinstance(result_df_item_contrato, pd.DataFrame) and not result_df_item_contrato.empty:
        # clone gate
        trunc_comprasrj_item_contrato_sql = "TRUNCATE TABLE comprasrj.\"ComprasRJ_ItemContrato\" CONTINUE IDENTITY RESTRICT"
        db_opengeo.execute_select(trunc_comprasrj_item_contrato_sql, result_mode=None)
        list_flds_clonegate_item_contrato = result_df_item_contrato.columns.values
        insert_sql_clonegate_item_contrato, insert_template_clonegate_item_contrato = db_opengeo.insert_values_sql(
            schema_name='comprasrj',
            table_name='ComprasRJ_ItemContrato',
            list_flds=list_flds_clonegate_item_contrato)
        db_opengeo.execute_values_insert(sql=insert_sql_clonegate_item_contrato,
                                         template=insert_template_clonegate_item_contrato,
                                         df_values_to_execute=result_df_item_contrato,
                                         fetch=False, server_encoding=server_encoding)
        # carga gate
        values_for_fillna_item_contrato = {'QTD': 0, 'QTD_ADITIV_SUPR': 0, 'VL_UNIT': 0.0, 'VL_UNIT_ADITIV_SUPR': 0.0}
        result_df_item_contrato = result_df_item_contrato.fillna(value=values_for_fillna_item_contrato).rename(
            str.upper, axis='columns')
        result_df_item_contrato['CHECKSUMID'] = commons.generate_checksum(result_df_item_contrato)
        result_df_item_contrato['DT_ULT_ATUALIZ'] = dt_now
        result_df_item_contrato['DT_EXTRACAO'] = dt_now
        result_df_item_contrato['DT_ULT_VER_GATE'] = dt_now
        list_flds_item_contrato = result_df_item_contrato.columns.values
        trunc_comprasrj_item_contrato_sql = "TRUNCATE TABLE comprasrj.item_contrato CONTINUE IDENTITY CASCADE"
        db_opengeo.execute_select(trunc_comprasrj_item_contrato_sql, result_mode=None)
        insert_sql_item_contrato, insert_template_item_contrato = db_opengeo.insert_values_sql(schema_name='comprasrj',
                                                                                               table_name='item_contrato',
                                                                                               list_flds=list_flds_item_contrato,
                                                                                               unique_field='ID',
                                                                                               pk_field='ID')
        db_opengeo.execute_values_insert(sql=insert_sql_item_contrato,
                                         template=insert_template_item_contrato,
                                         df_values_to_execute=result_df_item_contrato,
                                         fetch=True, server_encoding=server_encoding)

    if isinstance(result_df_contrato, pd.DataFrame) and not result_df_contrato.empty:
        # clone gate
        trunc_comprasrj_contrato_sql = "TRUNCATE TABLE comprasrj.\"ComprasRJ_Contrato\" CONTINUE IDENTITY RESTRICT"
        db_opengeo.execute_select(trunc_comprasrj_contrato_sql, result_mode=None)
        list_flds_clonegate_contrato = result_df_contrato.columns.values
        insert_sql_clonegate_contrato, insert_template_clonegate_contrato = db_opengeo.insert_values_sql(
            schema_name='comprasrj',
            table_name='ComprasRJ_Contrato',
            list_flds=list_flds_clonegate_contrato)
        db_opengeo.execute_values_insert(sql=insert_sql_clonegate_contrato,
                                         template=insert_template_clonegate_contrato,
                                         df_values_to_execute=result_df_contrato,
                                         fetch=False, server_encoding=server_encoding)
        # carga gate
        values_for_fillna_contrato = {'VL_ESTIMADO': 0.0, 'VL_EMPENHADO': 0.0, 'VL_EXECUTADO': 0.0, 'VL_PAGO': 0.0}
        result_df_contrato = result_df_contrato.fillna(value=values_for_fillna_contrato).rename(str.upper,
                                                                                                axis='columns')
        result_df_contrato['CHECKSUMID'] = commons.generate_checksum(result_df_contrato)
        result_df_contrato['DT_ULT_ATUALIZ'] = dt_now
        result_df_contrato['DT_EXTRACAO'] = dt_now
        result_df_contrato['DT_ULT_VER_GATE'] = dt_now
        list_flds_contrato = result_df_contrato.columns.values
        trunc_comprasrj_contrato_sql = "TRUNCATE TABLE comprasrj.contrato CONTINUE IDENTITY RESTRICT"
        db_opengeo.execute_select(trunc_comprasrj_contrato_sql, result_mode=None)
        insert_sql_contrato, insert_template_contrato = db_opengeo.insert_values_sql(schema_name='comprasrj',
                                                                                     table_name='contrato',
                                                                                     list_flds=list_flds_contrato,
                                                                                     unique_field='ID',
                                                                                     pk_field='ID')
        db_opengeo.execute_values_insert(sql=insert_sql_contrato,
                                         template=insert_template_contrato,
                                         df_values_to_execute=result_df_contrato,
                                         fetch=True, server_encoding=server_encoding)
    logger.info('fim carga_gate...')


def gerar_compras_itens_por_contrato(df_contrato, df_item_contrato):
    logger.info('gerar_compras_itens_por_contrato...')
    """
        Tabela de Saída 1: comprasrj.compras_itens_por_contrato:
        Objetivo: Coletar o valores dos itens por contrato;
        OBS: manter essa tabela 'aberta' por item_contrato;
    :param configs: configuracoes com as conexoes de banco
    :param df_contrato: dataframe da base comprasrj.contratos
    :param df_item_contrato: dataframe da base comprasrj.item_contrato
    :return: retorna os ids inseridos na base comprasrj.compras_itens_por_contrato
    """
    if isinstance(df_contrato, pd.DataFrame) and not df_contrato.empty and isinstance(df_item_contrato,
                                                                                      pd.DataFrame) and not df_item_contrato.empty:
        df_contrato = df_contrato.drop(['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO', 'DT_ULT_VER_GATE'],
                                       axis='columns')
        df_item_contrato = df_item_contrato.drop(
            ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO', 'DT_ULT_VER_GATE'], axis='columns')
        # Left join de comprasrj.item_contrato com comprasrj.contrato na coluna CONTRATACAO;
        df_compras_itens_por_contrato = pd.merge(df_contrato, df_item_contrato, how="left", on='CONTRATACAO',
                                                 suffixes=(None, '_Y'))
        # Criar coluna "Itens por Contrato" que é um Count pelo ID_ITEM;
        # o objetivo é saber quantos itens diferentes foram adquiridos;
        df_itens_por_contrato = df_compras_itens_por_contrato.groupby(['CONTRATACAO', 'ID_ITEM']).size().reset_index(
            name='ITENS_POR_CONTRATO')
        df_compras_itens_por_contrato = df_compras_itens_por_contrato.merge(df_itens_por_contrato,
                                                                            on=['CONTRATACAO', 'ID_ITEM'])
        # Criar campo "Master Key" que é "CONTRATACAO" + "-" + "ID_ITEM";
        df_compras_itens_por_contrato['CONTRATO_IDITEM'] = df_compras_itens_por_contrato['CONTRATACAO'].astype(
            str) + '-' + df_compras_itens_por_contrato['ID_ITEM'].astype(str)

        df_compras_itens_por_contrato['CHECKSUMID'] = commons.generate_checksum(df_compras_itens_por_contrato)
        df_compras_itens_por_contrato['DT_ULT_ATUALIZ'] = dt_now
        df_compras_itens_por_contrato['DT_ULT_VER_GATE'] = dt_now
        # TODO: !! rever essa data extracao. a ideia é ser a data do GATE ou a data de importacao do GATE
        df_compras_itens_por_contrato['DT_EXTRACAO'] = dt_now

        if isinstance(df_compras_itens_por_contrato, pd.DataFrame) and not df_compras_itens_por_contrato.empty:
            db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME],
                                              api=None)
            server_encoding = dbcommons.show_server_encoding(configs=configs,
                                                             jndi_name=configs.settings.JDBC_PROPERTIES[
                                                                 configs.settings.DB_OPENGEO_DS_NAME].jndi_name)
            # insert compras_itens_por_contrato
            list_flds_compras_itens_por_contrato = df_compras_itens_por_contrato.columns.values
            trunc_comprasrj_compras_itens_por_contrato_sql = "TRUNCATE TABLE comprasrj.compras_itens_por_contrato CONTINUE IDENTITY RESTRICT"
            db_opengeo.execute_select(trunc_comprasrj_compras_itens_por_contrato_sql, result_mode=None)
            insert_sql_compras_itens_por_contrato, insert_template_compras_itens_por_contrato = db_opengeo.insert_values_sql(
                schema_name='comprasrj', table_name='compras_itens_por_contrato',
                list_flds=list_flds_compras_itens_por_contrato, unique_field='ID', pk_field='ID')
            db_opengeo.execute_values_insert(
                sql=insert_sql_compras_itens_por_contrato, template=insert_template_compras_itens_por_contrato,
                df_values_to_execute=df_compras_itens_por_contrato, fetch=True, server_encoding=server_encoding)
    logger.info('fim gerar_compras_itens_por_contrato...')


def gerar_contratos_agregados(df_contrato):
    logger.info('gerar_contratos_agregados')
    """
        Tabela de Saída 2: comprasrj.contrato --> comprasrj.contratos_agregados;
        Objetivo: Ter uma série história de totais de valores dos contratos e 
                  ter o percentual de pagamento atual dos contratos;
    :param configs: configuracoes com as conexoes de banco
    :param df_contrato: dataframe da base comprasrj.contratos
    :return: retorna os ids inseridos na base comprasrj.contratos_agregados
    """
    if isinstance(df_contrato, pd.DataFrame) and not df_contrato.empty:
        """
        Agregar:
            CONTRATACAO: CONTAGEM;
            ORGAO: CONTAGEM;
            FORNECEDOR: CONTAGEM;
            VL_ESTIMADO: SOMA;
            VL_EMPENHADO: SOMA;
            VL_EXECUTADO: SOMA;
            VL_PAGO: SOMA;
        """
        df_contrato = df_contrato.drop(['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO', 'DT_ULT_VER_GATE'],
                                       axis='columns')
        group_valor_total_de_contratos = df_contrato.groupby(['CONTRATACAO'])
        count_orgao = df_contrato.groupby(['CONTRATACAO', 'ORGAO']).size().to_frame(name='COUNT_ORGAO')
        count_fornecedor = df_contrato.groupby(['CONTRATACAO', 'FORNECEDOR']).size().to_frame(name='COUNT_FORNECEDOR')
        df_valor_total_de_contratos = group_valor_total_de_contratos.size().to_frame(name='COUNT_CONTRATACAO')
        df_valor_total_de_contratos = (df_valor_total_de_contratos.join(count_orgao).join(count_fornecedor).join(
            group_valor_total_de_contratos.agg({'VL_ESTIMADO': 'sum'}).rename(
                columns={'VL_ESTIMADO': 'VL_ESTIMADO_SOMA'})).join(
            group_valor_total_de_contratos.agg({'VL_EMPENHADO': 'sum'}).rename(
                columns={'VL_EMPENHADO': 'VL_EMPENHADO_SOMA'})).join(
            group_valor_total_de_contratos.agg({'VL_EXECUTADO': 'sum'}).rename(
                columns={'VL_EXECUTADO': 'VL_EXECUTADO_SOMA'})).join(
            group_valor_total_de_contratos.agg({'VL_PAGO': 'sum'}).rename(columns={'VL_PAGO': 'VL_PAGO_SOMA'})))
        df_valor_total_de_contratos = df_valor_total_de_contratos.reset_index()

        """
            Criar colunas "PERC":
                PERC_VL_PAGO_VL_EXECUTADO: VL_PAGO/VL_EXECUTADO;
                PERC_VL_EXECUTADO_VL_EMPENHADO: VL_EXECUTADO/VL_EMPENHADO;
                PERC_VL_EMPENHADO_VL_ESTIMADO: VL_EMPENHADO/VL_ESTIMADO;
                PERC_VL_PAGO_VL_ESTIMADO: VL_PAGO/VL_ESTIMADO;
        """
        df_vl_pago_vl_executado = calc_percent_group_by_per_sum_of_values(df=df_contrato,
                                                                          group_by_column_name='CONTRATACAO',
                                                                          column_name_to_sum_1='VL_PAGO',
                                                                          column_name_to_sum_2='VL_EXECUTADO',
                                                                          perc_column_name='PERC_VL_PAGO_VL_EXECUTADO')
        df_vl_executado_vl_empenhado = calc_percent_group_by_per_sum_of_values(df=df_contrato,
                                                                               group_by_column_name='CONTRATACAO',
                                                                               column_name_to_sum_1='VL_EXECUTADO',
                                                                               column_name_to_sum_2='VL_EMPENHADO',
                                                                               perc_column_name='PERC_VL_EXECUTADO_VL_EMPENHADO')
        df_vl_empenhado_vl_estimado = calc_percent_group_by_per_sum_of_values(df=df_contrato,
                                                                              group_by_column_name='CONTRATACAO',
                                                                              column_name_to_sum_1='VL_EMPENHADO',
                                                                              column_name_to_sum_2='VL_ESTIMADO',
                                                                              perc_column_name='PERC_VL_EMPENHADO_VL_ESTIMADO')
        df_vl_pago_vl_estimado = calc_percent_group_by_per_sum_of_values(df=df_contrato,
                                                                         group_by_column_name='CONTRATACAO',
                                                                         column_name_to_sum_1='VL_PAGO',
                                                                         column_name_to_sum_2='VL_ESTIMADO',
                                                                         perc_column_name='PERC_VL_PAGO_VL_ESTIMADO')

        df_contratos_agregados = (df_valor_total_de_contratos
                                  .merge(df_vl_pago_vl_executado, on='CONTRATACAO', how='left', suffixes=(None, '_Y'))
                                  .merge(df_vl_executado_vl_empenhado, on='CONTRATACAO', how='left',
                                         suffixes=(None, '_Y'))
                                  .merge(df_vl_empenhado_vl_estimado, on='CONTRATACAO', how='left',
                                         suffixes=(None, '_Y'))
                                  .merge(df_vl_pago_vl_estimado, on='CONTRATACAO', how='left', suffixes=(None, '_Y'))
                                  )

        df_contratos_agregados = df_contratos_agregados.reindex()
        df_contratos_agregados = df_contratos_agregados.drop(
            ['VL_EXECUTADO_Y', 'VL_EMPENHADO_Y', 'VL_PAGO_Y', 'VL_ESTIMADO_Y'], axis='columns')
        df_contratos_agregados = replace_inf_nan(df_contratos_agregados)
        df_contratos_agregados['CHECKSUMID'] = commons.generate_checksum(df_contratos_agregados)
        df_contratos_agregados['ATUALIZACAO'] = datetime.now(timezone.utc)
        df_contratos_agregados['DT_ULT_ATUALIZ'] = dt_now
        df_contratos_agregados['DT_ULT_VER_GATE'] = dt_now
        # TODO: !! rever essa data extracao. a ideia é ser a data do GATE ou a data de importacao do GATE
        df_contratos_agregados['DT_EXTRACAO'] = dt_now

        if isinstance(df_contratos_agregados, pd.DataFrame) and not df_contratos_agregados.empty:
            db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME],
                                              api=None)
            server_encoding = dbcommons.show_server_encoding(configs=configs,
                                                             jndi_name=configs.settings.JDBC_PROPERTIES[
                                                                 configs.settings.DB_OPENGEO_DS_NAME].jndi_name)
            # insert contratos_agregados
            list_flds_contratos_agregados = df_contratos_agregados.columns.values
            trunc_comprasrj_contratos_agregados_sql = "TRUNCATE TABLE comprasrj.contratos_agregados CONTINUE IDENTITY RESTRICT"
            db_opengeo.execute_select(trunc_comprasrj_contratos_agregados_sql, result_mode=None)
            insert_sql_contratos_agregados, insert_template_contratos_agregados = db_opengeo.insert_values_sql(
                schema_name='comprasrj', table_name='contratos_agregados', list_flds=list_flds_contratos_agregados,
                unique_field='ID', pk_field='ID')
            db_opengeo.execute_values_insert(sql=insert_sql_contratos_agregados,
                                             template=insert_template_contratos_agregados,
                                             df_values_to_execute=df_contratos_agregados,
                                             fetch=True,
                                             server_encoding=server_encoding)
    logger.info('fim gerar_contratos_agregados...')


def calc_percent_group_by_per_sum_of_values(df, group_by_column_name, column_name_to_sum_1, column_name_to_sum_2,
                                            perc_column_name):
    group_df = df.groupby([group_by_column_name])
    df_sum_1 = group_df.agg({column_name_to_sum_1: 'sum'})
    df_sum_2 = group_df.agg({column_name_to_sum_2: 'sum'})
    df_sum1_per_sum2 = df_sum_1.merge(df_sum_2, on=group_by_column_name)
    df_sum1_per_sum2[column_name_to_sum_1] = df_sum1_per_sum2[column_name_to_sum_1].astype(np.float)
    df_sum1_per_sum2[column_name_to_sum_2] = df_sum1_per_sum2[column_name_to_sum_2].astype(np.float)
    df_sum1_per_sum2[perc_column_name] = replace_inf_nan(
        df_sum1_per_sum2[column_name_to_sum_1].mul(100).div(df_sum1_per_sum2[column_name_to_sum_2]))
    df_percent = df_sum1_per_sum2.reset_index()
    return df_percent


def replace_inf_nan(df):
    df = df.replace(np.nan, 0).replace(np.NAN, 0).replace(np.NaN, 0).replace(math.nan, 0)
    df = df.replace(np.NZERO, 0)
    df = df.replace(np.inf, -1).replace(np.infty, -1).replace(np.Inf, -1)
    df = df.replace(np.Infinity, -1).replace(np.NINF, -1).replace(np.PINF, -1).replace(math.inf, -1)
    return df


def update_dt_ult_ver_gate(configs, schema_name, table_name):
    logger.info('Updating %s.%s.DT_ULT_VER_GATE .' % (schema_name, table_name))
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    update_dt_ult_ver_gate_sql = "UPDATE \"" + schema_name + "\".\"" + table_name + "\" SET \"DT_ULT_VER_GATE\"=NOW()"
    db_opengeo.execute_select(update_dt_ult_ver_gate_sql, result_mode=None)


def update_tables_dt_ult_ver_gate(configs):
    logger.info('Update tables DT_ULT_VER_GATE.')
    update_dt_ult_ver_gate(configs=configs, schema_name='comprasrj', table_name='contrato')
    update_dt_ult_ver_gate(configs=configs, schema_name='comprasrj', table_name='item_contrato')
    update_dt_ult_ver_gate(configs=configs, schema_name='comprasrj', table_name='contratos_agregados')
    update_dt_ult_ver_gate(configs=configs, schema_name='comprasrj', table_name='compras_itens_por_contrato')


def main(run_painel_compras=True, diff_check_table=False, diff_count_table=False):
    try:
        logger.info('Starting %s.' % configs.settings.ETL_JOB)
        jdbc_gate = configs.settings.JDBC_PROPERTIES[configs.settings.DB_GATE_DS_NAME]
        jdbc_opengeo = configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME]

        schema_gate = 'stage'
        table_contrato_gate = 'ComprasRJ_Contrato'
        table_item_gate = 'ComprasRJ_ItemContrato'
        schema_opengeo = 'comprasrj'
        table_contrato_opengeo = 'contrato'
        table_item_opengeo = 'item_contrato'

        if not run_painel_compras and diff_check_table:
            diff_check_contrato = check_table_checksum(configs=configs, jndi_name=jdbc_gate.jndi_name,
                                                       schema_name=schema_gate, table_name=table_contrato_gate)
            diff_check_item = check_table_checksum(configs=configs, jndi_name=jdbc_gate.jndi_name,
                                                   schema_name=schema_gate, table_name=table_item_gate)
            if diff_check_contrato or diff_check_item:
                logger.info('Different data found.')
                run_painel_compras = True
            else:
                logger.info('No different data found.')
                run_painel_compras = False
        elif not run_painel_compras and diff_count_table:
            diff_count_contrato = check_table_row_count(configs=configs, jndi_name=jdbc_gate.jndi_name,
                                                        schema_name=schema_gate, table_name=table_contrato_gate)
            diff_count_item = check_table_row_count(configs=configs, jndi_name=jdbc_gate.jndi_name,
                                                    schema_name=schema_gate, table_name=table_item_gate)
            if diff_count_contrato[0] or diff_count_item[0]:
                logger.info('Different rows count found.')
                run_painel_compras = True
            else:
                logger.info('No different rows count found.')
                run_painel_compras = False

        if run_painel_compras:
            # json_table_info_1 = checking_changes_tables_structure_0(jdbc_gate, jdbc_gate.jndi_name,
            #                                                         schema_gate, table_contrato_gate)
            # json_table_info_2 = checking_changes_tables_structure_0(jdbc_gate, jdbc_gate.jndi_name,
            #                                                         schema_gate, table_item_gate)
            #
            # json_table_info_3 = checking_changes_tables_structure_0(jdbc_opengeo, jdbc_opengeo.jndi_name,
            #                                                         schema_opengeo, table_contrato_opengeo)
            # json_table_info_4 = checking_changes_tables_structure_0(jdbc_opengeo, jdbc_opengeo.jndi_name,
            #                                                         schema_opengeo, table_item_opengeo)
            #
            # checking_changes_tables_structure_1(json_table_info_1, json_table_info_3)
            # checking_changes_tables_structure_1(json_table_info_2, json_table_info_4)

            carga_gate()

            df_contrato = dbcommons.load_table(configs=configs, jndi_name=jdbc_opengeo.jndi_name,
                                               schema_name=schema_opengeo, table_name=table_contrato_opengeo)['table']
            df_item_contrato = dbcommons.load_table(configs=configs, jndi_name=jdbc_opengeo.jndi_name,
                                                    schema_name=schema_opengeo, table_name=table_item_opengeo)['table']
            gerar_compras_itens_por_contrato(df_contrato, df_item_contrato)
            gerar_contratos_agregados(df_contrato)

        update_tables_dt_ult_ver_gate(configs)
        logger.info('Finishing %s.' % configs.settings.ETL_JOB)
    except MPMapasErrorAccessingTable as c_err:
        logger.exception(c_err)
        exit(c_err.error_code)
    except MPMapasErrorCheckingChangesTableStructure as c_err:
        logger.exception(c_err)
        exit(c_err.error_code)
    except MPMapasErrorGettingTableStructure as c_err:
        logger.exception(c_err)
        exit(c_err.error_code)
    except MPMapasErrorThereAreNoRecordsInTable as c_err:
        logger.exception(c_err)
        exit(c_err.error_code)
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

        run_painel_compras = True
        diff_check_table = False
        diff_count_table = False

        try:
            opts, args = getopt.getopt(sys.argv[1:], "hrtc")
        except getopt.GetoptError:
            print('usage: ' + sys.argv[0] + ' [-h] --> for help')
            sys.exit(2)
        if len(opts) > 1:
            print('pass only one argument.')
            print('usage: ' + sys.argv[0] + ' [-h] --> for help')
            sys.exit()
        for opt, arg in opts:
            if opt == '-h':
                print('usage: ' + sys.argv[0] + ' [-r] --> to run painel_compras')
                print('usage: ' + sys.argv[0] + ' [-t] --> to check diffent table data')
                print('usage: ' + sys.argv[0] + ' [-c] --> to check diffent row count')
                sys.exit()
            elif opt in ["-r"]:
                run_painel_compras = True
            elif opt in ["-t"]:
                run_painel_compras = False
                diff_check_table = True
            elif opt in ["-c"]:
                run_painel_compras = False
                diff_count_table = True

        main(run_painel_compras=run_painel_compras, diff_check_table=diff_check_table,
             diff_count_table=diff_count_table)
    except Exception as excpt:
        logging.exception('Fatal error in __main__')
        exit(excpt)
