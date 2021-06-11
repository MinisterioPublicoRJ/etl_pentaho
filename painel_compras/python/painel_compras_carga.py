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


def safe_column_string_to_datetime(dataframe, column_name):
    return [None if not col_val or col_val == 'NaN' else col_val for col_val in dataframe[column_name]]


def safe_column_string_to_int(dataframe, column_name):
    return [None if not col_val or col_val == 'NaN' else int(round(float(col_val.replace(',', '.')))) for col_val in
            dataframe[column_name]]


def safe_column_string_to_float(dataframe, column_name):
    dataframe[column_name] = [None if not col_val or col_val == 'NaN' else col_val.replace(',', '.') for col_val in
                              dataframe[column_name]]
    return [None if not col_val or col_val == 'NaN' else col_val.replace('.', '', col_val.count('.') - 1) for col_val in
            dataframe[column_name]]


def carga_painel_comprasrj():
    logger.info('carga_painel_comprasrj...')
    """
        Import para o schema comrpasrj:
        importar as tabelas compra, contrato e item_contrato para o schema comprasrj 
        para atualizar com novos registros;
    """
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    fonte_dados = configs.settings.FONTE_DADOS

    if 'SIGA' in fonte_dados:
        # origem - SIGA
        result_df_item_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
            configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj_stage',
                                                       table_name='itens_contratos')[
            'table']
        result_df_item_contrato = result_df_item_contrato.rename(columns={
            'contratacao': 'CONTRATACAO',
            'id_item': 'ID_ITEM',
            'item': 'ITEM',
            'qtde_original': 'QTD',
            'vl_unitoriginal': 'VL_UNIT',
            'total_aditivadasuprimida': 'QTD_ADITIV_SUPR',
            'vl_unitaditivadosuprimido': 'VL_UNIT_ADITIV_SUPR',
            'dt_extracao': 'DT_EXTRACAO'
        }).drop(['id', 'dt_ult_atualiz'], axis='columns')
        result_df_item_contrato['VL_UNIT'] = safe_column_string_to_float(dataframe=result_df_item_contrato,
                                                                         column_name='VL_UNIT')
        result_df_item_contrato['QTD'] = safe_column_string_to_int(dataframe=result_df_item_contrato, column_name='QTD')
        result_df_item_contrato['VL_UNIT_ADITIV_SUPR'] = safe_column_string_to_float(dataframe=result_df_item_contrato,
                                                                                     column_name='VL_UNIT_ADITIV_SUPR')
        result_df_item_contrato['QTD_ADITIV_SUPR'] = safe_column_string_to_int(dataframe=result_df_item_contrato,
                                                                               column_name='QTD_ADITIV_SUPR')
        result_df_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
            configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj_stage', table_name='contratos')[
            'table']
        result_df_contrato = result_df_contrato.rename(columns={
            'contratacao': 'CONTRATACAO',
            'status_contratacao': 'STATUS',
            'data_contratacao': 'DT_CONTRATACAO',
            'unidade': 'ORGAO',
            'processo': 'PROCESSO',
            'objeto': 'OBJETO',
            'tipo_de_aquisicao': 'TIPO_AQUISICAO',
            'criterio_de_julgamento': 'CRITERIO_JULGAMENTO',
            'data_inicio_vigencia': 'DT_INICIO',
            'data_fim_vigencia': 'DT_FIM',
            'fornecedor': 'FORNECEDOR',
            'cpfcnpj': 'CPF_CNPJ',
            'valor_total_contratovalor_estimado_para_contratacao_r': 'VL_ESTIMADO',
            'valor_total_empenhado_r': 'VL_EMPENHADO',
            'valor_total_liquidado_r': 'VL_EXECUTADO',
            'valor_total_pago_r': 'VL_PAGO',
            'dt_extracao': 'DT_EXTRACAO'
        }).drop(['id', 'dt_ult_atualiz'], axis='columns')
        result_df_contrato['DT_INICIO'] = safe_column_string_to_datetime(dataframe=result_df_contrato,
                                                                         column_name='DT_INICIO')
        result_df_contrato['DT_FIM'] = safe_column_string_to_datetime(dataframe=result_df_contrato,
                                                                      column_name='DT_FIM')
        result_df_contrato['VL_ESTIMADO'] = safe_column_string_to_float(dataframe=result_df_contrato,
                                                                        column_name='VL_ESTIMADO')
        result_df_contrato['VL_EMPENHADO'] = safe_column_string_to_float(dataframe=result_df_contrato,
                                                                         column_name='VL_EMPENHADO')
        result_df_contrato['VL_EXECUTADO'] = safe_column_string_to_float(dataframe=result_df_contrato,
                                                                         column_name='VL_EXECUTADO')
        result_df_contrato['VL_PAGO'] = safe_column_string_to_float(dataframe=result_df_contrato, column_name='VL_PAGO')

        result_df_compra = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
            configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj_stage',
                                                table_name='compras_diretas')[
            'table']
        result_df_compra = result_df_compra.rename(columns={
            'unidade': 'ORGAO',
            'id_processo': 'ID_PROCESSO',
            'processo': 'PROCESSO',
            'objeto': 'OBJETO',
            'afastamento': 'AFASTAMENTO',
            'enquadramento_legal': 'ENQUADR_LEGAL',
            'data_de_aprovacao': 'DT_APROVACAO',
            'valor_do_processo_r': 'VL_PROCESSO',
            'fornecedor_vencedor': 'FORNECEDOR',
            'id_item': 'ID_ITEM',
            'item': 'ITEM',
            'qtd': 'QTD',
            'unidade_de_medida': 'UNID',
            'vl_unitario_r': 'VL_UNITARIO',
            'dt_extracao': 'DT_EXTRACAO'
        }).drop(['id', 'dt_ult_atualiz'], axis='columns')
        result_df_compra['QTD'] = safe_column_string_to_int(dataframe=result_df_compra, column_name='QTD')
        result_df_compra['VL_PROCESSO'] = safe_column_string_to_float(dataframe=result_df_compra,
                                                                      column_name='VL_PROCESSO')
        result_df_compra['VL_UNITARIO'] = safe_column_string_to_float(dataframe=result_df_compra,
                                                                      column_name='VL_UNITARIO')

        result_df_catalogo = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
            configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='comprasrj_stage', table_name='catalogo')[
            'table']
        server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
            configs.settings.DB_OPENGEO_DS_NAME].jndi_name)
    elif 'GATE' in fonte_dados:
        # origem - GATE
        result_df_item_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
            configs.settings.DB_GATE_DS_NAME].jndi_name, schema_name='stage', table_name='ComprasRJ_ItemContrato')[
            'table']
        result_df_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
            configs.settings.DB_GATE_DS_NAME].jndi_name, schema_name='stage', table_name='ComprasRJ_Contrato')['table']
        result_df_compra = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
            configs.settings.DB_GATE_DS_NAME].jndi_name, schema_name='stage', table_name='ComprasRJ_Compra')['table']
        server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
            configs.settings.DB_OPENGEO_DS_NAME].jndi_name)

    if 'SIGA' in fonte_dados and (isinstance(result_df_catalogo, pd.DataFrame) and not result_df_catalogo.empty):
        # carga catalogo
        values_for_fillna_catalogo = {'id_tipo': 0, 'id_familia': 0, 'id_classe': 0, 'id_artigo': 0, 'id_item': 0, }
        result_df_catalogo = result_df_catalogo.fillna(value=values_for_fillna_catalogo).rename(
            str.lower, axis='columns')
        result_df_catalogo['checksumid'] = commons.generate_checksum(result_df_catalogo)
        result_df_catalogo['un_familia'] = commons.unaccent_df(df=result_df_catalogo, col='familia')
        result_df_catalogo['un_classe'] = commons.unaccent_df(df=result_df_catalogo, col='classe')
        result_df_catalogo['un_artigo'] = commons.unaccent_df(df=result_df_catalogo, col='artigo')
        result_df_catalogo['un_descricao_item'] = commons.unaccent_df(df=result_df_catalogo, col='descricao_item')
        result_df_catalogo['DT_ULT_VER_GATE'] = dt_now
        if 'dt_extracao' not in result_df_catalogo.columns.values:
            result_df_catalogo['dt_extracao'] = dt_now
        list_flds_catalogo = result_df_catalogo.columns.values
        trunc_comprasrj_catalogo_sql = "TRUNCATE TABLE comprasrj.catalogo CONTINUE IDENTITY CASCADE"
        db_opengeo.execute_select(trunc_comprasrj_catalogo_sql, result_mode=None)
        insert_sql_catalogo, insert_template_catalogo = db_opengeo.insert_values_sql(schema_name='comprasrj',
                                                                                     table_name='catalogo',
                                                                                     list_flds=list_flds_catalogo,
                                                                                     unique_field='id',
                                                                                     pk_field='id')
        db_opengeo.execute_values_insert(sql=insert_sql_catalogo,
                                         template=insert_template_catalogo,
                                         df_values_to_execute=result_df_catalogo,
                                         fetch=True, server_encoding=server_encoding)

    if isinstance(result_df_item_contrato, pd.DataFrame) and not result_df_item_contrato.empty:
        # carga item_contrato
        values_for_fillna_item_contrato = {'QTD': 0, 'QTD_ADITIV_SUPR': 0, 'VL_UNIT': 0.0, 'VL_UNIT_ADITIV_SUPR': 0.0}
        result_df_item_contrato = result_df_item_contrato.fillna(value=values_for_fillna_item_contrato)
        if 'GATE' in fonte_dados:
            result_df_item_contrato = result_df_item_contrato.rename(str.upper, axis='columns')
        result_df_item_contrato['CHECKSUMID'] = commons.generate_checksum(result_df_item_contrato)
        result_df_item_contrato['un_item'] = commons.unaccent_df(df=result_df_item_contrato, col='ITEM')
        result_df_item_contrato['DT_ULT_ATUALIZ'] = dt_now
        result_df_item_contrato['DT_ULT_VER_GATE'] = dt_now
        if 'DT_EXTRACAO' not in result_df_item_contrato.columns.values:
            result_df_item_contrato['DT_EXTRACAO'] = dt_now
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
        logger.info('Starting REFRESH MATERIALIZED VIEW comprasrj.itens_a_classificar.')
        refresh_mview_sql = "REFRESH MATERIALIZED VIEW comprasrj.itens_a_classificar;"
        db_opengeo.execute_select(refresh_mview_sql, result_mode=None)
        logger.info('Finishing REFRESH MATERIALIZED VIEW comprasrj.itens_a_classificar.')

    if isinstance(result_df_contrato, pd.DataFrame) and not result_df_contrato.empty:
        # carga contrato
        values_for_fillna_contrato = {'VL_ESTIMADO': 0.0, 'VL_EMPENHADO': 0.0, 'VL_EXECUTADO': 0.0, 'VL_PAGO': 0.0}
        result_df_contrato = result_df_contrato.fillna(value=values_for_fillna_contrato)
        if 'GATE' in fonte_dados:
            result_df_contrato = result_df_contrato.rename(str.upper, axis='columns')
        result_df_contrato['CHECKSUMID'] = commons.generate_checksum(result_df_contrato)
        result_df_contrato['un_objeto'] = commons.unaccent_df(df=result_df_contrato, col='OBJETO')
        result_df_contrato['DT_ULT_ATUALIZ'] = dt_now
        result_df_contrato['DT_ULT_VER_GATE'] = dt_now
        if 'DT_EXTRACAO' not in result_df_contrato.columns.values:
            result_df_contrato['DT_EXTRACAO'] = dt_now
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

    if isinstance(result_df_compra, pd.DataFrame) and not result_df_compra.empty:
        # carga compra
        values_for_fillna_compra = {'VL_UNITARIO': 0.0, 'VL_PROCESSO': 0.0, 'QTD': 0}
        result_df_compra = result_df_compra.fillna(value=values_for_fillna_compra)
        if 'GATE' in fonte_dados:
            result_df_compra = result_df_compra.rename(str.upper, axis='columns')
        result_df_compra['CHECKSUMID'] = commons.generate_checksum(result_df_compra)
        result_df_compra['un_objeto'] = commons.unaccent_df(df=result_df_compra, col='OBJETO')
        result_df_compra['un_item'] = commons.unaccent_df(df=result_df_compra, col='ITEM')
        result_df_compra['un_unid'] = commons.unaccent_df(df=result_df_compra, col='UNID')
        result_df_compra['DT_ULT_ATUALIZ'] = dt_now
        result_df_compra['DT_ULT_VER_GATE'] = dt_now
        if 'DT_EXTRACAO' not in result_df_compra.columns.values:
            result_df_compra['DT_EXTRACAO'] = dt_now
        list_flds_compra = result_df_compra.columns.values
        trunc_comprasrj_compra_sql = "TRUNCATE TABLE comprasrj.compra CONTINUE IDENTITY RESTRICT"
        db_opengeo.execute_select(trunc_comprasrj_compra_sql, result_mode=None)
        insert_sql_compra, insert_template_compra = db_opengeo.insert_values_sql(schema_name='comprasrj',
                                                                                 table_name='compra',
                                                                                 list_flds=list_flds_compra,
                                                                                 unique_field='ID',
                                                                                 pk_field='ID')
        db_opengeo.execute_values_insert(sql=insert_sql_compra,
                                         template=insert_template_compra,
                                         df_values_to_execute=result_df_compra,
                                         fetch=True, server_encoding=server_encoding)
    logger.info('fim carga_painel_comprasrj...')


def main():
    try:
        logger.info('Starting painel_compras_carga para o %s.' % configs.settings.ETL_JOB)
        carga_painel_comprasrj()
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)
    finally:
        logger.info('Finishing painel_compras_carga para o %s.' % configs.settings.ETL_JOB)


global configs, logger

if __name__ == '__main__' or __name__ == 'painel_compras_carga':
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
