import logging
import math
import os
from datetime import datetime, timezone

import mpmapas_commons as commons
import mpmapas_logger
import numpy as np
import pandas as pd
from db_utils import mpmapas_db_commons as dbcommons
from mpmapas_exceptions import MPMapasException

os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)


def gerar_compras_itens_por_contrato(df_contrato, df_item_contrato):
    logger.info('gerar_compras_itens_por_contrato...')
    """
        Tabela de Saída 1: comprasrj.compras_itens_por_contrato:
        Objetivo: Coletar o valores dos itens por contrato;
        OBS: manter essa tabela 'aberta' por item_contrato;
    :param df_contrato: dataframe da base comprasrj.contratos
    :param df_item_contrato: dataframe da base comprasrj.item_contrato
    """
    if isinstance(df_contrato, pd.DataFrame) and not df_contrato.empty and isinstance(df_item_contrato,
                                                                                      pd.DataFrame) and not df_item_contrato.empty:
        df_contrato = df_contrato.drop(
            ['ID', 'un_objeto', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO', 'DT_ULT_VER_GATE'],
            axis='columns')
        df_item_contrato = df_item_contrato.drop(
            ['ID', 'un_item', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_ULT_VER_GATE'], axis='columns')
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
        df_compras_itens_por_contrato['un_objeto'] = commons.unaccent_df(df=df_compras_itens_por_contrato, col='OBJETO')
        df_compras_itens_por_contrato['un_item'] = commons.unaccent_df(df=df_compras_itens_por_contrato, col='ITEM')
        df_compras_itens_por_contrato['DT_ULT_ATUALIZ'] = dt_now
        df_compras_itens_por_contrato['DT_ULT_VER_GATE'] = dt_now
        # TODO: !! rever essa data extracao. a ideia é ser a data do GATE ou a data de importacao do GATE
        if 'DT_EXTRACAO' not in df_compras_itens_por_contrato.columns.values:
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
    :param df_contrato: dataframe da base comprasrj.contratos
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
        df_contrato = df_contrato.drop(
            ['ID', 'un_objeto', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_ULT_VER_GATE'],
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
        if 'DT_EXTRACAO' not in df_contratos_agregados.columns.values:
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
    df_sum1_per_sum2[column_name_to_sum_1] = df_sum1_per_sum2[column_name_to_sum_1].astype(float)
    df_sum1_per_sum2[column_name_to_sum_2] = df_sum1_per_sum2[column_name_to_sum_2].astype(float)
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


def main():
    try:
        logger.info('Starting painel_compras para o %s.' % configs.settings.ETL_JOB)
        jdbc_opengeo = configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME]
        schema_opengeo = 'comprasrj'
        table_contrato_opengeo = 'contrato'
        table_item_opengeo = 'item_contrato'
        df_contrato = dbcommons.load_table(configs=configs, jndi_name=jdbc_opengeo.jndi_name,
                                           schema_name=schema_opengeo, table_name=table_contrato_opengeo)['table']
        df_item_contrato = dbcommons.load_table(configs=configs, jndi_name=jdbc_opengeo.jndi_name,
                                                schema_name=schema_opengeo, table_name=table_item_opengeo)['table']
        gerar_compras_itens_por_contrato(df_contrato, df_item_contrato)
        gerar_contratos_agregados(df_contrato)
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)
    finally:
        logger.info('Finishing painel_compras para o %s.' % configs.settings.ETL_JOB)


global configs, logger

if __name__ == '__main__' or __name__ == 'painel_compras':
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
