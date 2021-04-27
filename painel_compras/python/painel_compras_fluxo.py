import os
os.environ["NLS_LANG"] = ".UTF8"
from datetime import datetime, timezone
import time
import traceback
import sys
import pandas as pd
import numpy as np
import math
import logger2x

import mpmapas_commons as commons
from db_utils import mpmapas_db_commons as dbcommons


configs = commons.read_config('../etc/settings.yml')
carga_gate(configs)


dt_now = datetime.now(timezone.utc)


my_log = logger2x.logger2x()


CD_ERRO_FALTA_DADO  = '01'
CD_ERRO_BANCO       = '02'
CD_ERRO_MAPEAMENTO  = '03'
CD_ERRO_PAREOU      = '04'



TABELA_ERROS {
    CD_ERRO_FALTA_DADO: {'msg': 'uma mensagem', 'status': 400},
    CD_ERRO_BANCO: {'msg': 'outra mensagem', 'status': 401},
    CD_ERRO_MAPEAMENTO: {'msg': 'mais uma mensagem', 'status': 402},
    CD_ERRO_PAREOU: {'msg': 'aquela mensagem', 'status': 403},
}


def carga_gate(configs):
    print('carga_gate...')
    """
        Import do GATE:
        importar as tabelas stage.ComprasRJ_Contrato e stage.ComprasRJ_ItemContrato do GATE para atualizar 
        as novas tabelas comprasrj.item_contrato e comprasrj.contrato do opengeo com novos registros;
    :param configs: configuracoes com a conexao de banco
    :return: retorna os ids inseridos nas bases comprasrj.contrato e comprasrj.item_contrato
    """
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    result_df_item_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[configs.settings.DB_GATE_DS_NAME].jndi_name, schema_name='stage', table_name='ComprasRJ_ItemContrato')['table']
    result_df_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[configs.settings.DB_GATE_DS_NAME].jndi_name, schema_name='stage', table_name='ComprasRJ_Contrato')['table']
    server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME].jndi_name)
    
    if isinstance(result_df_item_contrato, pd.DataFrame) and not result_df_item_contrato.empty:
        values_for_fillna_item_contrato = {'QTD': 0, 'QTD_ADITIV_SUPR': 0, 'VL_UNIT': 0.0, 'VL_UNIT_ADITIV_SUPR': 0.0}
        result_df_item_contrato = result_df_item_contrato.fillna(value=values_for_fillna_item_contrato).rename(str.upper, axis='columns')
        result_df_item_contrato['CHECKSUMID'] = commons.generate_checksum(result_df_item_contrato)
        result_df_item_contrato['DT_ULT_ATUALIZ'] = dt_now
        result_df_item_contrato['DT_EXTRACAO'] = dt_now
        list_flds_item_contrato = result_df_item_contrato.columns.values
        insert_sql_item_contrato, insert_template_item_contrato = db_opengeo.insert_values_sql(schema_name='comprasrj', table_name='item_contrato', list_flds=list_flds_item_contrato, unique_field='CHECKSUMID', pk_field='ID')
        result_insert_item_contrato_ids = db_opengeo.execute_values_insert(sql=insert_sql_item_contrato, template=insert_template_item_contrato, df_values_to_execute=result_df_item_contrato, fetch=True, server_encoding=server_encoding)

    if isinstance(result_df_contrato, pd.DataFrame) and not result_df_contrato.empty:
        values_for_fillna_contrato = {'VL_ESTIMADO': 0.0, 'VL_EMPENHADO': 0.0, 'VL_EXECUTADO': 0.0, 'VL_PAGO': 0.0}
        result_df_contrato = result_df_contrato.fillna(value=values_for_fillna_contrato).rename(str.upper, axis='columns')
        result_df_contrato['CHECKSUMID'] = commons.generate_checksum(result_df_contrato)
        result_df_contrato['DT_ULT_ATUALIZ'] = dt_now
        result_df_contrato['DT_EXTRACAO'] = dt_now
        list_flds_contrato = result_df_contrato.columns.values
        insert_sql_contrato, insert_template_contrato = db_opengeo.insert_values_sql(schema_name='comprasrj', table_name='contrato', list_flds=list_flds_contrato, unique_field='CHECKSUMID', pk_field='ID')
        result_insert_contrato_ids = db_opengeo.execute_values_insert(sql=insert_sql_contrato, template=insert_template_contrato, df_values_to_execute=result_df_contrato, fetch=True, server_encoding=server_encoding)
        print('fim carga_gate...')
    return result_insert_item_contrato_ids, result_insert_contrato_ids


def gerar_compras_itens_por_contrato(configs, df_contrato, df_item_contrato):
    print('gerar_compras_itens_por_contrato...')
    """
        Tabela de Saída 1: comprasrj.compras_itens_por_contrato:
        Objetivo: Coletar o valores dos itens por contrato;
        OBS: manter essa tabela 'aberta' por item_contrato;
    :param configs: configuracoes com as conexoes de banco
    :param df_contrato: dataframe da base comprasrj.contratos
    :param df_item_contrato: dataframe da base comprasrj.item_contrato
    :return: retorna os ids inseridos na base comprasrj.compras_itens_por_contrato
    """
    if isinstance(df_contrato, pd.DataFrame) and not df_contrato.empty and isinstance(df_item_contrato, pd.DataFrame) and not df_item_contrato.empty:
        # dt_extracao_contrato = df_contrato.drop(columns=df_contrato.drop(columns=['CONTRATACAO', 'DT_EXTRACAO']).columns.values).reset_index(drop=True)
        df_contrato = df_contrato.drop(['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO'], axis='columns')
        df_item_contrato = df_item_contrato.drop(['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO'], axis='columns')
        # Left join de comprasrj.item_contrato com comprasrj.contrato na coluna CONTRATACAO;
        df_compras_itens_por_contrato = pd.merge(df_contrato, df_item_contrato, how="left", on='CONTRATACAO', suffixes=(None, '_Y'))
        # Criar coluna "Itens por Contrato" que é um Count pelo ID_ITEM; o objetivo é saber quantos itens diferentes foram adquiridos;
        df_itens_por_contrato = df_compras_itens_por_contrato.groupby(['CONTRATACAO', 'ID_ITEM']).size().reset_index(name='ITENS_POR_CONTRATO')
        df_compras_itens_por_contrato = df_compras_itens_por_contrato.merge(df_itens_por_contrato, on=['CONTRATACAO', 'ID_ITEM'])
        # Criar campo "Master Key" que é "CONTRATACAO" + "-" + "ID_ITEM";
        df_compras_itens_por_contrato['CONTRATO_IDITEM'] = df_compras_itens_por_contrato['CONTRATACAO'].astype(str) + '-' + df_compras_itens_por_contrato['ID_ITEM'].astype(str)

        # TODO: !! Left join da nossa tabela em criação com stat.alerta_contrato_compras_rj na coluna "Master Key" = "contrato_iditem";
        # TODO: !! a tabela de alerta será definida pela ESTATÍSTICA, mas estou chamando de stat.alerta_contrato_painel_compras_rj;
        # df_alerta_contrato_compras_rj, dict_types_alerta_contrato_compras_rj = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME].jndi_name, schema_name='stat', table_name='alerta_contrato_compras_rj', return_dict_col_types=True)
        # if isinstance(df_alerta_contrato_compras_rj, pd.DataFrame) and not df_alerta_contrato_compras_rj.empty:
        #     # left join com stat.alerta_contrato_compras_rj mas tem q rodar a parte do etl de alerta de compras antes
        #     df_alerta_contrato_compras_rj = df_alerta_contrato_compras_rj[['contrato_iditem', 'unid', 'media_preco', 'preco_minimo', 'preco_maximo', 'amplitude_preco', 'quartil_um', 'quartil_dois', 'quartil_tres', 'outlier_inferior', 'outlier_superior', 'n', 'var_perc', 'alerta', 'mensagem', 'ult_atualizacao']].rename(str.upper, axis='columns')
        #     df_compras_itens_por_contrato = df_compras_itens_por_contrato.merge(df_alerta_contrato_compras_rj, how='left', on='CONTRATO_IDITEM')
        #     df_compras_itens_por_contrato = df_compras_itens_por_contrato.rename(columns={'ULT_ATUALIZACAO': 'ULT_ATUALIZACAO_ALERTA'})
        #

        #     # Na coluna "alerta" que estiver nulo, atribuir string "Preço Normal"; OBS: confirmar com isso pra V2
        #     df_compras_itens_por_contrato['ALERTA'] = df_compras_itens_por_contrato['ALERTA'].replace('', 'Preço Normal')

        df_compras_itens_por_contrato['CHECKSUMID'] = commons.generate_checksum(df_compras_itens_por_contrato)
        df_compras_itens_por_contrato['DT_ULT_ATUALIZ'] = dt_now
        # TODO: !! rever essa data extracao. a ideia é ser a data do GATE ou a data de importacao do GATE
        df_compras_itens_por_contrato['DT_EXTRACAO'] = dt_now

        if isinstance(df_compras_itens_por_contrato, pd.DataFrame) and not df_compras_itens_por_contrato.empty:
            db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
            server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME].jndi_name)
            # insert compras_itens_por_contrato
            list_flds_compras_itens_por_contrato = df_compras_itens_por_contrato.columns.values
            insert_sql_compras_itens_por_contrato, insert_template_compras_itens_por_contrato = db_opengeo.insert_values_sql(schema_name='comprasrj', table_name='compras_itens_por_contrato', list_flds=list_flds_compras_itens_por_contrato, unique_field='CHECKSUMID', pk_field='ID')
            result_insert_compras_itens_por_contrato_ids = db_opengeo.execute_values_insert(sql=insert_sql_compras_itens_por_contrato, template=insert_template_compras_itens_por_contrato, df_values_to_execute=df_compras_itens_por_contrato, fetch=True, server_encoding=server_encoding)
            print('fim gerar_compras_itens_por_contrato...')
    return result_insert_compras_itens_por_contrato_ids


def gerar_contratos_agregados(configs, df_contrato):
    print('gerar_contratos_agregados')
    """
        Tabela de Saída 2: comprasrj.contrato --> comprasrj.contratos_agregados;
        Objetivo: Ter uma série história de totais de valores dos contratos e ter o percentual de pagamento atual dos contratos;
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
        df_contrato = df_contrato.drop(['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO'], axis='columns')
        group_valor_total_de_contratos = df_contrato.groupby(['CONTRATACAO'])
        count_orgao = df_contrato.groupby(['CONTRATACAO', 'ORGAO']).size().to_frame(name='COUNT_ORGAO')
        count_fornecedor = df_contrato.groupby(['CONTRATACAO', 'FORNECEDOR']).size().to_frame(name='COUNT_FORNECEDOR')
        df_valor_total_de_contratos = group_valor_total_de_contratos.size().to_frame(name='COUNT_CONTRATACAO')
        df_valor_total_de_contratos = (df_valor_total_de_contratos
                                        .join(count_orgao)
                                        .join(count_fornecedor)
                                        .join(group_valor_total_de_contratos.agg({'VL_ESTIMADO': 'sum'}).rename(columns={'VL_ESTIMADO': 'VL_ESTIMADO_SOMA'}))
                                        .join(group_valor_total_de_contratos.agg({'VL_EMPENHADO': 'sum'}).rename(columns={'VL_EMPENHADO': 'VL_EMPENHADO_SOMA'}))
                                        .join(group_valor_total_de_contratos.agg({'VL_EXECUTADO': 'sum'}).rename(columns={'VL_EXECUTADO': 'VL_EXECUTADO_SOMA'}))
                                        .join(group_valor_total_de_contratos.agg({'VL_PAGO': 'sum'}).rename(columns={'VL_PAGO': 'VL_PAGO_SOMA'}))
                                       )
        df_valor_total_de_contratos = df_valor_total_de_contratos.reset_index()

        """
            Criar colunas "PERC":
                PERC_VL_PAGO_VL_EXECUTADO: VL_PAGO/VL_EXECUTADO;
                PERC_VL_EXECUTADO_VL_EMPENHADO: VL_EXECUTADO/VL_EMPENHADO;
                PERC_VL_EMPENHADO_VL_ESTIMADO: VL_EMPENHADO/VL_ESTIMADO;
                PERC_VL_PAGO_VL_ESTIMADO: VL_PAGO/VL_ESTIMADO;
        """
        df_vl_pago_vl_executado = calc_percent_group_by_per_sum_of_values(df=df_contrato, group_by_column_name='CONTRATACAO', column_name_to_sum_1='VL_PAGO', column_name_to_sum_2='VL_EXECUTADO', perc_column_name='PERC_VL_PAGO_VL_EXECUTADO')
        df_vl_executado_vl_empenhado = calc_percent_group_by_per_sum_of_values(df=df_contrato, group_by_column_name='CONTRATACAO', column_name_to_sum_1='VL_EXECUTADO', column_name_to_sum_2='VL_EMPENHADO', perc_column_name='PERC_VL_EXECUTADO_VL_EMPENHADO')
        df_vl_empenhado_vl_estimado = calc_percent_group_by_per_sum_of_values(df=df_contrato, group_by_column_name='CONTRATACAO', column_name_to_sum_1='VL_EMPENHADO', column_name_to_sum_2='VL_ESTIMADO', perc_column_name='PERC_VL_EMPENHADO_VL_ESTIMADO')
        df_vl_pago_vl_estimado = calc_percent_group_by_per_sum_of_values(df=df_contrato, group_by_column_name='CONTRATACAO', column_name_to_sum_1='VL_PAGO', column_name_to_sum_2='VL_ESTIMADO', perc_column_name='PERC_VL_PAGO_VL_ESTIMADO')

        df_contratos_agregados = (df_valor_total_de_contratos
                                        .merge(df_vl_pago_vl_executado, on='CONTRATACAO', how='left', suffixes=(None, '_Y'))
                                        .merge(df_vl_executado_vl_empenhado, on='CONTRATACAO', how='left', suffixes=(None, '_Y'))
                                        .merge(df_vl_empenhado_vl_estimado, on='CONTRATACAO', how='left', suffixes=(None, '_Y'))
                                        .merge(df_vl_pago_vl_estimado, on='CONTRATACAO', how='left', suffixes=(None, '_Y'))
                                     )

        df_contratos_agregados = df_contratos_agregados.reindex()
        df_contratos_agregados = df_contratos_agregados.drop(['VL_EXECUTADO_Y', 'VL_EMPENHADO_Y', 'VL_PAGO_Y', 'VL_ESTIMADO_Y'], axis='columns')
        df_contratos_agregados = replace_inf_nan(df_contratos_agregados)
        df_contratos_agregados['CHECKSUMID'] = commons.generate_checksum(df_contratos_agregados)
        df_contratos_agregados['ATUALIZACAO'] = datetime.now(timezone.utc)
        df_contratos_agregados['DT_ULT_ATUALIZ'] = dt_now
        # TODO: !! rever essa data extracao. a ideia é ser a data do GATE ou a data de importacao do GATE
        df_contratos_agregados['DT_EXTRACAO'] = dt_now

        if isinstance(df_contratos_agregados, pd.DataFrame) and not df_contratos_agregados.empty:
            db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
            server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME].jndi_name)
            # insert contratos_agregados
            list_flds_contratos_agregados = df_contratos_agregados.columns.values
            insert_sql_contratos_agregados, insert_template_contratos_agregados = db_opengeo.insert_values_sql(schema_name='comprasrj', table_name='contratos_agregados', list_flds=list_flds_contratos_agregados, unique_field='CHECKSUMID', pk_field='ID')
            result_insert_contratos_agregados_ids = db_opengeo.execute_values_insert(sql=insert_sql_contratos_agregados, template=insert_template_contratos_agregados, df_values_to_execute=df_contratos_agregados, fetch=True, server_encoding=server_encoding)
            print('fim gerar_contratos_agregados...')
    return result_insert_contratos_agregados_ids


def calc_percent_group_by_per_sum_of_values(df, group_by_column_name, column_name_to_sum_1, column_name_to_sum_2, perc_column_name):
    group_df = df.groupby([group_by_column_name])
    df_sum_1 = group_df.agg({column_name_to_sum_1: 'sum'})
    df_sum_2 = group_df.agg({column_name_to_sum_2: 'sum'})
    df_sum1_per_sum2 = df_sum_1.merge(df_sum_2, on=group_by_column_name)
    df_sum1_per_sum2[column_name_to_sum_1] = df_sum1_per_sum2[column_name_to_sum_1].astype(np.float)
    df_sum1_per_sum2[column_name_to_sum_2] = df_sum1_per_sum2[column_name_to_sum_2].astype(np.float)
    df_sum1_per_sum2[perc_column_name] = replace_inf_nan(df_sum1_per_sum2[column_name_to_sum_1].mul(100).div(df_sum1_per_sum2[column_name_to_sum_2]))
    df_percent = df_sum1_per_sum2.reset_index()
    return df_percent

def replace_inf_nan(df):
    df = df.replace(np.nan, 0).replace(np.NAN, 0).replace(np.NaN, 0).replace(math.nan, 0)
    df = df.replace(np.NZERO, 0)
    df = df.replace(np.inf, -1).replace(np.infty, -1).replace(np.Inf, -1).replace(np.Infinity, -1).replace(np.NINF, -1).replace(np.PINF, -1).replace(math.inf, -1)
    return df


def tarefa_um():
    list_source_tables = []
    list_target_tables = []

    list_source_tables.append({'table_schema': 'stage', 'table_name': 'ComprasRJ_Contrato'})
    list_target_tables.append({'table_schema': 'opengeo', 'table_name': 'comprasrj.contrato'})
    list_source_tables.append({'table_schema': 'stage', 'table_name': 'ComprasRJ_ItemContrato'})
    list_target_tables.append({'table_schema': 'opengeo', 'table_name': 'comprasrj.item_contratoo'})

    return list_source_tables, list_target_tables

def tarefa_dois(source, target):
    # TODO: !!! terminar
    df_contrato = dbcommons.load_table(
        configs=configs,
        jndi_name=configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME].jndi_name,
        schema_name='comprasrj',
        table_name='contrato')['table']
    df_item_contrato = dbcommons.load_table(
        configs=configs,
        jndi_name=configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME].jndi_name,
        schema_name='comprasrj',
        table_name='item_contrato')['table']

    if deuproblema:
        raise (MeuErro('01'))
    gerar_compras_itens_por_contrato(configs, df_contrato, df_item_contrato)
    gerar_contratos_agregados(configs, df_contrato)


def MeuErro(Exception):
    def __init__(self, code):
        self.code = code


def trataerro(exception_launched):
    print(TABELA_ERROS[exception_launched.code])


# Memento mori
def main(task_id = 0):
    # TODO: as primeiras coisas primeiro, rever uso dessa variável local dict_log_text
    dict_log_text = {'dt':time.strftime("%Y_%m_%d_%H_%M_%S").lower(), 'dl': 'error', 'ti': '0', 'msg': ''}

    my_log.folder_log = configs.folders.LOG_DIR
    print('Log DIR-->', configs.folders.LOG_DIR)
    print('Start --> ' + configs.settings.ETL_JOB)
    print('banco: ORIGEM ' + configs.settings.JDBC_PROPERTIES[configs.settings.DB_GATE_DS_NAME].jndi_name + ' - ' + configs.settings.JDBC_PROPERTIES[configs.settings.DB_GATE_DS_NAME].user)
    print('banco: ORIGEM ' + configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME].jndi_name + ' - ' + configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME].user)
    print('Python DIR --> ' + configs.folders.PYTHON_SCRIPT_DIR)
    print('Entrada DIR --> ' + configs.folders.ENTRADA_DIR)

    try:
        list_source_tables, list_target_tables = tarefa_um()

        for source, target in zip(list_source_tables, list_target_tables):
            tarefa_dois(source, target)
    
    except MeuErro as error:
        trataerro(error) 
    except:
        'tratamento de erros genéricos'
        pass
 
    my_log.setting('dl', 'debug')
    task_id = next_task_id


if __name__ == '__main__':
    main()
