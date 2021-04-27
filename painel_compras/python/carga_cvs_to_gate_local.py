import logging
import os

import mpmapas_commons as commons
import numpy as np
import pandas as pd
from db_utils import mpmapas_db_commons as dbcommons

os.environ["NLS_LANG"] = ".UTF8"


def carga_gate_csv_to_stage(configs):
    logging.warning('carga_gate_CSV_to_stage...')
    """
        Import csv pro GATE:
        importar os csvs para as tabelas stage.ComprasRJ_Contrato e stage.ComprasRJ_ItemContrato do GATE (banco local/dsv)
    :param configs: configuracoes com a conexao de banco
    :return: retorna os ids inseridos nas base do gate
    """
    dict_item_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GATE_DS_NAME].jndi_name, schema_name='stage', table_name='ComprasRJ_ItemContrato',
                                              csv_source=configs.settings.GATE_CSV_ITEM_CONTRATO)
    dict_contrato = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GATE_DS_NAME].jndi_name, schema_name='stage', table_name='ComprasRJ_Contrato',
                                         csv_source=configs.settings.GATE_CSV_CONTRATO)
    result_df_contrato = dict_contrato['table']
    result_df_item_contrato = dict_item_contrato['table']
    db_gate = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_GATE_DS_NAME], api=None)
    server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_OPENGEO_DS_NAME].jndi_name)
    if isinstance(result_df_item_contrato, pd.DataFrame) and not result_df_item_contrato.empty:
        values_for_fillna_item_contrato = {'QTD': 0, 'QTD_ADITIV_SUPR': 0, 'VL_UNIT': 0.0, 'VL_UNIT_ADITIV_SUPR': 0.0}
        result_df_item_contrato = result_df_item_contrato.fillna(value=values_for_fillna_item_contrato).rename(
            str.upper, axis='columns')
        list_flds_item_contrato = result_df_item_contrato.columns.values
        insert_sql_item_contrato, insert_template_item_contrato = db_gate.insert_values_sql(schema_name='stage',
                                                                                            table_name='ComprasRJ_ItemContrato',
                                                                                            list_flds=list_flds_item_contrato)
        db_gate.execute_values_insert(sql=insert_sql_item_contrato,
                                      template=insert_template_item_contrato,
                                      df_values_to_execute=result_df_item_contrato,
                                      fetch=False, server_encoding=server_encoding)
    if isinstance(result_df_contrato, pd.DataFrame) and not result_df_contrato.empty:
        values_for_fillna_contrato = {'VL_ESTIMADO': 0.0, 'VL_EMPENHADO': 0.0, 'VL_EXECUTADO': 0.0, 'VL_PAGO': 0.0}
        result_df_contrato = result_df_contrato.fillna(value=values_for_fillna_contrato).rename(str.upper,
                                                                                                axis='columns')
        result_df_contrato['DT_INICIO'] = result_df_contrato['DT_INICIO'].replace(np.nan, None)
        result_df_contrato['DT_FIM'] = result_df_contrato['DT_FIM'].replace(np.nan, None)
        list_flds_contrato = result_df_contrato.columns.values
        insert_sql_contrato, insert_template_contrato = db_gate.insert_values_sql(schema_name='stage',
                                                                                  table_name='ComprasRJ_Contrato',
                                                                                  list_flds=list_flds_contrato)
        db_gate.execute_values_insert(sql=insert_sql_contrato,
                                      template=insert_template_contrato,
                                      df_values_to_execute=result_df_contrato, fetch=False,
                                      server_encoding=server_encoding)
        logging.warning('fim carga_gate...')


# Memento mori
def main():
    configs = commons.read_config('../etc/settings.yml')
    carga_gate_csv_to_stage(configs)


if __name__ == '__main__':
    main()
