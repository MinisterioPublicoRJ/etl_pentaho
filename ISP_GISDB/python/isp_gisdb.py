# -*- coding: utf-8 -*-
import logging
import os

import mpmapas_commons as commons
import mpmapas_logger
import numpy as np
import pandas as pd
from mpmapas_exceptions import MPMapasException


def read_df_isp():
    logger.info('reading isp_base_dp_evolucao_mensal_cisp_linha.csv.')
    df_isp = pd.read_csv(os.path.abspath(configs.folders.ENTRADA_DIR + 'isp_base_dp_evolucao_mensal_cisp_linha.csv'),
                         delimiter=';', encoding='utf-8',
                         usecols=['objectid', 'cisp', 'typefield', 'delito', 'typefield_tirmestre_ano'])
    df_isp.delito = pd.to_numeric(df_isp.delito, errors='coerce').fillna(0).astype(np.int64)
    df_isp = df_isp.astype(
        {'objectid': np.int64, 'cisp': np.int64, 'typefield': str, 'delito': np.int64, 'typefield_tirmestre_ano': str})
    return df_isp


def read_df_agregacao():
    logger.info('reading agregacao.csv.')
    df_agregacao = pd.read_csv(os.path.abspath(configs.folders.ENTRADA_DIR + 'agregacao.csv'), delimiter=';',
                               encoding='utf-8', usecols=['delito_simples', 'delito_agregado'])
    return df_agregacao


def transf_grupo_delito(df_agregacao):
    logger.info('transf grupo_delito.')
    dict_grupo_delito = {}
    grupos_delito = df_agregacao.groupby(['delito_agregado'])
    for idx_gd, df_gd in grupos_delito:
        dict_grupo_delito[idx_gd] = df_gd
    return dict_grupo_delito


def transf_pivot(df):
    logger.info('transf pivot table.')
    df_pivot = df.pivot_table(index='cisp', columns='typefield_tirmestre_ano', values='delito', aggfunc=np.sum,
                              fill_value=0)
    df_pivot = df_pivot.astype(np.int64)
    # print(df_pivot.head)
    return df_pivot


def gravar_saida(idx_tf, df):
    logger.info('writing csv %s with %s rows and %s columns.' % (idx_tf, df.shape[0], df.shape[1]))
    df.to_csv(os.path.abspath(configs.folders.SAIDA_DIR + idx_tf + '.csv'), sep=';', encoding='utf-8')


def transf_grupos_isp(df_isp):
    logger.info('transf grupos_isp.')
    dict_grupos_isp = {}
    grupos_typefield = df_isp.groupby(['typefield'])
    for idx_tf, df_tf in grupos_typefield:
        dict_grupos_isp[idx_tf] = df_tf.reset_index()
    return dict_grupos_isp


def transf_grupos_agreg(dict_grupos_isp, dict_grupo_delito):
    logger.info('transf grupos_agreg.')
    grupos_pivot = {}
    grupos_agreg = {}
    dict_agreg_isp = {}
    for delito_agregado in dict_grupo_delito:
        if delito_agregado in grupos_agreg:
            grupos_agreg[delito_agregado] = pd.concat(grupos_agreg[delito_agregado], dict_grupo_delito[delito_agregado])
        else:
            grupos_agreg[delito_agregado] = dict_grupo_delito[delito_agregado]
    for delito_agregado in grupos_agreg:
        for idx_del_agreg, df_del_agreg in grupos_agreg[delito_agregado].iterrows():
            if df_del_agreg.delito_agregado not in dict_agreg_isp:
                df_int = []
            else:
                df_int = dict_agreg_isp[df_del_agreg.delito_agregado]
            df_int.append(dict_grupos_isp[df_del_agreg.delito_simples])
            dict_agreg_isp[df_del_agreg.delito_agregado] = df_int
    for del_agreg in dict_agreg_isp:
        dict_agreg_isp[del_agreg] = pd.concat(dict_agreg_isp[del_agreg])
        grupos_pivot[del_agreg] = transf_pivot(dict_agreg_isp[del_agreg])
        gravar_saida(del_agreg, grupos_pivot[del_agreg])
    return grupos_pivot


def main():
    try:
        logger.info('Starting %s.' % configs.settings.ETL_JOB)
        # jdbc_opengeo = configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME]
        # jdbc_gisdb_seguranca = configs.settings.JDBC_PROPERTIES[configs.settings.DB_GISDB_SEGURANCA_DS_NAME]

        df_isp = read_df_isp()

        df_agregacao = read_df_agregacao()

        dict_grupo_delito = transf_grupo_delito(df_agregacao)

        dict_grupos_isp = transf_grupos_isp(df_isp)
        grupos_agreg = transf_grupos_agreg(dict_grupos_isp, dict_grupo_delito)

        logger.info('Finishing %s.' % configs.settings.ETL_JOB)
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
