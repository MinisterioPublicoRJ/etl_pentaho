import config

import sqlalchemy
import urllib.request
import pandas as pd
import numpy as np
import psycopg2 as pg2

from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

pd.options.mode.chained_assignment = None


def download_files(url, name):

    filename = "./download_folder/" + name + ".xlsx"

    try:
        print("Baixando arquivo: " + url)
        urllib.request.urlretrieve(url, filename)
        print('\n')
        print("Arquivo salvo: " + filename)

    except Exception as e:
        print("Erro no download:")
        print(e)


def check_database_lastdate(table_name, engine):

    try:
        query = "select MAX(data_final) from anp.{}".format(table_name)

        response = engine.execute(query)

        for r in response:
            result = dict(r)

        return result['max']

    except Exception as e:
        print(e)
        return None,


def check_file_lastdate(table_name):

    if table_name == "temp_semanal_municipio":

        try:
            df = pd.read_excel(
                "./download_folder/temp_semanal_municipio.xlsx", skiprows=11)
            cols = ['DATA INICIAL', 'DATA FINAL', 'REGIÃO', 'ESTADO', 'MUNICÍPIO',
                    'PRODUTO', 'NÚMERO DE POSTOS PESQUISADOS', 'UNIDADE DE MEDIDA',
                    'PREÇO MÉDIO REVENDA', 'DESVIO PADRÃO REVENDA', 'PREÇO MÍNIMO REVENDA',
                    'PREÇO MÁXIMO REVENDA', 'MARGEM MÉDIA REVENDA', 'COEF DE VARIAÇÃO REVENDA']
            df_redux = df[cols]
            df_redux = df_redux[df_redux['ESTADO'] == "RIO DE JANEIRO"]
            df_redux = df_redux[df_redux['PRODUTO'].isin(
                ["GASOLINA COMUM", "ETANOL HIDRATADO"])]
            df_redux.columns = ['data_inicial', 'data_final', 'regiao', 'estado', 'municipio',
                                'produto', 'numero_de_postos_pesquisados', 'unidade_de_medida',
                                'preco_medio_revenda', 'desvio_padrao_revenda', 'preco_minimo_revenda',
                                'preco_maximo_revenda', 'margem_media_revenda', 'coef_de_variacao_revenda']

            df_redux['margem_media_revenda'] = df_redux['margem_media_revenda'].str.replace(
                "[^0-9]", "", regex=True)

            df_redux = df_redux.replace(r'^\s*$', np.nan, regex=True)

            date_file = df_redux['data_final'].max()

            return date_file, df_redux

        except Exception as e:
            print(e)
            return None


def current_file_newer_than_database(table_name, engine):

    last_date_database = check_database_lastdate(table_name, engine)

    last_date_file, df_redux = check_file_lastdate(table_name)

    if last_date_file is not None:

        if last_date_file.to_pydatetime().date() > last_date_database:

            df_insert = df_redux[df_redux['data_final']
                                 >= pd.to_datetime(last_date_database)]

            try:
                df_insert.to_sql(table_name, con=engine,
                                 schema='anp', if_exists='append', index=False)

            except Exception as e:
                print(e)

            print("""Arquivo baixado contém dados mais novos que os encontrados no banco de dados.
                        Foi realizado o update: {}""".format(table_name))

        else:
            print("Escrever mensagem bonitinha")


if __name__ == '__main__':

    try:
        engine = create_engine(
            config.params['url_sqlalchemy'], echo=True, poolclass=NullPool)

    except Exception as e:
        print(e)

    params = config.params['links'][0]

    for table_name, link in params.items():

        download_files(link, table_name)

        if current_file_newer_than_database(table_name, engine):

            # do insert
            print("if do insert")
        else:
            # do nothing
            print("else do nothing")
