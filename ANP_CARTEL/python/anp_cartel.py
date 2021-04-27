# -*- coding: utf-8 -*-
import os
import csv
import pandas as pd
# import numpy as np
# # -*- add python_utils to pythonpath commons -*- #
# import sys
# sys.path.append('E:/pentaho/etl/UTIL/python_utils/')
# import commons
# # -*- -------------------------------------- -*- #
import mpmapas_commons as commons


def ascii_normalizer(text):
    import re
    import unicodedata
    text = str(text).lower()
    text = text.strip()
    text = re.sub("\s\-\s", "_", text)
    text = re.sub("\s", "_", text)
    text = ''.join(ch for ch in unicodedata.normalize('NFKD', text) if not unicodedata.combining(ch))
    return text


def download_file(urlstr, file_nam, mode='wb'):
    import urllib.request
    import shutil
    try:
        print('Starting download ... ' + file_nam + ' !')
        with urllib.request.urlopen(urlstr) as response, open(file=file_nam, mode=mode) as out_file:
            shutil.copyfileobj(response, out_file)
            #meta = response.info()
            #print(meta)
    except urllib.error.HTTPError:
        print('HTTP Error 404: URL Not Found: ' + urlstr)
        raise Exception('HTTP Error 404')
    #except:
        #print("An exception occurred")


def gravar_saida(df, file_pat, sep=';', decimal=',', na_rep='', quoting=csv.QUOTE_ALL, quotechar='"', encoding='utf-8'):
    print('Gravando arquivo: ' + file_pat + ' ....')
    df.to_csv(path_or_buf=file_pat, sep=sep, decimal=decimal, na_rep=na_rep, quoting=quoting, quotechar=quotechar, encoding=encoding)


def read_file(csvs_columns, csvs, file_nam, file_extension, header=0, delimiter='\t'):
    print('Lendo aquivo: ' + file_nam + file_extension + ' ....')
    if file_extension == '.xlsx':
        csvs[file_nam] = pd.read_excel(os.path.abspath(configs.folders.ENTRADA_DIR + file_nam + file_extension), na_values='-', keep_default_na=True, sheet_name=0, header=header).rename(ascii_normalizer, axis='columns')
        csvs_columns[file_nam] = pd.Series(csvs[file_nam].columns.values)
    elif file_extension == '.csv':
        file_pat = os.path.abspath(configs.folders.ENTRADA_DIR + file_nam + file_extension)
        file_encoding = commons.detect_encoding(file_pat)
        delimiter = commons.detect_delimiter(file_pat, file_encoding)
        csvs[file_nam] = pd.read_csv(filepath_or_buffer=file_pat, header=0, delimiter=delimiter, encoding=file_encoding, na_values='-', keep_default_na=True, dayfirst=True, decimal=',').rename(ascii_normalizer, axis='columns')
        csvs_columns[file_nam] = pd.Series(csvs[file_nam].columns.values[0].split(';'))
    return csvs, csvs_columns


if __name__ == '__main__':
    configs = commons.read_config('./configs/settings.yml')
    print('Start --> '+configs.settings.ETL_JOB)
    print('banco: ' + configs.settings.JDBC_PROPERTIES['mpmapas_geo'].jndi_name + ' - ' + configs.settings.JDBC_PROPERTIES['mpmapas_geo'].user)
    print('Python DIR --> ' + configs.folders.PYTHON_SCRIPT_DIR)
    print('Entrada DIR --> ' + configs.folders.ENTRADA_DIR)

    database = commons.get_database(configs.settings.JDBC_PROPERTIES['mpmapas_geo'])
    print(database.bdtype)
    print(database.simple_jdbc.hostname)
    #result = database.execute_sql('select * from anp.historico_preco_postos')
    list_flds = ['regiao_sigla', 'estado_sigla', 'municipio', 'revenda', 'cnpj_da_revenda', 'produto', 'data_da_coleta', 'valor_de_venda', 'valor_de_compra', 'unidade_de_medida', 'bandeira']
    select_sql = database.select_sql('anp', 'historico_preco_postos', list_flds, '')
    result = database.execute_sql(select_sql, result_mode='all')
    result_df = pd.DataFrame(result, columns=list_flds)
    print('-----------')
    print(result_df)

    # old_links
    # file_urls = {'semanal_municipios_2021.xlsx': 'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal-municipios-2021.xlsx',
    #                 'semanal_municipios_desde_ago2020_liquidos.xlsx': 'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/pdc/semanal-municipios-desde-ago2020-liquidos.xlsx',
    #                 'precos_semanais_ultimas_4_semanas_diesel_gnv.csv': 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/da/shpc/precos-semanais-ultimas-4-semanas-diesel-gnv.csv',
    #                 'precos_semanais_ultimas_4_semanas_gasolina_etanol.csv': 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/da/shpc/precos-semanais-ultimas-4-semanas-gasolina-etanol.csv',
    #                 'precos_semanais_ultimas_4_semanas_glp.csv': 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/da/shpc/precos-semanais-ultimas-4-semanas-glp.csv'
    #              }

    file_urls = {'semanal_municipios_2021.xlsx': 'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/semanal-municipios-2021.xlsx',
                    'semanal_municipios_desde_ago2020_liquidos.xlsx': 'https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/pdc/semanal-municipios-desde-ago2020-liquidos.xlsx',
                    'precos_semanais_ultimas_4_semanas_diesel_gnv.csv': 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/da/shpc/qus/ultimas-4-semanas-diesel-gnv.csv',
                    'precos_semanais_ultimas_4_semanas_gasolina_etanol.csv': 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/da/shpc/qus/ultimas-4-semanas-gasolina-etanol.csv',
                    'precos_semanais_ultimas_4_semanas_glp.csv': 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/da/shpc/qus/ultimas-4-semanas-glp.csv'
                 }

    # Download csvs
    # for file_name, url in file_urls.items():
    #     print('Downloading '+file_name+' ....')
    #     download_file(url, configs.folders.ENTRADA_DIR + file_name)

    # Read csvs
    dict_csvs_columns = {}
    dict_csvs = {}
    read_file(dict_csvs_columns, dict_csvs, 'semanal_municipios_2021', '.xlsx', 11)
    read_file(dict_csvs_columns, dict_csvs, 'semanal_municipios_desde_ago2020_liquidos', '.xlsx', 8)
    read_file(dict_csvs_columns, dict_csvs, 'precos_semanais_ultimas_4_semanas_diesel_gnv', '.csv')
    read_file(dict_csvs_columns, dict_csvs, 'precos_semanais_ultimas_4_semanas_gasolina_etanol', '.csv')
    read_file(dict_csvs_columns, dict_csvs, 'precos_semanais_ultimas_4_semanas_glp', '.csv')

    # Gravar csvs
    for name, df_csv in dict_csvs.items():
        file_path = os.path.abspath(configs.folders.SAIDA_DIR + name + '.csv')
        gravar_saida(df_csv, file_path)

    # Gravar csv df_columns
    df_columns = pd.DataFrame.from_dict(dict_csvs_columns, orient='index')
    df_columns = df_columns.transpose()
    gravar_saida(pd.DataFrame.from_dict(df_columns), os.path.abspath(configs.folders.SAIDA_DIR + 'df_csvs_columns.csv'))

    print('End --> ' + configs.settings.ETL_JOB)
