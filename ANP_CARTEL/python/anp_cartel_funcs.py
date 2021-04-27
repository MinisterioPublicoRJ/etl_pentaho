# -*- coding: utf-8 -*-

# -*- add python_utils to pythonpath commons -*- #
import sys
sys.path.append('E:/pentaho/etl/UTIL/python_utils/')
import commons
# -*- -------------------------------------- -*- #
import os
import csv
import pandas as pd


class AnpFile:
    def __init__(self, config, link, max_number_retry_task=5, task_attempt_number=0, interval_between_attempts=2, request_response=None, line_content='',
                 lines_text=[], number_of_lines=0, list_1_url_links_to_check=[], number_1_files_to_check=0, file_to_check='', url_link_file_to_check='',
                 file_fold_tranferred='', file_fold_verify='', mtime_0=0, mtime_1=0, index_1=-1, recno_id=-1, dict_csv_columns={}, dict_csv_files={},
                 fold_transferred='files/transferred/', fold_verify='files/verify/', fold_verified='files/verified/'):
        self.link_url_1 = link #"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"
        self.max_number_retry_task = max_number_retry_task
        self.task_attempt_number = task_attempt_number
        self.interval_between_attempts = interval_between_attempts
        self.request_response = request_response
        self.line_content = line_content
        self.lines_text = lines_text
        self.number_of_lines = number_of_lines
        self.list_1_url_links_to_check = list_1_url_links_to_check  # lista de arquivos a serem transferidos
        self.number_1_files_to_check = number_1_files_to_check
        self.file_to_check = file_to_check  # nome do arquivo a ser verificado
        self.url_link_file_to_check = url_link_file_to_check  # link url do arquivo a ser verificado
        self.file_fold_tranferred = file_fold_tranferred
        self.file_fold_verify = file_fold_verify
        self.mtime_0 = mtime_0  # datetime do arquivo que foi transferido recentemente
        self.mtime_1 = mtime_1  # datetime do arquivo que esta em fold_transferred
        self.index_1 = index_1  # índice para a lista de arquivos a verificar
        self.recno_id = recno_id  # usado como index número da linha do arquivo CSV
        self.fold_transferred = config.folders.ETL_DIR + fold_transferred
        self.fold_verify = config.folders.ETL_DIR + fold_verify
        self.fold_verified = config.folders.ETL_DIR + fold_verified
        self.dict_csv_columns = dict_csv_columns
        self.dict_csv_files = dict_csv_files


def load_configs():
    return commons.read_config('./configs/settings.yml')


def ascii_normalizer(text):
    import re
    import unicodedata
    text = str(text).lower()
    text = text.strip()
    text = re.sub("\s\-\s", "_", text)
    text = re.sub("\s", "_", text)
    text = ''.join(ch for ch in unicodedata.normalize('NFKD', text) if not unicodedata.combining(ch))
    return text


def download_file_urllib(anp, mode='wb'):
    import urllib.request
    import shutil
    try:
        print('Starting download ... ' + anp.fold_transferred+anp.file_to_check + ' !')
        with urllib.request.urlopen(anp.url_link_file_to_check) as response, open(file=anp.fold_transferred+anp.file_to_check, mode=mode) as out_file:
            shutil.copyfileobj(response, out_file)
            #meta = response.info()
            #print(meta)
    except urllib.error.HTTPError:
        print('HTTP Error 404: URL Not Found: ' + anp.url_link_file_to_check)
        raise Exception('HTTP Error 404')
    #except:
        #print("An exception occurred")


def gravar_saida(df, file_pat, sep=';', decimal=',', na_rep='', quoting=csv.QUOTE_ALL, quotechar='"', encoding='utf-8'):
    print('Gravando arquivo: ' + file_pat + ' ....')
    df.to_csv(path_or_buf=file_pat, sep=sep, decimal=decimal, na_rep=na_rep, quoting=quoting, quotechar=quotechar, encoding=encoding)


def read_file(anp, file_extension, header=0, na_values='-', decimal=',', dayfirst=True):
    print('Lendo aquivo: ' + anp.fold_verify + anp.file_to_check + file_extension + ' ....')
    if file_extension == '.xlsx':
        anp.dict_csv_files[anp.file_to_check] = pd.read_excel(os.path.abspath(anp.fold_verify + anp.file_to_check + file_extension), na_values=na_values, keep_default_na=True, sheet_name=0, header=header).rename(ascii_normalizer, axis='columns')
        anp.dict_csv_columns[anp.file_to_check] = pd.Series(anp.dict_csv_files[anp.file_to_check].columns.values)
    elif file_extension == '.csv' or file_extension == '':
        file_pat = os.path.abspath(anp.fold_verify + anp.file_to_check + file_extension)
        file_encoding = commons.detect_encoding(file_pat)
        delimiter = commons.detect_delimiter(file_pat)
        anp.dict_csv_files[anp.file_to_check] = pd.read_csv(filepath_or_buffer=file_pat, header=header, delimiter=delimiter, encoding=file_encoding, na_values=na_values, keep_default_na=True, dayfirst=dayfirst, decimal=decimal).rename(ascii_normalizer, axis='columns')
        anp.dict_csv_columns[anp.file_to_check] = pd.Series(anp.dict_csv_files[anp.file_to_check].columns.values[0].split(';'))


# TODO: !!! possiveis metodos genericos. verificar necessidade
def check_file_name_lastdate(anp):
    import pathlib, datetime
    from urllib.parse import urlparse
    anp.file_to_check = os.path.basename(urlparse(anp.url_link_file_to_check).path)
    if anp.file_to_check in os.listdir(anp.fold_transferred):
        anp.file_fold_verify = pathlib.Path(anp.fold_transferred + anp.file_to_check)
        anp.mtime_1 = datetime.datetime.fromtimestamp(anp.file_fold_verify.stat().st_mtime)


def filter_dataframe(df, estado_sigla, produtos):
    return df.loc[(df['estado_sigla'].isin(estado_sigla)) & (df['produto'].isin(produtos)), :]


def print_and_log(text):
    import time
    HORA_INICIO = time.strftime("%Y_%m_%d_%H_%M_%S").lower()
    # TODO: revisar a maneira de gravar mensagens de LOG...
    print('"file_name":"log_%s.txt"' % HORA_INICIO, text)
    with open("../logs/log_{}.txt".format(HORA_INICIO), 'a') as log:
        log.write(time.strftime("%d_%b_%Y_%H_%M_%S").lower() + " -> " + text + "\n" )


# def read_file(file_path, text_format = False, file_encoding = '', data_type = 'str'):
#     if file_encoding == '':
#         with open(file_path, 'rb') as f:
#             test_str = b''
#             count = 0
#             line = f.readline()
#             while line and count < 250:  #Set based on lines you'd want to check
#                 test_str = test_str + line
#                 count = count + 1
#                 line = f.readline()
#             result = chardet.detect(test_str)
#             file_encoding = result['encoding']
#             print(file_encoding, file_path)
#     if text_format:
#         with io.open(file_path, 'r', encoding=file_encoding) as f_text:
#             df = f_text.read()
#     else:
#         df = pd.read_csv(file_path, sep = '\t', encoding=file_encoding, dtype=data_type, decimal='.')
#     return df, file_encoding


# def remove_accented_chars(string):
#     result = ''
#     result = string.replace("Á", "A").replace("á","a").replace("Â","A").replace("â;","a")
#     result = result.replace("À", "A").replace("à","a").replace("Ä","A").replace("ä","a")
#     result = result.replace("Ã", "A").replace("ã","a")
#     result = result.replace("É", "E").replace("é","e").replace("Ê","E").replace("ê","e")
#     result = result.replace("È", "E").replace("è","e").replace("Ë","E").replace("ë","e")
#     result = result.replace("Í", "I").replace("í","i").replace("Î","I").replace("î","i")
#     result = result.replace("Ì", "I").replace("ì","i").replace("Ï","I").replace("ï","i")
#     result = result.replace("Ó", "O").replace("ó","o").replace("Ô","O").replace("ô","o")
#     result = result.replace("Ò", "O").replace("ò","o").replace("Ö","O").replace("ö","o")
#     result = result.replace("Õ", "O").replace("õ","o")
#     result = result.replace("Ú", "U").replace("ú","u").replace("Û","U").replace("û","u")
#     result = result.replace("Ù", "U").replace("ù","u").replace("Ü","U").replace("ü","u")
#     result = result.replace("Ç", "C").replace("ç","c").replace("Ñ","N").replace("ñ","n")
#     return result


