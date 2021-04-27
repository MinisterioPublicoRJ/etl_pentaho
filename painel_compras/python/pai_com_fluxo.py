# -*- coding: utf-8 -*-

# WARNNING: This code is totally out of standards used in MPRJ and I left it here to remind me of what not to do.

import os, sys, time, argparse, binascii, traceback
from datetime import datetime, timezone
from decimal import Decimal
from sys import platform

import numpy
from psycopg2.extensions import register_adapter, AsIs


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

dt_now = datetime.now(timezone.utc)


def replace_inf_nan(df):
    import numpy as np
    import math
    df = df.replace(np.nan, 0).replace(np.NAN, 0).replace(np.NaN, 0).replace(math.nan, 0)
    df = df.replace(np.NZERO, 0)
    df = df.replace(np.inf, -1).replace(np.infty, -1).replace(np.Inf, -1).replace(np.Infinity, -1).replace(np.NINF,
                                                                                                           -1).replace(
        np.PINF, -1).replace(math.inf, -1)
    return df


# Memento mori
def main():
    # TODO: as primeiras coisas primeiro, rever uso dessa variável local dict_log_text
    str_tmp = ''
    dict_log_text = {'dt': time.strftime("%Y_%m_%d_%H_%M_%S").lower(), 'dl': 'error', 'ti': '0', 'msg': ''}
    task_id = 0
    next_task_id = 10
    index_table = -1
    index_row = -1
    file_name = ''
    list_fields = []
    recno_db_1 = {}
    recno_crc_32 = 0
    recno_db_1_fields = []  # variável usada para facilitar o entendimento do código
    recno_db_1_values = []  # lista de tuplas a ser usada no comando execute da psycopg2
    list_source_tables = {}
    list_target_tables = {}
    json_table_info_0 = {}  # informações da tabela salvas em disco
    json_table_info_1 = {}  # informações da tabela pesquisadas na ORIGEM
    json_table_info_2 = {}  # informações da tabela pesquisadas no DESTINO
    list_update_ctl_fields = []  # fields list to be added for record update control
    list_to_check_fields = ['data_type', 'numeric_precision',
                            'numeric_scale']  # ja vai comparar se 'column_name' for a mesma
    # TODO: list_to_check_fields   = ['column_name', 'data_type', 'ordinal_position', 'column_default', 'is_nullable', 'numeric_precision', 'numeric_scale']
    unique_field = 'CHECKSUMID'
    pk_field = 'ID'
    config_file = "../etc/settings.yml"
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config_file", type=str, help="Configuration file to be used in script",
                        default=config_file)
    parser.add_argument("-t", "--task_id", type=int, help="Identifier of initial task to be performed", default=task_id)
    parser.add_argument("-n", "--next_task_id", type=int, help="Identifier of the next task to be performed",
                        default=next_task_id)
    parser.add_argument("-p", "--path_append", type=str, help="Folder to search for modules to import", default="")
    try:
        args = parser.parse_args()
    except Exception:
        traceback.print_exc()
        print('pai_com_fluxo.py -c <config_file> -t <task_id> -n <next_task_id> -p <path_append>')
        print('Example:')
        if platform == "linux" or platform == "linux2" or platform == "darwin":
            print('pai_com_fluxo.py -c "../etc/settings.yml" -t 0 -n 10 -p "/usr/src/python_utils)"')
        else:
            print('pai_com_fluxo.py -c "..\\etc\\settings.yml" -t 0 -n 10 -p "E:\\pentaho\\etl\\UTIL\\python_utils)"')
    if args.config_file:
        config_file = args.config_file
    if args.task_id:
        # TODO: criticar os parâmetros
        task_id = args.task_id
    if args.next_task_id:
        # TODO: criticar os parâmetros
        next_task_id = args.next_task_id
    if args.path_append:
        if os.path.isdir(args.path_append):
            sys.path.append(args.path_append)
        else:
            dict_log_text['msg'] = "Folder NOT found: %s" % args.path_append
            print(dict_log_text)
            dict_log_text['msg'] = "Path NOT added sys.path.append: %s" % args.path_appendg
            print(dict_log_text)
            sys.exit()
    if not os.path.isfile(config_file):
        print("ERROR - Configuration file not found:", config_file)
        print("ERROR - It is not possible to continue without it!")
        sys.exit()
    # TODO: precisamos conferir utilitários e bibliotecas.
    try:
        import pandas as pd
        pd.options.mode.chained_assignment = None  # default='warn'
    except Exception:
        dict_log_text['msg'] = "Pandas: python3 -m pip install pandas"
        dict_log_text['url'] = "https://pypi.org/project/pandas/"
        print(dict_log_text)
        traceback.print_exc()
        dict_log_text[
            'msg'] = "pai_com_fluxo.py: não é possível continuar sem 'pandas'... ela disse que nos encontraríamos em um dia de sol!"
        print(dict_log_text)
        next_task_id = 17
        exit()
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        dict_log_text['msg'] = "urllib3: python3 -m pip install urllib3"
        dict_log_text['url'] = "https://pypi.org/project/urllib3/"
        print(dict_log_text)
        traceback.print_exc()
        next_task_id = 17
        dict_log_text[
            'msg'] = "pai_com_fluxo.py: não é possível continuar sem 'urllib3'... ela disse que nos encontraríamos em um dia de sol!"
        print(dict_log_text)
        exit()
    try:
        import yaml
    except Exception:
        dict_log_text['msg'] = "YAML: python3 -m pip install pyyaml"
        dict_log_text['url'] = "https://zetcode.com/python/yaml/"
        print(dict_log_text)
        traceback.print_exc()
        next_task_id = 17
        dict_log_text[
            'msg'] = "pai_com_fluxo.py: não é possível continuar sem 'yaml'... ela disse que nos encontraríamos em um dia de sol!"
        print(dict_log_text)
        exit()
    try:
        from pyjavaproperties import Properties
    except Exception:
        dict_log_text['msg'] = "pyjavaproperties: python3 -m pip install pyjavaproperties"
        dict_log_text['url'] = "https://pypi.org/project/pyjavaproperties/"
        print(dict_log_text)
        traceback.print_exc()
        next_task_id = 17
        dict_log_text[
            'msg'] = "pai_com_fluxo.py: não é possível continuar sem 'pyjavaproperties'... ela disse que nos encontraríamos em um dia de sol!"
        print(dict_log_text)
        exit()
    try:
        import psycopg2
        from psycopg2 import sql

    except Exception:
        dict_log_text[
            'msg'] = "pyjavaproperties: python3 -m pip install psycopg2 or pip install psycopg2-binary (Mac OS X Apple M1)"
        dict_log_text['url'] = "https://pypi.org/project/psycopg2/"
        print(dict_log_text)
        traceback.print_exc()
        next_task_id = 17
        dict_log_text[
            'msg'] = "pai_com_fluxo.py: não é possível continuar sem 'psycopg2'... ela disse que nos encontraríamos em um dia de sol!"
        print(dict_log_text)
        exit()
    try:  # bibliotecas personalizadas, para usar você precisará adicionar o path antes por parâmetro de linha de comando
        import mpmapas_commons as commons
        from db_utils import mpmapas_db_commons as dbcommons
    except Exception:
        dict_log_text[
            'msg'] = "mpmapas_commons.py: python3 -m pip install psycopg2 or pip install psycopg2-binary (Mac OS X Apple M1)"
        print(dict_log_text)
        traceback.print_exc()
        next_task_id = 17
        dict_log_text[
            'msg'] = "pai_com_fluxo.py: não é possível continuar sem 'psycopg2'... ela disse que nos encontraríamos em um dia de sol!"
        print(dict_log_text)
        exit()
    try:
        configs = commons.read_config(config_file)
        if os.path.isdir(configs.settings.PYTHON_UTILS):
            sys.path.append(configs.settings.PYTHON_UTILS)
        else:
            dict_log_text['msg'] = "Configuration file:%s" % config_file
            print(dict_log_text)
            dict_log_text['msg'] = "Check value set in parameter configs.settings.PYTHON_UTILS"
            print(dict_log_text)
            dict_log_text['msg'] = "Folder NOT found:", configs.settings.PYTHON_UTILS
            print(dict_log_text)
            dict_log_text['msg'] = "Path NOT added sys.path.append:", configs.settings.PYTHON_UTILS
            print(dict_log_text)
            dict_log_text['msg'] = "It is not possible to continue without it!"
            print(dict_log_text)
            sys.exit()
    except Exception:
        traceback.print_exc()
        dict_log_text[
            'msg'] = "pai_com_fluxo.py: não é possível continuar sem definir configs... isso é embaraçoso, desculpe-me!"
        print(dict_log_text)
        exit()
        next_task_id = 17
    try:
        import logger2x
        my_log = logger2x.logger2x()
    except Exception:
        dict_log_text['msg'] = "Você precisa da logger2x"
        print(dict_log_text)
        traceback.print_exc()
        dict_log_text[
            'msg'] = "pai_com_fluxo.py: não é possível continuar sem 'logger2x'... ela disse que nos encontraríamos em um dia de sol!"
        print(dict_log_text)
        next_task_id = 17
        exit()
    try:
        my_log.folder_log = configs.folders.LOG_DIR
        my_log.prefix_file_name = "log_pai_com_"
        my_log.setting('dl', 'warning')
        my_log.logging('msg', 'Following parameters of this execution...')
        my_log.logging('msg', 'configs.folders.LOG_DIR: "%s"' % configs.folders.LOG_DIR)
        my_log.logging('msg', 'configs.settings.ETL_JOB: "%s"' % configs.settings.ETL_JOB)
        my_log.logging('msg', 'configs.settings.DB_SOURCE_DS_NAME: "%s"' % configs.settings.DB_SOURCE_DS_NAME)
        my_log.logging('msg', 'configs.settings.DB_TARGET_DS_NAME: "%s"' % configs.settings.DB_TARGET_DS_NAME)
        my_log.logging('msg', 'configs.settings.JDBC_PROPERTIES[configs.settings.DB_SOURCE_DS_NAME].jndi_name: "%s"' %
                       configs.settings.JDBC_PROPERTIES[configs.settings.DB_SOURCE_DS_NAME].jndi_name)
        my_log.logging('msg', 'configs.settings.JDBC_PROPERTIES[configs.settings.DB_SOURCE_DS_NAME].user: "%s"' %
                       configs.settings.JDBC_PROPERTIES[configs.settings.DB_SOURCE_DS_NAME].user)
        my_log.logging('msg', 'configs.settings.JDBC_PROPERTIES[configs.settings.DB_TARGET_DS_NAME].jndi_name: "%s"' %
                       configs.settings.JDBC_PROPERTIES[configs.settings.DB_TARGET_DS_NAME].jndi_name)
        my_log.logging('msg', 'configs.settings.JDBC_PROPERTIES[configs.settings.DB_TARGET_DS_NAME].user: "%s"' %
                       configs.settings.JDBC_PROPERTIES[configs.settings.DB_TARGET_DS_NAME].user)
        my_log.logging('msg', 'configs.folders.PYTHON_SCRIPT_DIR: "%s"' % configs.folders.PYTHON_SCRIPT_DIR)
        my_log.logging('msg', 'configs.folders.ENTRADA_DIR: "%s"' % configs.folders.ENTRADA_DIR)
        my_log.setting('dl', 'error')
    except Exception:
        traceback.print_exc()
        my_log.logging('msg', "Foguete explodiu antes do lançamento")
        my_log.logging('msg',
                       "pai_com_fluxo.py: não é possível continuar sem definições mínimas...  isso é embaraçoso, desculpe-me!")
        next_task_id = 17
        exit()
    # inicialização padrão de objetos
    df_result = pd.DataFrame()
    df_result_1 = pd.DataFrame()
    # TODO: precisamos conferir se temos o wget instalado por aqui dependendo do SO
    while True:
        my_log.setting('ti', task_id)
        # my_log.setting('dl', 'debug')
        # my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
        if task_id == 0:  # verificar parâmetros e diretórios
            next_task_id = 1
            try:
                if not os.path.isdir(my_log.folder_log):
                    os.makedirs(my_log.folder_log)
            except Exception:
                next_task_id = 150
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 1:  # vamos iniciar a lista de tabelas de interesse
            next_task_id = 10
            index_table = -1
            list_fields = ['table_name', 'column_name', 'data_type', 'ordinal_position', 'column_default',
                           'is_nullable', 'numeric_precision', 'numeric_scale']
            list_source_tables[0] = {'table_schema': 'stage', 'table_name': 'ComprasRJ_Contrato'}
            list_target_tables[0] = {'table_schema': 'comprasrj', 'table_name': 'contrato'}
            list_source_tables[1] = {'table_schema': 'stage', 'table_name': 'ComprasRJ_ItemContrato'}
            list_target_tables[1] = {'table_schema': 'comprasrj', 'table_name': 'item_contrato'}
            json_table_info_0 = {'dt': '%s' % int(time.time()), 'table_name': '', 'fields': {}}
            json_table_info_1 = {'dt': '%s' % int(time.time()), 'table_name': '', 'fields': {}}
            json_table_info_2 = {'dt': '%s' % int(time.time()), 'table_name': '', 'fields': {}}
            # Campos acrescentados na tabela de DESTINO para controle do processo de importação de registro
            # TODO: eu preferia que o campo CHECKSUMID tivesse o nome ETL_REC_CRC32
            list_update_ctl_fields = ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO']
        elif task_id == 10:  # verificar se todas as tabelas já foram processadas
            next_task_id = 20
            try:
                if index_table >= len(list_source_tables) - 1:
                    next_task_id = 1000
                else:
                    index_table = index_table + 1
                    my_log.setting('dt', time.strftime("%Y_%m_%d_%H_%M_%S").lower())
            except Exception:
                next_task_id = 11
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
        elif task_id == 11:  # erro na verificação de lista de tabelas
            next_task_id = 17
            try:
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'Não foi possível obter index_table de list_source_tables')
                my_log.logging('msg', 'index_table = %s' % index_table)
                my_log.logging('msg', 'list_source_tables = %s' % list_source_tables)
            except Exception:
                next_task_id = 17
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 17:  # TODO: as coisas não acabaram bem, vamos decidir depois se precisamos registrar mais sobre
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Tempos melhores virão! :-)')
            break
        elif task_id == 20:  # coletar informações sobre os campos da tabela ORIGEM
            next_task_id = 30
            try:
                json_table_info_1 = {'dt': '%s' % int(time.time()),
                                     'table_name': list_source_tables[index_table]['table_name'], 'fields': {}}
                db_source = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_SOURCE_DS_NAME],
                                                 api='postgresql')
                list_fields = ['table_name', 'column_name', 'data_type', 'ordinal_position', 'column_default',
                               'is_nullable', 'numeric_precision', 'numeric_scale']
                select_sql = "SELECT %s FROM information_schema.columns WHERE table_name = '%s';" % (
                ", ".join(i for i in list_fields), list_source_tables[index_table]['table_name'])
                df_result = pd.DataFrame(db_source.execute_select(select_sql, result_mode='all'), columns=list_fields)
                # TODO: temos que tratar esse erro aqui depois psycopg2.OperationalError: could not connect to server: Operation timed out
                if len(df_result) == 0:
                    next_task_id = 22
                else:
                    for recno, row in df_result.iterrows():
                        json_table_info_1['fields'][recno] = {}
                        for c in df_result.columns:
                            if 'table_name' not in c:
                                str_tmp = "%s" % row[c]
                                if 'nan' in str_tmp.lower():
                                    json_table_info_1['fields'][recno][
                                        c] = None  # TODO: depois todo campo None vai virar NULL
                                else:
                                    json_table_info_1['fields'][recno][c] = row[c]
            except Exception:
                next_task_id = 21
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 21:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro ao acessar a tabela [%s] no banco de dados [%s]"' % (
            list_source_tables[index_table]['table_name'], configs.settings.DB_SOURCE_DS_NAME))
        elif task_id == 22:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro ao pesquisar a estrutura da tabela [%s] no banco de dados ORIGEM [%s]"' % (
            list_source_tables[index_table]['table_name'], configs.settings.DB_SOURCE_DS_NAME))
        elif task_id == 30:  # conferir se os campos da tabela ainda são os mesmos
            next_task_id = 40
            try:  # TODO: será que o melhor lugar para esse arquivo é na pasta ENTRADA já que é um arquivo que o SCRIPT escreve?
                file_name = "%ssource-%s-%s.txt" % (
                configs.folders.ENTRADA_DIR, list_source_tables[index_table]['table_schema'],
                list_source_tables[index_table]['table_name'])
                if os.path.isfile(file_name):
                    with open(file_name, 'r') as fw:
                        json_table_info_0 = eval(fw.read())
                    # dicionários são inalteráveis, vamos converter em strings para classificação
                    sorted_0 = eval('%s' % json_table_info_0)
                    sorted_1 = eval('%s' % json_table_info_1)
                    # vamos desconsiderar campos acrescentados para o processo de ETL
                    for f in list_update_ctl_fields:  # ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO']
                        if f in sorted_0:
                            del sorted_0[f]
                    sorted_0 = sorted([repr(x) for x in sorted_0])
                    sorted_1 = sorted([repr(x) for x in sorted_1])
                    # TODO: se mudou então vamos parar a extração
                    if sorted_0 != sorted_1:
                        next_task_id = 32
                else:
                    with open(file_name, 'a') as fw:  # TODO: quando vamos atualizar esse arquivo? Quando excluir?
                        fw.write("%s\n" % json_table_info_1)  # na primeira vez vamos salvar o que foi lido do banco
            except Exception:
                next_task_id = 31
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 31:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro ao conferir se há modificações na tabela [%s] no banco de dados ORIGEM [%s]' % (
            list_source_tables[index_table]['table_name'], configs.settings.DB_SOURCE_DS_NAME))
            my_log.logging('msg', '{"index_table":"%s"}' % index_table)
            my_log.logging('msg', '{"len(list_source_tables)":"%s"}' % len(list_source_tables))
            my_log.logging('msg', '{"list_source_tables":"%s"}' % list_source_tables)
        elif task_id == 32:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro há modificações na estrutura da tabela [%s] no banco de dados [%s]' % (
            list_source_tables[index_table]['table_name'], configs.settings.DB_SOURCE_DS_NAME))
            my_log.logging('msg', '{"index_table":"%s"}' % index_table)
            my_log.logging('msg', '{"len(list_source_tables)":"%s"}' % len(list_source_tables))
            my_log.logging('msg', '{"list_source_tables":"%s"}' % list_source_tables)
        elif task_id == 40:  # verificar se no banco DESTINO a tabela existe
            next_task_id = 50
            try:
                json_table_info_2 = {'dt': '%s' % int(time.time()),
                                     'table_name': list_target_tables[index_table]['table_name'], 'fields': {}}
                db_target = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_TARGET_DS_NAME],
                                                 api='postgresql')
                list_fields = ['table_name', 'column_name', 'data_type', 'ordinal_position', 'column_default',
                               'is_nullable', 'numeric_precision', 'numeric_scale']
                select_sql = "SELECT %s FROM information_schema.columns WHERE table_name = '%s';" % (
                ", ".join(i for i in list_fields), list_target_tables[index_table]['table_name'])
                df_result = pd.DataFrame(db_target.execute_select(select_sql, result_mode='all'), columns=list_fields)
                if len(df_result) == 0:
                    next_task_id = 42
                else:
                    for recno, row in df_result.iterrows():
                        json_table_info_2['fields'][recno] = {}
                        for c in df_result.columns:
                            if 'table_name' not in c:
                                str_tmp = "%s" % row[c]
                                if 'nan' in str_tmp.lower():
                                    json_table_info_2['fields'][recno][
                                        c] = None  # TODO: depois todo campo None vai virar NULL
                                else:
                                    json_table_info_2['fields'][recno][c] = row[c]
            except Exception:
                next_task_id = 41
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 41:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro há modificações na estrutura da tabela [%s] no banco de dados DESTINO [%s]' % (
            list_target_tables[index_table]['table_name'], configs.settings.DB_TARGET_DS_NAME))
        elif task_id == 42:
            next_task_id = 60
            my_log.setting('dl', 'warnning')
            my_log.logging('msg',
                           'Aviso NÃO foi encontrada a estrutura da tabela [%s] no banco de dados DESTINO [%s]' % (
                           list_target_tables[index_table]['table_name'], configs.settings.DB_TARGET_DS_NAME))
            my_log.logging('msg', 'A tabela [%s] será criada no banco de dados DESTINO [%s]' % (
            list_target_tables[index_table]['table_name'], configs.settings.DB_TARGET_DS_NAME))
        elif task_id == 50:  # Existe no banco DESTINO então precisamos conferir se todos os campos da ORIGEM estão no DESTINO e são do mesmo tipo
            next_task_id = 70
            try:
                # dicionários são inalteráveis, vamos converter em strings para classificação
                can_i_proceed = False
                for recno in json_table_info_1['fields']:
                    for recno2 in json_table_info_2['fields']:
                        if json_table_info_1['fields'][recno]['column_name'] == json_table_info_2['fields'][recno2][
                            'column_name']:
                            for k in list_to_check_fields:
                                if json_table_info_1['fields'][recno][k] == json_table_info_2['fields'][recno2][k]:
                                    can_i_proceed = True
                                else:
                                    can_i_proceed = False
                                    my_log.setting('dl', 'error')
                                    my_log.logging('msg',
                                                   'Definition for attribute "%s" in column "%s" is not same at source and destination: "%s" != "%s"' % \
                                                   (k, json_table_info_1['fields'][recno]['column_name'],
                                                    json_table_info_1['fields'][recno][k],
                                                    json_table_info_2['fields'][recno2][k]))
                                    break
                if not can_i_proceed:
                    next_task_id = 52
            except Exception:
                next_task_id = 51
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 51:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro ao conferir se campos da tabela ORIGEM [%s] estão na tabela DESTINO [%s]' % (
            list_source_tables[index_table]['table_name'], list_target_tables[index_table]['table_name']))
            my_log.logging('msg', '{"index_table":"%s"}' % index_table)
            my_log.logging('msg', '{"len(list_source_tables)":"%s"}' % len(list_source_tables))
            my_log.logging('msg', '{"list_source_tables":"%s"}' % list_source_tables)
            my_log.logging('msg', '{"len(list_target_tables)":"%s"}' % len(list_target_tables))
            my_log.logging('msg', '{"list_target_tables":"%s"}' % list_target_tables)
        elif task_id == 52:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro há diferenças na estrutura da tabela ORIGEM [%s] e a tabela DESTINO [%s]' % (
            list_source_tables[index_table]['table_name'], list_target_tables[index_table]['table_name']))
            my_log.logging('msg', '{"index_table":"%s"}' % index_table)
            my_log.logging('msg', '{"len(list_source_tables)":"%s"}' % len(list_source_tables))
            my_log.logging('msg', '{"list_source_tables":"%s"}' % list_source_tables)
            my_log.logging('msg', '{"len(list_target_tables)":"%s"}' % len(list_target_tables))
            my_log.logging('msg', '{"list_target_tables":"%s"}' % list_target_tables)
        elif task_id == 60:  # criar a tabela no banco DESTINO   #TODO: acho que vamos retirar esse recurso de criar a tabela
            next_task_id = 70
            try:
                db_target = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_TARGET_DS_NAME],
                                                 api='postgresql')
                # TODO: list_fields = get_fields_for_sql_create(json_table_info_1)
                list_fields = []
                c_1 = ''
                c_2 = 'text'
                c_3 = 'NULL'
                for r in json_table_info_1['fields']:
                    c_1 = json_table_info_1['fields'][r]['column_name']
                    c_2 = json_table_info_1['fields'][r]['data_type']
                    if 'numeric' in json_table_info_1['fields'][r]['data_type']:
                        c_2 = 'numeric(%s,%s)' % (int(json_table_info_1['fields'][r]['numeric_precision']),
                                                  int(json_table_info_1['fields'][r]['numeric_scale']))
                    elif 'bigint' in json_table_info_1['fields'][r]['data_type']:
                        c_2 = 'int8'
                    if 'YES' in json_table_info_1['fields'][r]['is_nullable']:
                        c_3 = 'NULL'
                    else:
                        c_3 = 'NOT NULL'
                    list_fields.append('"%s" %s %s' % (c_1, c_2, c_3))
                # TODO: colocar o trecho acima em uma função separada na classe "e/pentaho/etl/python_utils/db_utils/mpmapas_db_postgresql.py"?
                select_sql = "CREATE TABLE %s.%s ('%s');" % (
                list_target_tables[index_table]['table_schema'], list_target_tables[index_table]['table_name'],
                ", ".join(i for i in list_fields))
                df_result = pd.DataFrame(db_source.execute_select(select_sql, result_mode='all'),
                                         columns=['table_name', 'column_name', 'data_type'])
                if len(df_result) == 0:
                    next_task_id = 62
                else:
                    for recno, row in df_result:
                        json_table_info_1['fields'][recno] = {'column_name': row['column_name'],
                                                              'data_type': row['data_type']}
            except Exception:
                next_task_id = 61
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 61:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro não foi possível criar a tabela [%s] no banco de dados DESTINO [%s]' % (
            list_target_tables[index_table]['table_name'], configs.settings.DB_TARGET_DS_NAME))
            my_log.logging('msg',
                           'Erro falha ao montar comando para criar a tabela [%s] no banco de dados DESTINO [%s]' % (
                           list_target_tables[index_table]['table_name'], configs.settings.DB_TARGET_DS_NAME))
        elif task_id == 62:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg',
                           'Aviso NÃO foi encontrada a estrutura da tabela [%s] no banco de dados DESTINO [%s]' % (
                           list_target_tables[index_table]['table_name'], configs.settings.DB_TARGET_DS_NAME))
            my_log.logging('msg', 'A tabela [%s] será criada no banco de dados DESTINO [%s]' % (
            list_target_tables[index_table]['table_name'], configs.settings.DB_TARGET_DS_NAME))
        elif task_id == 70:  # coletar os registros da tabela no banco de origem
            next_task_id = 80
            try:
                list_fields = []
                for r in json_table_info_1['fields']:  # colunas da tabela de ORIGEM
                    list_fields.append(json_table_info_1['fields'][r]['column_name'])
                # TODO: para testes e depuração vou coletar os 12 próximos
                # select_object = db_source.select_sql(schema_name=list_source_tables[index_table]['table_schema'], table_name=list_source_tables[index_table]['table_name'], list_flds=list_fields, list_where_clause='')
                # sql_statement = """SELECT {} FROM {} LIMIT 16"""
                sql_statement = """SELECT {} FROM {}"""
                select_object = sql.SQL(sql_statement).format(sql.SQL(", ").join(map(sql.Identifier, list_fields)),
                                                              sql.Identifier(
                                                                  list_source_tables[index_table]['table_schema'],
                                                                  list_source_tables[index_table]['table_name']))
                #
                result_sql = db_source.execute_select(select_object, result_mode='all')
                df_result = pd.DataFrame(result_sql, columns=list_fields)
                # TODO: inclui chamada de função tratar valores NULL
                # print("list_values =", df_result)
                # df_result = replace_inf_nan(df_result)
                # print("list_values =", df_result)
                if len(df_result) == 0:
                    my_log.setting('dl', 'debug')
                    my_log.logging('msg', 'Começando com tabela ORIGEM [%s] no banco de dados ORIGEM [%s]' % \
                                   (list_source_tables[index_table]['table_name'], configs.settings.DB_SOURCE_DS_NAME))
                    my_log.logging('msg', '{"index_table":"%s"}' % index_table)
                    my_log.logging('msg', '{"len_table":"%s"}' % len(df_result))
                    my_log.logging('msg', '{"len(list_source_tables)":"%s"}' % len(list_source_tables))
                    my_log.logging('msg', '{"list_source_tables":"%s"}' % list_source_tables)
                    next_task_id = 72
            except Exception:
                next_task_id = 71
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 71:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro ao coletar registros da tabela ORIGEM [%s] no banco de dados ORIGEM [%s]' % \
                           (list_source_tables[index_table]['table_name'], configs.settings.DB_SOURCE_DS_NAME))
            my_log.logging('msg', '{"index_table":"%s"}' % index_table)
            my_log.logging('msg', '{"len(list_source_tables)":"%s"}' % len(list_source_tables))
            my_log.logging('msg', '{"list_source_tables":"%s"}' % list_source_tables)
        elif task_id == 72:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro NÃO HÁ registros da tabela ORIGEM [%s] no banco de dados ORIGEM [%s]' % \
                           (list_source_tables[index_table]['table_name'], configs.settings.DB_SOURCE_DS_NAME))
            my_log.logging('msg', '{"index_table":"%s"}' % index_table)
            my_log.logging('msg', '{"len(list_source_tables)":"%s"}' % len(list_source_tables))
            my_log.logging('msg', '{"list_source_tables":"%s"}' % list_source_tables)
        elif task_id == 80:  # verificar se todos registros da tabela ORIGEM já foram processadas
            next_task_id = 90
            try:
                if index_row >= len(
                        df_result) - 1:  # se processamos todos os registros de uma tabela então vamos para a próxima
                    next_task_id = 10
                    index_row = -1  # reiniciamos a contagem de registros
                    my_log.setting('dt', time.strftime("%Y_%m_%d_%H_%M_%S").lower())
                else:
                    index_row = index_row + 1
            except Exception:
                next_task_id = 81
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 90:  # vamos calcular o CRC32 do registro de ORIGEM
            next_task_id = 100
            try:
                df_result_1 = df_result.iloc[index_row]
                list_fields = list(df_result.columns)
                list_fields = sorted(list_fields)
                recno_db_1 = {}
                # vamos verificar se teremos campos NaN
                for f in list_fields:
                    str_tmp = "%s" % df_result_1[f]
                    if 'nan' in str_tmp.lower():
                        df_result_1[f] = None  # TODO: depois todo campo None vai virar NULL
                    recno_db_1[f] = df_result_1[f]
                if len(recno_db_1) > 0:
                    str_tmp = "%s" % recno_db_1
                    b = bytes(str_tmp, 'utf-8')
                    recno_crc_32 = binascii.crc32(b)
                else:
                    next_task_id = 92
            except Exception:
                next_task_id = 91
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 91:
            next_task_id = 80
            my_log.setting('dl', 'error')
            my_log.logging('msg',
                           'Erro ao calcular CRC32 do registro %s da tabela ORIGEM [%s] no banco de dados ORIGEM [%s]' % \
                           (index_row, list_source_tables[index_table]['table_name'],
                            configs.settings.DB_SOURCE_DS_NAME))
            my_log.logging('msg', 'recno_db_1 = "%s"' % recno_db_1)
        elif task_id == 92:
            next_task_id = 17
            my_log.setting('dl', 'error')
            my_log.logging('msg', 'Erro NÃO HÁ registros da tabela ORIGEM [%s] no banco de dados ORIGEM [%s]' % \
                           (list_source_tables[index_table]['table_name'], configs.settings.DB_SOURCE_DS_NAME))
            my_log.logging('msg', 'recno_db_1 = "%s"' % recno_db_1)
        elif task_id == 100:  # vamos procurar por aquele CRC na tabela DESTINO, se existir não vamos inserir
            next_task_id = 110
            try:
                sql_statement = """select c."ID", c."DT_ULT_ATUALIZ" from {} c where c."CHECKSUMID" like '%s'""" % recno_crc_32
                select_object = sql.SQL(sql_statement).format(
                    sql.Identifier(list_target_tables[index_table]['table_schema'],
                                   list_target_tables[index_table]['table_name']))
                lst_result = db_target.execute_select(select_object, result_mode='all')
                if len(lst_result) > 0:
                    next_task_id = 80
                    # my_log.setting('dl', 'error')
                    # my_log.logging('msg', 'CHECKSUMID[%s], já foi atualizado em DT_ULT_ATUALIZ[%s] no ID[%s]' % (recno_crc_32, lst_result[0][1], lst_result[0][0]))
            except Exception:
                next_task_id = 101
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 101:
            next_task_id = 80
            my_log.setting('dl', 'error')
            my_log.logging('msg',
                           'Erro ao consultar CHECKSUMID[%s] e DT_ULT_ATUALIZ da tabela DESTINO [%s] no banco de dados DESTINO [%s]' % \
                           (recno_crc_32, list_target_tables[index_table]['table_name'],
                            configs.settings.DB_TARGET_DS_NAME))
        elif task_id == 110:  # vamos incluir os campos list_update_ctl_fields = ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO']
            next_task_id = 120
            try:
                # str_tmp = time.strftime("%Y-%m-%d").lower()
                dt = datetime.now(timezone.utc)
                df_result_1['CHECKSUMID'] = "%s" % recno_crc_32
                df_result_1['DT_EXTRACAO'] = dt
                df_result_1['DT_ULT_ATUALIZ'] = dt
                recno_db_1_fields = []
                recno_db_1_values = []
                recno_db_1_fields = list(df_result_1.keys())
                for f in df_result_1:
                    recno_db_1_values.append(f)
                # TODO: vamos acertar alguns campos que não podem ter o valor NULL e vamos alterar para ZERO
                # print(608, 'recno_db_1_values= ', recno_db_1_values)
                for f in recno_db_1_fields:
                    if df_result_1[f] is None:
                        if f in ['VL_ESTIMADO', 'VL_EMPENHADO', 'VL_EXECUTADO', 'VL_PAGO']:
                            df_result_1[f] = 0
                            recno_db_1_values[recno_db_1_fields.index(f)] = 0
                    # print(614, f, '=', df_result_1[f])
                recno_db_1_values = [tuple(recno_db_1_values)]
            except Exception:
                next_task_id = 111
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())
        elif task_id == 111:
            next_task_id = 80
            my_log.setting('dl', 'error')
            my_log.logging('msg',
                           'Erro ao adicionar CHECKSUMID, DT_EXTRACAO e DT_ULT_ATUALIZ no registro temporário %s da tabela ORIGEM [%s] no banco de dados ORIGEM [%s]' % \
                           (index_row, list_source_tables[index_table]['table_name'],
                            configs.settings.DB_SOURCE_DS_NAME))
            my_log.logging('msg', 'recno_db_1 = "%s"' % recno_db_1)
            my_log.logging('msg', 'df_result_1 = "%s"' % df_result_1)
            my_log.logging('msg', 'recno_db_1_fields = "%s"' % recno_db_1_fields)
            my_log.logging('msg', 'recno_db_1_values = "%s"' % recno_db_1_values)
        elif task_id == 120:  # trata-se de um comando INSERT
            next_task_id = 130
            try:
                sql_statement = """INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING RETURNING {},{} """
                select_object = sql.SQL(sql_statement).format(
                    sql.Identifier(list_target_tables[index_table]['table_schema'],
                                   list_target_tables[index_table]['table_name']),
                    sql.SQL(", ").join(map(sql.Identifier, recno_db_1_fields)),
                    sql.Identifier(unique_field),
                    sql.Identifier(pk_field),
                    sql.Identifier('DT_ULT_ATUALIZ'))
                lst_result = db_target.execute_select(select_object, result_mode='all_commit',
                                                      list_values=recno_db_1_values)
                if not lst_result is None and len(lst_result) > 0:
                    next_task_id = 80
                else:
                    my_log.setting('dl', 'error')
                    my_log.logging('msg',
                                   'Erro ao adicionar CHECKSUMID, DT_EXTRACAO e DT_ULT_ATUALIZ no registro %s da tabela DESTINO [%s] no banco de dados DESTINO [%s]' % \
                                   (index_row, list_source_tables[index_table]['table_name'],
                                    configs.settings.DB_SOURCE_DS_NAME))
            except Exception:
                next_task_id = 131
                traceback.print_exc()
                my_log.setting('dl', 'error')
                my_log.logging('msg', 'task_id = %s, next_task_id = %s' % (task_id, next_task_id))
                # print("Globals=",globals())
                # print("Locals=",locals())

        elif task_id == 1000:
            my_log.setting('dt', time.strftime("%Y_%m_%d_%H_%M_%S").lower())
            my_log.setting('dl', 'debug')
            my_log.logging('msg', 'Tempos melhores virão! :-)')
            break
        else:
            my_log.setting('dl', 'debug')
            my_log.setting('msg', 'Parece que não há o estado')
            break
        task_id = next_task_id


if __name__ == '__main__':
    main()

