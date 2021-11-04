import logging
import os, subprocess, sys
import time
from datetime import datetime, timezone
from arcgis.gis import GIS

import json
import mpmapas_commons as commons
import mpmapas_logger
import numpy
import pandas as pd
import urllib3
from db_utils import mpmapas_db_commons as dbcommons
from mpmapas_exceptions import MPMapasDataBaseException, MPMapasErrorFileNotFound, MPMapasException
from psycopg2.extensions import register_adapter, AsIs, Float

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
pd.options.mode.chained_assignment = None  # default='warn'


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def nan_to_null(f, _NULL=AsIs('NULL'), _Float=Float):
    if not numpy.isnan(f):
        return _Float(f)
    return _NULL


register_adapter(float, nan_to_null)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)


def load_text_config_file (file):
    text = ''
    with open(file, encoding='utf-8-sig') as text_file:
        text = text_file.read()
    return text

def load_json_config_file (file):
    json_data = {}
    with open(file, encoding='utf-8-sig') as json_file:
        text = json_file.read()
        json_data = json.loads(text)
    return json_data


def get_list_json_from_arcgis (url, user, password, search_info):
    gisSource=GIS(url, user, password, verify_cert=False)
    search_info_stakeholder = 'id:"%s"' % search_info
    items = gisSource.content.search(query=search_info_stakeholder)
    layer = items[0].layers[0]
    r = layer.query()
    return r.features


def chk_value(value, type_value):
    result = value
    if result is not None:
        if 'datetime' == type_value:
            result = datetime.utcfromtimestamp(result/1000).strftime('%Y-%m-%d %H:%M:%S')
        elif 'datetime3' == type_value:
            result = result - (3*3600)  # acertando o fuso para o data hora incluídas automaticamente pelo ArcGIS
            result = datetime.utcfromtimestamp(result/1000).strftime('%Y-%m-%d %H:%M:%S')
        elif 'float' == type_value:
            result = float(value)
        elif 'int' == type_value:
            if type(value) is not int:
                if 'nao' in value.lower():
                    result = 0
                elif 'sim' in value.lower():
                    result = 1
                else:
                    result = int(value)
    return result


def convert_json_file_to_csv(file, config, checksum_column='checksumid'):
    df_result = None
    answer = {}
    text = load_text_config_file (file)
    # TODO: ponto de revisão: verificar como tratar o 'null' depois devido a "Features"/JSON do ArcGIS
    null = None
    answer = eval(text)
    list_attr_name_column = {}
    list_attr_name_type = {}
    records = []
    for a in config:
        list_attr_name_column[a['name']] = a['column']
        list_attr_name_type[a['name']] = a['type']
    if len(answer) > 0:
        list_fields = {}
        for r in answer:
            list_fields={}
            for a in r['attributes']:
                if a in list_attr_name_column:
                    value = r['attributes'][a]
                    column = list_attr_name_column[a]
                    list_fields[column] = chk_value(value, list_attr_name_type[a])
            records.append(list_fields)
    if len(records) > 0:
        json_object = json.dumps(records)
        df_result = pd.read_json(json_object)
        for r in answer:    # todo: ponto de revisão
            for a in r['attributes']:
                column = list_attr_name_column[a]
                if 'int' in list_attr_name_type[a] and column in df_result.columns:
                    df_result[column] = df_result[column].fillna(value=-1)
                    df_result[column] = df_result[column].astype('int')
                    #df_result[column] = df_result[column].replace(to_replace=-1, value=numpy.NaN)
                    #print(130, 'df_result[column] =', df_result[column])
        df_result[checksum_column] = commons.generate_checksum(df_result)
        file_output, file_extension = os.path.splitext(file)
        complete_file_name = file_output + '.csv'
        commons.gravar_saida(df_result, complete_file_name)
    return df_result


def export_json_from_arcgis_to_file (file, url, user, password, search_info):
    list_features = get_list_json_from_arcgis(url, user, password, search_info)
    # Formato Features usado pelo ArcGIS é JSON "pero no mucho"
    # <https://pro.arcgis.com/en/pro-app/2.7/tool-reference/conversion/features-to-json.htm> 
    #with open(file, 'w', encoding='utf-8') as f:
    #    json.dump(list_features, f, ensure_ascii=False, indent=4)
    size = 0
    with open(file, 'w', encoding='utf-8-sig') as fout:
        text = str(list_features)
        size = fout.write(text)
    return size


def get_df_from_arcgis_stakeholder(file_config, file_output):
    df = None
    config = load_json_config_file(file_config)
    url = config['url']
    user = config['user']
    password = config['password']
    search_info = config['search_info']
    export_json_from_arcgis_to_file (file_output, url, user, password, search_info)
    df = convert_json_file_to_csv(file_output, config['attributes_to_table_fiels'])
    return df

def import_csv_to_table(obj_jdbc, table_name, file_name, folder_name, df_chk_already_loaded,
                        schema_name='assistencia', checksum_column='checksumid',
                        checksum_drop_column_list=['id', 'checksumid', 'dt_ult_atualiz'],
                        fillna_values_column_list={'note0': '', 'note1': '', 'note2': '',},
                        datetime_field='dt_ult_atualiz',
                        unique_field='id', pk_field='id', raise_error_if_file_does_not_exist=True):
    
    folder_name = os.path.abspath(folder_name)
    complete_file_name = folder_name + os.sep + file_name

    logger.info('Starting [%s] - import_csv_to_table.' % complete_file_name)

    db = commons.get_database(obj_jdbc, api=None)
    server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GISDB_DS_NAME].jndi_name)

    if os.path.isfile(complete_file_name):
        dict_1 = dbcommons.load_table(configs=configs, jndi_name=obj_jdbc.jndi_name, schema_name=schema_name,
                                      table_name=table_name, csv_source=complete_file_name, csvEncoding='UTF-8',
                                      csvDelimiter=';', csvDecimal=',')
        df = pd.DataFrame(data=dict_1['table'], columns=dict_1['table_info']['column_name'])
        if len(df) > 0:
            # vamos separar registros que não foram enviados ainda
            #checksum_drop_column_list.append('shape')
            #checksum_drop_column_list.append('gdb_geomattr_data')
            for f in checksum_drop_column_list:
                df = df.drop([f], axis='columns')
            df = df.fillna(fillna_values_column_list)
            df = df.where(pd.notnull(df), None)
            df[checksum_column] = commons.generate_checksum(df)
            dt_now = datetime.now()
            df[datetime_field] = dt_now
            list_fields = df.columns.values
            df_insert = df.loc[~df[checksum_column].isin(df_chk_already_loaded[checksum_column])]
            if len(df_insert) > 0:
                insert_sql_statement, insert_template_statement = db.insert_values_sql(schema_name=schema_name,
                                                                                       table_name=table_name,
                                                                                       list_flds=list_fields,
                                                                                       unique_field=unique_field,
                                                                                       pk_field=pk_field)
                result = db.execute_values_insert(sql=insert_sql_statement,
                                                  template=insert_template_statement,
                                                  df_values_to_execute=df_insert, fetch=False,
                                                  server_encoding=server_encoding)
            # vamos atualizar o campo "shape" de cada tabela com as informações de geometria
            cmd = 'update ' + schema_name + '.' + table_name + \
                    ' set shape = ST_SetSRID( st_POINT (y::double precision, x::double precision), 4326) ' + \
                    'where shape is null and y is not null and x is not null;'
            db.execute_select(sql=cmd, result_mode = 'no_result', list_values=[])
        logger.info('Finishing import [%s] to [%s.%s] - import_csv_to_table.' % (file_name, schema_name, table_name))
    else:
        logger.info('Finishing NOK [%s] - import_csv_to_table.' % complete_file_name)
        if raise_error_if_file_does_not_exist:
            raise MPMapasErrorFileNotFound(etl_name=configs.settings.ETL_JOB, error_name='Import CSV file not found',
                                           abs_path=folder_name, file_name=file_name)


def main():
    try:
        jdbc_opengeo = configs.settings.JDBC_PROPERTIES[configs.settings.DB_GISDB_DS_NAME]
        folder_config = configs.folders.CONFIG_DIR
        folder_output = configs.folders.SAIDA_DIR

        file_config = folder_config + os.sep + 'config_01_arcgis_detran.json'
        file_output = folder_output + os.sep + 'answer_01_detran.json'
        df_input_detran = get_df_from_arcgis_stakeholder(file_config, file_output)

        file_config = folder_config + os.sep + 'config_02_arcgis_hospital.json'
        file_output = folder_output + os.sep + 'answer_02_hospital.json'
        df_input_hospital = get_df_from_arcgis_stakeholder(file_config, file_output)
        
        file_config = folder_config + os.sep + 'config_03_arcgis_cartorio.json'
        file_output = folder_output + os.sep + 'answer_03_cartorio.json'
        df_input_cartorio = get_df_from_arcgis_stakeholder(file_config, file_output)

        import_csv_to_table(obj_jdbc=jdbc_opengeo, table_name='survey_nascer_legal_detran',
                            file_name='answer_01_detran.csv', folder_name=folder_output,
                            df_chk_already_loaded=df_input_detran, unique_field='checksumid',
                            raise_error_if_file_does_not_exist=False)

        import_csv_to_table(obj_jdbc=jdbc_opengeo, table_name='survey_nascer_legal_hosp',
                            file_name='answer_02_hospital.csv', folder_name=folder_output,
                            df_chk_already_loaded=df_input_hospital, unique_field='checksumid',
                            raise_error_if_file_does_not_exist=False)

        import_csv_to_table(obj_jdbc=jdbc_opengeo, table_name='survey_nascer_legal_cart_3',
                            file_name='answer_03_cartorio.csv', folder_name=folder_output,
                            df_chk_already_loaded=df_input_cartorio, unique_field='checksumid',
                            raise_error_if_file_does_not_exist=False)

    except MPMapasDataBaseException as c_err:
        logger.exception(c_err)
        exit(c_err.error_code)
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)
    finally:
        logger.info('Finishing item imports CSV to %s database.' % configs.settings.DB_GISDB_DS_NAME)

global configs, logger

if __name__ == '__main__' or __name__ == 'nascer_legal_get_data_arcgis':
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