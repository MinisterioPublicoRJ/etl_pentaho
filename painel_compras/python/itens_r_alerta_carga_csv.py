import logging
import os, subprocess
import time
from datetime import datetime, timezone

import mpmapas_commons as commons
import mpmapas_logger
import numpy
import pandas as pd
import pandasql as ps
import urllib3
from db_utils import mpmapas_db_commons as dbcommons
from mpmapas_exceptions import MPMapasDataBaseException, MPMapasException
from psycopg2.extensions import register_adapter, AsIs

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
pd.options.mode.chained_assignment = None  # default='warn'


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

def set_checksumid_tables():
    # update para inclusao checksumid
    jdbc_opengeo = configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME]
    ds_name = jdbc_opengeo.jndi_name
    db = commons.get_database(jdbc_opengeo, api=None)
    schema_name = 'comprasrj'
    list_columns = ['id', 'contratacao', 'status', 'orgao', 'processo', 'objeto', 'tipo_aquisicao', 'criterio_julgamento', 
        'fornecedor', 'cpf_cnpj', 'dt_contratacao', 'vl_estimado', 'vl_empenhado', 'vl_executado', 'vl_pago', 'id_item', 
        'item', 'tp_item', 'qtd', 'qtd_aditiv_supr', 'vl_unit', 'vl_unit_aditiv_supr', 'preco_minimo', 'preco_maximo', 
        'quartil_um', 'quartil_dois', 'quartil_tres', 'n', 'periodo_analise', 'dt_ult_atualiz', 'checksumid']
    table_name = 'alertas_contratos_avaliacao'
    sql_statement = 'SELECT %s FROM %s.%s order by id' % (','.join(list(list_columns)), schema_name, table_name)
    df = pd.DataFrame(db.execute_select(sql_statement, result_mode='all'), columns=list_columns)
    if len(df) > 0:
        df_chk = df.drop(['id'], axis='columns')
        df_chk = df_chk.drop(['checksumid'], axis='columns')
        df_chk = df_chk.drop(['dt_ult_atualiz'], axis='columns')
        df['checksumid'] = commons.generate_checksum(df_chk)
        for index, row in df.iterrows():
            #dt_now = datetime.now(timezone.utc)
            dt_now = datetime.now()
            sql_cmd = "update %s.%s set checksumid ='%s', dt_ult_atualiz = '%s' where id='%s'" % (schema_name, table_name, row.checksumid, dt_now, row.id)
            result = db.execute_select(sql_cmd, result_mode=None)
    list_columns = ['id', 'contratacao', 'status', 'orgao', 'processo', 'objeto', 'tipo_aquisicao', 'criterio_julgamento', 
        'fornecedor', 'cpf_cnpj', 'dt_contratacao', 'vl_estimado', 'vl_empenhado', 'vl_executado', 'vl_pago', 'id_item', 
        'item', 'tp_item', 'qtd', 'qtd_aditiv_supr', 'vl_unit', 'vl_unit_aditiv_supr', 'preco_minimo', 'preco_maximo', 
        'quartil_um', 'quartil_dois', 'quartil_tres', 'n', 'var_perc', 'alerta', 'mensagem', 'contrato_id_item', 'periodo_analise',
        'dt_ult_atualiz', 'checksumid']
    table_name = 'alertas_contratos_produtos'
    sql_statement = 'SELECT %s FROM %s.%s order by id' % (','.join(list(list_columns)), schema_name, table_name)
    df = pd.DataFrame(db.execute_select(sql_statement, result_mode='all'), columns=list_columns)
    if len(df) > 0:
        df_chk = df.drop(['id'], axis='columns')
        df_chk = df_chk.drop(['checksumid'], axis='columns')
        df_chk = df_chk.drop(['dt_ult_atualiz'], axis='columns')
        df['checksumid'] = commons.generate_checksum(df_chk)
        for index, row in df.iterrows():
            #dt_now = datetime.now(timezone.utc)
            dt_now = datetime.now()
            sql_cmd = "update %s.%s set checksumid ='%s', dt_ult_atualiz = '%s' where id='%s'" % (schema_name, table_name, row.checksumid, dt_now, row.id)
            result = db.execute_select(sql_cmd, result_mode=None)
    table_name = 'alertas_historico'
    sql_statement = 'SELECT %s FROM %s.%s order by id' % (','.join(list(list_columns)), schema_name, table_name)
    df = pd.DataFrame(db.execute_select(sql_statement, result_mode='all'), columns=list_columns)
    if len(df) > 0:
        df_chk = df.drop(['id'], axis='columns')
        df_chk = df_chk.drop(['checksumid'], axis='columns')
        df_chk = df_chk.drop(['dt_ult_atualiz'], axis='columns')
        df['checksumid'] = commons.generate_checksum(df_chk)
        for index, row in df.iterrows():
            #dt_now = datetime.now(timezone.utc)
            dt_now = datetime.now()
            sql_cmd = "update %s.%s set checksumid ='%s', dt_ult_atualiz = '%s' where id='%s'" % (schema_name, table_name, row.checksumid, dt_now, row.id)
            result = db.execute_select(sql_cmd, result_mode=None)

# TODO: depois mover essa função db commons
def get_list_fields_from_table(obj_jdbc, schema_name, table_name, raise_error_if_changes=True):
    logger.debug('get_info_table_structure')
    ds_name = obj_jdbc.jndi_name
    json_table_info = {'dt': '%s' % int(time.time()), 'ds_name': ds_name, 'schema_name': schema_name,
                       'table_name': table_name, 'list_columns': [], 'fields': {}}
    db = commons.get_database(obj_jdbc, api='postgresql')
    list_fields = ['table_name', 'column_name', 'data_type', 'ordinal_position', 'column_default', 'is_nullable',
                   'numeric_precision', 'numeric_scale']
    select_sql = "SELECT %s FROM information_schema.columns WHERE table_name = '%s';" % (
        ", ".join(i for i in list_fields), table_name)
    df = pd.DataFrame(db.execute_select(select_sql, result_mode='all'), columns=list_fields)
    # TODO: psycopg2.OperationalError: could not connect to server: Operation timed out
    if len(df) == 0:
        if raise_error_if_changes:
            print(ds_name, schema_name, table_name)
            raise MPMapasErrorAccessingTable(connection=ds_name, schema=schema_name, table=table_name)
    else:
        for recno, row in df.iterrows():
            json_table_info['fields'][recno] = {}
            for c in df.columns:
                if 'table_name' not in c:
                    str_tmp = "%s" % row[c]
                    if 'nan' in str_tmp.lower():
                        json_table_info['fields'][recno][c] = None
                    else:
                        json_table_info['fields'][recno][c] = row[c]
            json_table_info['list_columns'].append(row['column_name'])
    return json_table_info, df


def export_table_to_csv(obj_jdbc, schema_name='comprasrj', table_name='itens_contratos_filtrados',
                        list_columns = [],
                        file_name_output='itens_contratos_filtrados.csv', folder_name_output = './'):
    logger.info('Starting %s - export_table_to_csv.' % configs.settings.ETL_JOB)
    result = 0
    if os.path.isfile(file_name_output):
        print('Exporting to an existing file [ %s ] is not allowed, delete file before in [ %s ]' % (file_name_output, folder_name_output))
    else:
        ds_name = obj_jdbc.jndi_name
        db = commons.get_database(obj_jdbc, api=None)
        print("Exporting table to CSV file...")
        print("Schema input: ds_name='%s'" % ds_name)
        print("Schema input: schema_name='%s'" % schema_name)
        print("Table input: table_name='%s'" % table_name)
        if len(list_columns) == 0:
            json_table_info, df = get_list_fields_from_table(obj_jdbc, schema_name, table_name, raise_error_if_changes=True)
            list_columns = sorted(json_table_info['list_columns'])
        print("Table input columns: list_columns='%s'" % list_columns)
        print("File output: file_name_output='%s'" % file_name_output)
        print("File output: folder_name_output='%s'" % folder_name_output)
        logger.info('Load table [%s] from schema [%s].' % (table_name, schema_name))
        sql_statement = 'SELECT %s FROM %s.%s;' % (list(list_columns), schema_name, table_name)
        for i in list_columns:
            if not i.islower():
                sql_statement = sql_statement.replace("'%s'" % i, '"%s"' % i)
        sql_statement = sql_statement.replace('[', '').replace(']', '').replace("'", '')
        
        df = pd.DataFrame(db.execute_select(sql_statement, result_mode='all'), columns=list_columns)
        complete_file_name_output = folder_name_output + os.sep + file_name_output
        # TODO: conferir tamanho do arquivo, emitir aviso para arquivos vazios?
        df.to_csv(complete_file_name_output, sep=';', header=True, index=False, encoding='utf-8')
        if os.path.isfile(complete_file_name_output):
            print("Output file updated successfully.")
    logger.info('Finish %s - export_table_to_csv.' % configs.settings.ETL_JOB)
    return result

def main():
    try:
        logger.info('Starting Task 01 export of items to folder %s.' % configs.settings.ETL_JOB)
        
        jdbc_opengeo = configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME]
        
        # TODO: carregar as duas tabelas de entrada para o script R
        # ATENÇÃO caracter usado para separador de decimal deve ser PONTO
        export_table_to_csv(obj_jdbc = jdbc_opengeo, schema_name='comprasrj', table_name='itens_contratos_filtrados',
            file_name_output='itens_contratos_filtrados.csv', folder_name_output = configs.folders.SAIDA_DIR)
        
        export_table_to_csv(obj_jdbc = jdbc_opengeo, schema_name='comprasrj', table_name='alertas_historico',
            file_name_output='alertas_historico.csv', folder_name_output = configs.folders.SAIDA_DIR)
        
        logger.info('Finish Task 01 export of items to folder %s.' % configs.settings.ETL_JOB)
        
        folder_name = os.path.abspath(configs.folders.SAIDA_DIR) 
        table_output_filename = folder_name + os.sep + 'alertas_contratos_avaliacao.csv'
        logger.info('Starting Task 02 remove [%s].' % table_output_filename)
        if os.path.isfile(table_output_filename):
            try:
                os.remove(table_output_filename)
            except OSError:
                pass
        logger.info('Finish Task 02 remove [%s].' % table_output_filename)

        folder_name = os.path.abspath(configs.folders.SAIDA_DIR)
        table_output_filename = folder_name + os.sep + 'alertas_contratos_produtos.csv'
        logger.info('Starting Task 03 remove [%s].' % table_output_filename)
        if os.path.isfile(table_output_filename):
            try:
                os.remove(table_output_filename)
            except OSError:
                pass
        logger.info('Finish Task 03 remove [%s].' % table_output_filename)

        file_script_r = 'script_alerta_precos_contratos.R'
        folder_name = os.path.abspath(configs.folders.R_SCRIPT_DIR)
        complete_name_file_script_r = folder_name + os.sep + file_script_r
        if os.path.isfile(complete_name_file_script_r):
            logger.info('Starting Task 04 run script R  [%s].' % complete_name_file_script_r)
            subprocess.call(['R', 'CMD', 'BATCH', complete_name_file_script_r])
            logger.warning('Finish Task 04 Alerta script R run...')
            ds_name = jdbc_opengeo.jndi_name
            db = commons.get_database(jdbc_opengeo, api=None)
            server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
                                                                            configs.settings.DB_OPENGEO_DS_NAME].jndi_name)
            folder_name = os.path.abspath(configs.folders.SAIDA_DIR)
            table_input_filename = folder_name + os.sep + 'alertas_contratos_produtos.csv'
            if os.path.isfile(table_input_filename):
                logger.warning('Finish Task 05 checking [alertas_contratos_produtos]...')
                dict_1 = dbcommons.load_table(configs=configs, jndi_name=jdbc_opengeo.jndi_name, schema_name='comprasrj', 
                    table_name='alertas_contratos_produtos', csv_source=table_input_filename, csvEncoding='UTF-8',
                    csvDelimiter=';', csvDecimal=',')
                df = pd.DataFrame(data=dict_1['table'], columns=dict_1['table_info']['column_name'])
                if len(df) > 0: # então temos registros a processar
                    df = df.drop(['id'], axis='columns')
                    df = df.drop(['checksumid'], axis='columns')
                    df = df.drop(['dt_ult_atualiz'], axis='columns')
                    df = df.fillna({'alerta':'','mensagem':'', })
                    df['checksumid'] = commons.generate_checksum(df)
                    dt_now = datetime.now()
                    df['dt_ult_atualiz'] = dt_now
                    list_fields = df.columns.values
                    # TODO: solicitar revisão do Gabriel pois esse ponto aqui pode melhorar
                    for index, row in df.iterrows():
                        sql_statement = "select id from comprasrj.alertas_contratos_produtos where checksumid ='%s'" % (row.checksumid)
                        df_chk = pd.DataFrame(db.execute_select(sql_statement, result_mode='all'), columns=['id'])
                        dt_now = datetime.now()
                        row['dt_ult_atualiz'] = dt_now    # vamos atualizar a data e hora do registro
                        if len(df_chk) == 0:
                            insert_sql_statement, insert_template_statement = db.insert_values_sql(schema_name='comprasrj',
                                                                                            table_name='alertas_contratos_produtos',
                                                                                            list_flds=list_fields,
                                                                                            unique_field='id', pk_field='id')
                            result = db.execute_values_insert(sql=insert_sql_statement,
                                        template=insert_template_statement,
                                        df_values_to_execute=row.to_frame().T, fetch=False,
                                        server_encoding=server_encoding)
                        sql_statement = "select id from comprasrj.alertas_historico where checksumid ='%s'" % (row.checksumid)
                        df_chk = pd.DataFrame(db.execute_select(sql_statement, result_mode='all'), columns=['id'])
                        if len(df_chk) == 0:
                            insert_sql_statement, insert_template_statement = db.insert_values_sql(schema_name='comprasrj',
                                                                                            table_name='alertas_historico',
                                                                                            list_flds=list_fields,
                                                                                            unique_field='id', pk_field='id')
                            result = db.execute_values_insert(sql=insert_sql_statement,
                                        template=insert_template_statement,
                                        df_values_to_execute=row.to_frame().T, fetch=False,
                                        server_encoding=server_encoding)
            logger.warning('Finish Task 05 load [alertas_contratos_produtos]...')
            table_input_filename = folder_name + os.sep + 'alertas_contratos_avaliacao.csv'
            if os.path.isfile(table_input_filename):
                logger.warning('Finish Task 06 checking [alertas_contratos_avaliacao]...')
                dict_1 = dbcommons.load_table(configs=configs, jndi_name=jdbc_opengeo.jndi_name, schema_name='comprasrj', 
                    table_name='alertas_contratos_avaliacao', csv_source=table_input_filename, csvEncoding='UTF-8',
                    csvDelimiter=';', csvDecimal=',')
                df = pd.DataFrame(data=dict_1['table'], columns=dict_1['table_info']['column_name'])
                if len(df) > 0: # então temos registros a processar
                    df = df.drop(['id'], axis='columns')
                    df = df.drop(['checksumid'], axis='columns')
                    df = df.drop(['dt_ult_atualiz'], axis='columns')
                    df['checksumid'] = commons.generate_checksum(df)
                    dt_now = datetime.now()
                    df['dt_ult_atualiz'] = dt_now
                    df = df.fillna({'alerta':'','mensagem':'', })
                    list_fields = df.columns.values
                    # TODO: solicitar revisão do Gabriel pois esse ponto aqui pode melhorar
                    for index, row in df.iterrows():
                        sql_statement = "select id from comprasrj.alertas_contratos_avaliacao where checksumid ='%s'" % (row.checksumid)
                        df_chk = pd.DataFrame(db.execute_select(sql_statement, result_mode='all'), columns=['id'])
                        if len(df_chk) == 0:
                            dt_now = datetime.now()
                            row['dt_ult_atualiz'] = dt_now    # vamos atualizar a data e hora do registro
                            insert_sql_statement, insert_template_statement = db.insert_values_sql(schema_name='comprasrj',
                                                                                            table_name='alertas_contratos_avaliacao',
                                                                                            list_flds=list_fields,
                                                                                            unique_field='id', pk_field='id')
                            result = db.execute_values_insert(sql=insert_sql_statement,
                                        template=insert_template_statement,
                                        df_values_to_execute=row.to_frame().T, fetch=False,
                                        server_encoding=server_encoding)
            logger.warning('Finish Task 06 load [alertas_contratos_avaliacao]...')
        else:
            # TODO: incluir essa execeção em UTIL/mpmapas_python_utils/mpmapas_exceptions.py
            # raise MPMapasErrorFileNotFound(etl_name='painel_compras', folder=folder_name, file_name=file_script_r)
            
            # TODO: depois de excluir a execeção retirar essas instruções de print abaixo            
            print("MPMapasErrorFileNotFound['etl_name'] : 'painel_compras'")
            print("MPMapasErrorFileNotFound['folder'] : '%s'" % folder_name)
            print("MPMapasErrorFileNotFound['file_name'] : '%s'" % file_script_r)
            logger.info('Fail Task 03 run script R file not found: [%s].' % complete_name_file_script_r)

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
        logger.info('Finishing item exports to CSV in %s folder.' % configs.folders.SAIDA_DIR)


global configs, logger

if __name__ == '__main__' or __name__ == 'itens_classificar':
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
