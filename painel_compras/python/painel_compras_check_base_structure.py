import logging
import os
import time
from datetime import datetime, timezone

import mpmapas_commons as commons
import mpmapas_logger
import pandas as pd
from mpmapas_exceptions import MPMapasErrorAccessingTable, MPMapasErrorCheckingChangesTableStructure, \
    MPMapasErrorGettingTableStructure, MPMapasErrorThereAreNoRecordsInTable, MPMapasException

os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)


def get_info_table_structure(obj_jdbc, schema_name, table_name, raise_error_if_changes=True):
    logger.debug('get_info_table_structure')
    ds_name = obj_jdbc.jndi_name
    json_table_info = {'dt': '%s' % int(time.time()), 'ds_name': ds_name, 'schema_name': schema_name,
                       'table_name': table_name, 'fields': {}}
    db = commons.get_database(obj_jdbc, api='postgresql')
    list_fields = ['table_name', 'column_name', 'data_type', 'ordinal_position', 'column_default', 'is_nullable',
                   'numeric_precision', 'numeric_scale']
    select_sql = "SELECT %s FROM information_schema.columns WHERE table_name = '%s';" % (
        ", ".join(i for i in list_fields), table_name)
    df_result = pd.DataFrame(db.execute_select(select_sql, result_mode='all'), columns=list_fields)
    # TODO: psycopg2.OperationalError: could not connect to server: Operation timed out
    if len(df_result) == 0:
        if raise_error_if_changes:
            print(ds_name, schema_name, table_name)
            raise MPMapasErrorAccessingTable(connection=ds_name, schema=schema_name, table=table_name)
    else:
        for recno, row in df_result.iterrows():
            json_table_info['fields'][recno] = {}
            for c in df_result.columns:
                if 'table_name' not in c:
                    str_tmp = "%s" % row[c]
                    if 'nan' in str_tmp.lower():
                        json_table_info['fields'][recno][c] = None
                    else:
                        json_table_info['fields'][recno][c] = row[c]
    return json_table_info, df_result


def checking_changes_tables_structure_0(obj_jdbc, ds_name, schema_name, table_name, raise_error_if_changes=True):
    logger.debug('checking_changes_tables_structure_0')
    json_table_info_0 = {'dt': '%s' % int(time.time()), 'table_name': '', 'fields': {}}
    json_table_info_1, df_result = get_info_table_structure(obj_jdbc, schema_name, table_name)
    if len(df_result) == 0:
        if raise_error_if_changes:
            raise MPMapasErrorThereAreNoRecordsInTable(connection=ds_name, schema=schema_name, table=table_name)

    else:
        for recno, row in df_result.iterrows():
            json_table_info_1['fields'][recno] = {}
            for c in df_result.columns:
                if 'table_name' not in c:
                    str_tmp = "%s" % row[c]
                    if 'nan' in str_tmp.lower():
                        json_table_info_1['fields'][recno][c] = None
                    else:
                        json_table_info_1['fields'][recno][c] = row[c]
    # let's save structure for future verification
    file_name = "%s%s_%s-%s.txt" % (configs.folders.ENTRADA_DIR, ds_name, schema_name, table_name)
    logger.debug("file_name='%s'" % file_name)
    if os.path.isfile(file_name):
        with open(file_name, 'r') as fr:
            json_table_info_0 = eval(fr.read())
        sorted_0 = []
        sorted_1 = []
        for f in json_table_info_0['fields']:
            # let's disregard added fields for ETL process
            if json_table_info_0['fields'][f]['column_name'] not in ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ',
                                                                     'DT_EXTRACAO']:
                # it will convert them to strings for classification
                sorted_0.append("%s" % json_table_info_0['fields'][f])
        for f in json_table_info_1['fields']:
            # let's disregard added fields for ETL process
            if json_table_info_1['fields'][f]['column_name'] not in ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ',
                                                                     'DT_EXTRACAO']:
                # it will convert them to strings for classification
                sorted_1.append("%s" % json_table_info_1['fields'][f])
        sorted_0 = sorted([repr(x) for x in sorted_0])
        sorted_1 = sorted([repr(x) for x in sorted_1])
        # If it changed then we will stop extraction
        if sorted_0 != sorted_1:
            logger.debug("sorted_0 = %s" % sorted_0)
            logger.debug("sorted_1 = %s" % sorted_1)
            if raise_error_if_changes:
                raise MPMapasErrorGettingTableStructure(connection=ds_name, schema=schema_name, table=table_name)
    else:
        with open(file_name, 'a') as fw:  # TODO: when will we update this file? When to delete?
            fw.write("%s\n" % json_table_info_1)  # na primeira vez vamos salvar o que foi lido do banco
        # returning df_result was useful for debugging but is no longer used
        # return json_table_info_1, df_result
    return json_table_info_1


def checking_changes_tables_structure_1(jt_1, jt_2, raise_error_if_changes=True):
    logger.warning('checking_changes_tables_structure_1')
    # Check if all fields in table 1 are in table 2 and have the same type
    list_to_check_fields = ['data_type', 'numeric_precision', 'numeric_scale']
    # TODO: Should we check these other columns?
    # ['column_name', 'data_type', 'ordinal_position', 'column_default', 
    # 'is_nullable', 'numeric_precision', 'numeric_scale']
    list_columns_to_check_1 = []
    list_recno_1 = []
    for f in jt_1['fields']:
        # let's disregard added fields for ETL process
        if jt_1['fields'][f]['column_name'] not in ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO']:
            # it will convert them to strings for classification
            list_columns_to_check_1.append(jt_1['fields'][f]['column_name'])
            list_recno_1.append(f)

    list_columns_to_check_2 = []
    list_recno_2 = []
    for f in jt_2['fields']:
        # let's disregard added fields for ETL process
        if jt_2['fields'][f]['column_name'] not in ['ID', 'CHECKSUMID', 'DT_ULT_ATUALIZ', 'DT_EXTRACAO']:
            # it will convert them to strings for classification
            list_columns_to_check_2.append(jt_2['fields'][f]['column_name'])
            list_recno_2.append(f)

    can_i_proceed = True
    for recno in jt_1['fields']:
        if jt_1['fields'][recno]['column_name'] in list_columns_to_check_2:
            recno2 = list_columns_to_check_2.index(jt_1['fields'][recno]['column_name'])
            if jt_1['fields'][recno]['column_name'] == jt_2['fields'][recno2]['column_name']:
                for properties_to_check in list_to_check_fields:  # checking properties
                    if jt_1['fields'][recno][properties_to_check] == jt_2['fields'][recno2][properties_to_check]:
                        can_i_proceed = True
                    else:
                        can_i_proceed = False
                        # TODO: rever essa mensagem ou adicinar uma MPMapasErrorDefinitionForAttributeIsDifferent
                        logger.error(
                            'Definition for attribute "%s" in column "%s" is not same at source and destination: "%s" != "%s"' % \
                            (properties_to_check, jt_1['fields'][recno]['column_name'],
                             jt_1['fields'][recno][properties_to_check], jt_2['fields'][recno2][properties_to_check]))
                        raise MPMapasErrorCheckingChangesTableStructure(connection=jt_2['ds_name'],
                                                                        schema=jt_2['schema_name'],
                                                                        table=jt_2['table_name'])
        else:
            can_i_proceed = False
            # TODO: rever essa mensagem ou adicinar uma MPMapasErrorColumnDoesNotExistInTable
            logger.warning('Column "%s" does not exist in table "%s" schema "%s" connection "%s"' % \
                           (jt_1['fields'][recno]['column_name'], jt_2['table_name'], jt_2['schema_name'],
                            jt_2['ds_name']))
            raise MPMapasErrorCheckingChangesTableStructure(connection=jt_2['ds_name'], schema=jt_2['schema_name'],
                                                            table=jt_2['table_name'])
    return can_i_proceed


def main():
    try:
        logger.info('Starting painel_compras_check_base_structure para o %s.' % configs.settings.ETL_JOB)
        jdbc_opengeo = configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME]
        schema_opengeo = 'comprasrj'
        table_contrato_opengeo = 'contrato'
        table_item_opengeo = 'item_contrato'
        jdbc_gate = configs.settings.JDBC_PROPERTIES[configs.settings.DB_GATE_DS_NAME]
        schema_gate = 'stage'
        table_contrato_gate = 'ComprasRJ_Contrato'
        table_item_gate = 'ComprasRJ_ItemContrato'

        json_table_info_1 = checking_changes_tables_structure_0(jdbc_gate, jdbc_gate.jndi_name,
                                                                schema_gate, table_contrato_gate)
        json_table_info_2 = checking_changes_tables_structure_0(jdbc_gate, jdbc_gate.jndi_name,
                                                                schema_gate, table_item_gate)

        json_table_info_3 = checking_changes_tables_structure_0(jdbc_opengeo, jdbc_opengeo.jndi_name,
                                                                schema_opengeo, table_contrato_opengeo)
        json_table_info_4 = checking_changes_tables_structure_0(jdbc_opengeo, jdbc_opengeo.jndi_name,
                                                                schema_opengeo, table_item_opengeo)

        checking_changes_tables_structure_1(json_table_info_1, json_table_info_3)
        checking_changes_tables_structure_1(json_table_info_2, json_table_info_4)
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)
    finally:
        logger.info('Finishing painel_compras_check_base_structure para o %s.' % configs.settings.ETL_JOB)


global configs, logger

if __name__ == '__main__' or __name__ == 'painel_compras_check_base_structure':
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
