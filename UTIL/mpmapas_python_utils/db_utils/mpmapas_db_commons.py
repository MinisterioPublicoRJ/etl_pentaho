import logging
import os

import mpmapas_commons as commons
import numpy as np
import pandas as pd
from ConvertTypes import ConvertTypes
from db_utils.mpmapas_db_postgresql import PostgresqlDB
from db_utils.mpmapas_db_sqlalchemy import SqlalchemyDB

os.environ["NLS_LANG"] = ".UTF8"
logging = logging.getLogger('mpmapas_db_commons')


def get_db(simple_jdbc, api='sqlalchemy'):
    database = None
    if simple_jdbc.sgbd in ('postgresql'):
        if api and api in ('sqlalchemy'):
            database = SqlalchemyDB(simple_jdbc)
        else:
            database = PostgresqlDB(simple_jdbc)
    return database


def show_server_encoding(configs, jndi_name):
    database = commons.get_database(configs.settings.JDBC_PROPERTIES[jndi_name], api=None)
    sql = "SHOW SERVER_ENCODING;"
    server_encoding = database.execute_select(sql, result_mode='one')
    return server_encoding[0]


def load_table(configs, jndi_name, schema_name, table_name, columns=None, convert_data_types=False, result_mode='all',
               csv_source=None):
    """
        carregar a base <schema_name>.<table_name>
    :param configs: configuracoes com as conexao de banco
    :param jndi_name: nome da conexao no jdbc.properties
    :param schema_name: nome do schema do banco
    :param table_name: nome da tabela no banco
    :param columns:
    :param convert_data_types:
    :param result_mode:
    :param csv_source:
    :return: dataframe da tabela <schema_name>.<table_name>
    """
    database = commons.get_database(configs.settings.JDBC_PROPERTIES[jndi_name], api=None)
    info_sql = "SELECT table_schema, table_name, column_name, data_type, ordinal_position FROM " \
               "information_schema.columns WHERE table_schema = \'" + schema_name + "\' and table_name = \'" + \
               table_name + "\' order by ordinal_position asc; "
    df_fields = pd.DataFrame(database.execute_select(info_sql, result_mode='all'),
                             columns=['table_schema', 'table_name', 'column_name', 'data_type', 'ordinal_position'])
    if not len(df_fields) > 0:
        info_mview = "select ns.nspname as table_schema, cls.relname as table_name,  attr.attname as column_name, " \
                     "trim(leading '_' from tp.typname) as data_type, attr.attnum as ordinal_position from " \
                     "pg_catalog.pg_attribute as attr join pg_catalog.pg_class as cls on cls.oid = attr.attrelid join " \
                     "pg_catalog.pg_namespace as ns on ns.oid = cls.relnamespace join pg_catalog.pg_type as tp on " \
                     "tp.typelem = attr.atttypid where ns.nspname = \'" + schema_name + "\' and cls.relname = \'" + \
                     table_name + "\' and not attr.attisdropped and cast(tp.typanalyze as text) = 'array_typanalyze' " \
                                  "and attr.attnum > 0 order by attr.attnum; "
        df_fields = pd.DataFrame(database.execute_select(info_mview, result_mode='all'),
                                 columns=['table_schema', 'table_name', 'column_name', 'data_type', 'ordinal_position'])
    if (isinstance(columns, list) and len(columns) > 0) or (isinstance(columns, np.ndarray) and columns.size > 0):
        if isinstance(columns, np.ndarray):
            columns = columns.tolist()
        df_fields = df_fields.loc[df_fields['column_name'].isin(columns)].reset_index(drop=True)
    column_types = {'pandas': dict(zip(df_fields['column_name'], df_fields['data_type'].apply(
        lambda datatype: ConvertTypes.convert_from_to(datatype, database.simple_jdbc.sgbd, 'python')))),
                    database.simple_jdbc.sgbd: dict(zip(df_fields['column_name'], df_fields['data_type']))}

    if csv_source:
        df_table = commons.read_csv(file_csv=csv_source)
    else:
        select_sql = database.select_sql(schema_name=df_fields['table_schema'][0],
                                         table_name=df_fields['table_name'][0], list_flds=df_fields['column_name'])
        result_sql = database.execute_select(select_sql, result_mode=result_mode)
        if convert_data_types and len(result_sql) > 0:
            df_table = pd.DataFrame(result_sql, columns=df_fields['column_name']).astype(column_types['pandas'])
            if pd._libs.tslibs.period.Period in df_table.dtypes.tolist():
                for col in df_table.columns:
                    if isinstance(df_table[col][0], pd._libs.tslibs.period.Period):
                        df_table[col] = df_table[col].apply(lambda x: x.to_timestamp())
        else:
            if isinstance(result_sql, tuple):
                result_sql = [list(result_sql)]
            df_table = pd.DataFrame(data=result_sql, columns=df_fields['column_name'])

    dict_table = {'table': df_table, 'column_types': column_types, 'table_info': df_fields}
    return dict_table


def reorder_columns_with_table(df, configs, jndi_name, schema_name, table_name, columns=None):
    dict_df = load_table(configs=configs, jndi_name=jndi_name, schema_name=schema_name, table_name=table_name,
                         result_mode='one')
    for index, row in dict_df['table_info'].iterrows():
        if row['column_name'] in df:
            col_values = df[row['column_name']]
            df = df.drop(columns=row['column_name'])
        else:
            col_values = pd.Series()
        df.insert(loc=row['ordinal_position'] - 1, column=row['column_name'], value=col_values)
    if (isinstance(columns, list) or isinstance(columns, np.ndarray)) and columns.size > 0:
        drop_columns = df.drop(columns=columns).columns.values
        df = df.drop(columns=drop_columns)
    return df


def period_to_timestamp(col):
    # if None in col:
    #     print(col)
    return col.to_timestamp()


def convert_df_types(df_table, dict_col_types):
    # df_table = pd.DataFrame(result_sql, columns=df_fields['column_name']).astype(dict_col_types['pandas'])
    # for col in df_table.columns:
    df_table = df_table.astype(dict_col_types['pandas'])
    # for col in dict_col_types['pandas']:
    #     if col in df_table:
    #         df_table[col] = df_table[col].astype(dict_col_types['pandas'][col])
    if pd._libs.tslibs.period.Period in df_table.dtypes.tolist():
        for col in df_table.columns:
            if isinstance(df_table[col][0], pd._libs.tslibs.period.Period):
                df_table[col] = df_table[col].apply(lambda x: period_to_timestamp(x))
    return df_table
