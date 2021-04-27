import logging
import os

import pandas as pd
import psycopg2
from mpmapas_exceptions import MPMapasErrorPostgresqlPsycopg2
from psycopg2 import Error
from psycopg2 import extras
from psycopg2 import sql

os.environ["NLS_LANG"] = ".UTF8"
logging = logging.getLogger('mpmapas_db_postgresql')


class PostgresqlDB:
    # Warning Never, never, NEVER use Python string concatenation (+) or string parameters interpolation (%) to pass
    # variables to a SQL query string. Not even at gunpoint.
    # https://www.psycopg.org/docs/usage.html#the-problem-with-the-query-parameters
    def __init__(self, simple_jdbc):
        self.database = simple_jdbc.database
        self.simple_jdbc = simple_jdbc

    def connect(self, client_encoding='utf8'):
        return psycopg2.connect(user=self.simple_jdbc.user,
                                password=self.simple_jdbc.password,
                                host=self.simple_jdbc.hostname,
                                port=self.simple_jdbc.port,
                                database=self.simple_jdbc.database,
                                client_encoding=client_encoding
                                )

    def conn_rollback(self, conn):
        if conn.closed == 0:
            logging.warning('PostgresqlDB calling rollback() for connection with {db} database.'.format(
                db=self.simple_jdbc.database))
            conn.rollback()

    def conn_close(self, conn):
        if conn.closed == 0:
            logging.warning(
                'PostgresqlDB calling close() for connection with {db} database.'.format(db=self.simple_jdbc.database))
            conn.close()

    def execute_select(self, sql, result_mode='all', client_encoding='utf8', list_values=[]):
        connection = self.connect()
        logging.warning('Function execute_select abrindo conexao com o Postgresql.')
        try:
            result = []
            with connection as conn:
                with conn.cursor() as cursor:
                    if list_values:
                        cursor.execute(sql, list_values)
                    else:
                        cursor.execute(sql)
                    if result_mode == 'one':
                        result = cursor.fetchone()
                    elif result_mode == 'all':
                        result = cursor.fetchall()
                    elif result_mode == 'all_commit':
                        result = cursor.fetchall()
                        conn.commit()

            return result
        except (Exception, Error) as error:
            self.conn_rollback(connection)
            logging.exception('Function: execute_select - error: conectando ao Postgresql: %s' % error)
            raise MPMapasErrorPostgresqlPsycopg2(db=self.simple_jdbc.database, met='execute_select')
        finally:
            self.conn_close(connection)
            logging.warning('Function execute_select encerrando conexao com o Postgres.')

    def execute_insert(self, sql, df_values_to_execute=[], client_encoding='utf8'):
        connection = self.connect()
        logging.warning('Function execute_insert abrindo conexao com o Postgresql.')
        try:
            result = ''
            with connection as conn:
                with conn.cursor() as cursor:
                    if isinstance(df_values_to_execute, pd.DataFrame) and not df_values_to_execute.empty:
                        if len(df_values_to_execute.index) > 500:
                            cursor.executemany(sql, df_values_to_execute.to_dict('records'))
                        else:
                            extras.execute_batch(cursor, sql, df_values_to_execute.to_dict('records'), page_size=100)
            return result
        except (Exception, Error) as error:
            self.conn_rollback(connection)
            logging.exception('Function execute_insert erro conectando ao Postgresql: %s' % error)
            raise MPMapasErrorPostgresqlPsycopg2(db=self.simple_jdbc.database, met='execute_insert')
        finally:
            self.conn_close(connection)
            logging.warning('Function execute_insert encerrando conexao com o Postgres.')

    def execute_values_insert(self, sql, template, df_values_to_execute=[], fetch=False, server_encoding=None,
                              client_encoding='utf8'):
        connection = self.connect()
        logging.warning('Function execute_values_insert abrindo conexao com o Postgresql.')
        try:
            result_ids = []
            with connection as conn:
                with conn.cursor() as cursor:
                    if server_encoding:
                        conn.set_client_encoding(server_encoding)
                    result_ids = extras.execute_values(cur=cursor, sql=sql.as_string(cursor),
                                                       argslist=df_values_to_execute.to_dict('records'),
                                                       template=template, page_size=100, fetch=fetch)
            return result_ids
        except (Exception, Error) as error:
            self.conn_rollback(connection)
            logging.exception('Function execute_values_insert erro conectando ao Postgresql: %s' % error)
            raise MPMapasErrorPostgresqlPsycopg2(db=self.simple_jdbc.database, met='execute_values_insert')
        finally:
            self.conn_close(connection)
            logging.warning('Function execute_values_insert encerrando conexao com o Postgres.')

    @staticmethod
    def insert_values_sql(schema_name, table_name, list_flds, unique_field=None, pk_field=None):
        # https://www.psycopg.org/docs/sql.html
        # https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
        # https://www.psycopg.org/docs/extras.html#fast-exec
        pre_sql = """INSERT INTO {} ({}) VALUES %s"""
        if unique_field:
            pre_sql = pre_sql + ' ON CONFLICT ({}) DO NOTHING '

        if pk_field:
            pre_sql = pre_sql + ' RETURNING {} '

        insert_list_sql = sql.SQL(
            pre_sql
        )
        if not unique_field and not pk_field:
            insert_list_sql = insert_list_sql.format(
                sql.Identifier(schema_name, table_name)
                , sql.SQL(", ").join(map(sql.Identifier, list_flds))
            )
        elif unique_field and not pk_field:
            insert_list_sql = insert_list_sql.format(
                sql.Identifier(schema_name, table_name)
                , sql.SQL(", ").join(map(sql.Identifier, list_flds))
                , sql.Identifier(unique_field)
            )
        elif not unique_field and pk_field:
            insert_list_sql = insert_list_sql.format(
                sql.Identifier(schema_name, table_name)
                , sql.SQL(", ").join(map(sql.Identifier, list_flds))
                , sql.Identifier(pk_field)
            )
        elif unique_field and pk_field:
            insert_list_sql = insert_list_sql.format(
                sql.Identifier(schema_name, table_name)
                , sql.SQL(", ").join(map(sql.Identifier, list_flds))
                , sql.Identifier(unique_field)
                , sql.Identifier(pk_field)
            )

        insert_template = sql.SQL(
            """({})"""
        ).format(
            sql.SQL(", ").join(map(sql.Placeholder, list_flds))
        )

        return insert_list_sql, insert_template

    @staticmethod
    def insert_batch_sql(schema_name, table_name, list_flds, unique_field=None, pk_field=None):
        # https://www.psycopg.org/docs/sql.html
        # https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
        pre_sql = """INSERT INTO {} ({}) VALUES ({})"""
        if unique_field:
            pre_sql = pre_sql + ' ON CONFLICT ({}) DO NOTHING '

        if pk_field:
            pre_sql = pre_sql + ' RETURNING {} '

        insert_list_sql = sql.SQL(pre_sql)

        if not unique_field and not pk_field:
            insert_list_sql = insert_list_sql.format(
                sql.Identifier(schema_name, table_name),
                sql.SQL(", ").join(map(sql.Identifier, list_flds)),
                sql.SQL(", ").join(map(sql.Placeholder, list_flds))
            )
        elif unique_field and not pk_field:
            insert_list_sql = insert_list_sql.format(
                sql.Identifier(schema_name, table_name),
                sql.SQL(", ").join(map(sql.Identifier, list_flds)),
                sql.SQL(", ").join(map(sql.Placeholder, list_flds)),
                sql.Identifier(unique_field)
            )

        elif not unique_field and pk_field:
            insert_list_sql = insert_list_sql.format(
                sql.Identifier(schema_name, table_name),
                sql.SQL(", ").join(map(sql.Identifier, list_flds)),
                sql.SQL(", ").join(map(sql.Placeholder, list_flds)),
                sql.Identifier(pk_field)
            )
        elif unique_field and pk_field:
            insert_list_sql = insert_list_sql.format(
                sql.Identifier(schema_name, table_name),
                sql.SQL(", ").join(map(sql.Identifier, list_flds)),
                sql.SQL(", ").join(map(sql.Placeholder, list_flds)),
                sql.Identifier(unique_field),
                sql.Identifier(pk_field)
            )
        return insert_list_sql

    @staticmethod
    def select_sql(schema_name, table_name, list_flds, list_where_clause='', list_oper_clause='', limit=''):
        if list_where_clause != '':
            if limit != '':
                pre_sql = """SELECT {} FROM {} WHERE {}{}{} LIMIT %s""" % limit
                select_list_sql = sql.SQL(
                    pre_sql
                ).format(
                    sql.SQL(", ").join(map(sql.Identifier, list_flds)),
                    sql.Identifier(schema_name, table_name),
                    sql.SQL(" and ").join(map(sql.Identifier, list_where_clause)),
                    sql.SQL(" and ").join(map(sql.Literal, list_oper_clause)),
                    sql.SQL(" and ").join(map(sql.Placeholder, list_where_clause)),
                    sql.SQL(" ").join(map(sql.Placeholder, limit))
                )
            else:
                select_list_sql = sql.SQL(
                    """SELECT {} FROM {} WHERE {}{}{}"""
                ).format(
                    sql.SQL(", ").join(map(sql.Identifier, list_flds)),
                    sql.Identifier(schema_name, table_name),
                    sql.SQL(" and ").join(map(sql.Identifier, list_where_clause)),
                    sql.SQL(" and ").join(map(sql.Literal, list_oper_clause)),
                    sql.SQL(" and ").join(map(sql.Placeholder, list_where_clause))
                )
        else:
            if limit != '':
                pre_sql = """SELECT {} FROM {} LIMIT %s""" % limit
                select_list_sql = sql.SQL(pre_sql).format(
                    sql.SQL(", ").join(map(sql.Identifier, list_flds)),
                    sql.Identifier(schema_name, table_name),
                    sql.Placeholder(limit)
                )
            else:
                select_list_sql = sql.SQL(
                    """SELECT {} FROM {}"""
                ).format(
                    sql.SQL(", ").join(map(sql.Identifier, list_flds)),
                    sql.Identifier(schema_name, table_name)
                )
        return select_list_sql
