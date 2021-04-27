import csv
import io
import logging
import os

import pandas as pd
import sqlalchemy

os.environ["NLS_LANG"] = ".UTF8"
logging = logging.getLogger('mpmapas_db_sqlalchemy')


class SqlalchemyDB:
    # https://www.sqlalchemy.org/
    # https://docs.sqlalchemy.org/en/14/tutorial/index.html
    # https://docs.sqlalchemy.org/en/14/tutorial/engine.html
    # https://docs.sqlalchemy.org/en/14/glossary.html#term-DBAPI
    # https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.session.Session.add
    def __init__(self, simple_jdbc):
        self.database = simple_jdbc.database
        self.simple_jdbc = simple_jdbc
        self.engine = None

    def create_engine(self, client_encoding='utf8'):
        dbapi = None
        if self.simple_jdbc.sgbd in ('postgresql'):
            dbapi = 'psycopg2'
        url_string = "{}+{}://{}:{}@{}:{}/{}".format(
            self.simple_jdbc.sgbd,
            dbapi,
            self.simple_jdbc.user,
            self.simple_jdbc.password,
            self.simple_jdbc.hostname,
            self.simple_jdbc.port,
            self.simple_jdbc.database
        )
        self.engine = sqlalchemy.create_engine(url=url_string, echo=True, future=True, client_encoding=client_encoding)

    def execute_bulk_insert_df(self, df, schema_name, table_name, encoding='‘utf-8', quoting=csv.QUOTE_ALL,
                               quotechar='"'):
        self.create_engine()
        retorno = ''  # TODO: Gabriel, variable is unused!

        raw_conn = self.engine.raw_connection()
        cursor = raw_conn.cursor()
        output = io.StringIO()
        # for columns with uppercase name

        df_columns = pd.Series(df.columns.values.tolist()).apply(lambda x: '"' + x + '"').tolist()
        df.to_csv(output, sep=';', header=False, index=False, encoding=encoding)
        # jump to start of stream
        output.seek(0)
        contents = output.getvalue()
        # null values become ''
        cursor.copy_from(file=output, table=schema_name + '.' + table_name, sep=';', null="", size=8192,
                         columns=df_columns)
        raw_conn.commit()
        cursor.close()
        return retorno  # TODO: Gabriel, variable is unused!
