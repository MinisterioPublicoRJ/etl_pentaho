import logging
import os

import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy import text
from sqlalchemy.pool import NullPool

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
        self.conn = None

    def create_engine(self, echo=True, client_encoding='utf8'):
        dbapi = None
        if self.simple_jdbc.sgbd in 'postgresql':
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
        self.engine = sqlalchemy.create_engine(url=url_string, echo=echo, future=True, client_encoding=client_encoding,
                                               poolclass=NullPool)

    def df_insert(self, df, schema_name, table_name, if_exists='append', index=False, index_label=None, chunksize=None,
                  dtype=None, method=None, echo=True, datestyle=None):
        self.create_engine(echo=echo)
        with self.engine.connect() as conn:
            if dtype and not isinstance(dtype, dict):
                meta = MetaData()
                meta.bind = self.engine
                meta.schema = schema_name
                meta.reflect()
                datatable = meta.tables[schema_name + '.' + table_name]
                dtype = {c.name: c.type for c in datatable.columns}
            if datestyle:
                conn.execute(text("SET DateStyle='{dstyle}'".format(dstyle=datestyle)))
            df.to_sql(table_name, con=self.engine, schema=schema_name, if_exists=if_exists, index=index,
                      index_label=index_label, chunksize=chunksize, dtype=dtype, method=method)
