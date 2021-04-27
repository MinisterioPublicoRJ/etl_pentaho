# -*- coding: utf-8 -*-


class MPMapasException(Exception):
    def __init__(self, **kwargs):
        error_code = kwargs.get('error_code')
        error_msg = kwargs.get('error_msg')
        error_name = kwargs.get('error_name')

        if error_code and error_msg:
            self.msg = "Error_code: %s msg: %s" % (
                error_code, error_msg.fomrat(error_name=error_name if error_name else ''))
        else:
            self.msg = 'Fatal Error'

    def __str__(self):
        return self.msg


class MPMapasDataBaseException(MPMapasException):
    def __init__(self, **kwargs):
        connection = kwargs.get('connection')
        schema = kwargs.get('schema')
        table = kwargs.get('table')
        name = kwargs.get('name')
        db = kwargs.get('db')
        met = kwargs.get('met')
        error_code = kwargs.get('error_code')
        error_msg = kwargs.get('error_msg')
        self.error_code = error_code if error_code else self.error_code
        self.error_msg = (error_msg if error_msg else self.error_msg).format(
            connection=connection if connection else '',
            schema=schema if schema else '',
            table=table if table else '',
            name=name if name else '',
            db=db if db else '',
            met=met if met else '')

        self.msg = "Error_code: {error_code} msg: {error_msg}".format(error_code=self.error_code,
                                                                      error_msg=self.error_msg)

    def __str__(self):
        return self.msg


class MPMapasErrorCantBeNull(MPMapasDataBaseException):
    error_code = 1
    error_msg = 'A variavel %s nao pode ser nula.'


class MPMapasErrorAccessingTable(MPMapasDataBaseException):
    error_code = 1
    error_msg = 'Erro ao acessar conexão [{connection}], esquema [{schema}], tabela [{table}]'


class MPMapasErrorGettingTableStructure(MPMapasDataBaseException):
    error_code = 2
    error_msg = 'Erro ao pesquisar a estrutura da tabela [{table}], esquema [{schema}], conexão [{connection}]'


class MPMapasErrorCheckingChangesTableStructure(MPMapasDataBaseException):
    error_code = 3
    error_msg = 'Erro ao conferir se há modificações na tabela [{table}], esquema [{schema}], conexão [{connection}]'


class MPMapasErrorThereAreTableStructureChanges(MPMapasDataBaseException):
    error_code = 4
    error_msg = 'Erro há modificações na estrutura da tabela [{table}], esquema [{schema}], conexão [{connection}]'


class MPMapasErrorNoTableInDatabase(MPMapasDataBaseException):
    error_code = 5
    error_msg = 'Aviso NÃO foi encontrada a estrutura da tabela [{table}], esquema [{schema}], conexão [{connection}]'


class MPMapasErrorCheckingFieldsTable(MPMapasDataBaseException):
    error_code = 6
    error_msg = 'Erro ao conferir se campos da tabela [{table}] estão na tabela [{table}]'


class MPMapasErrorThereAreDiffTable(MPMapasDataBaseException):
    error_code = 7
    error_msg = 'Erro há diferenças na estrutura da tabela [{table}] e a tabela [{table}]'


class MPMapasErrorThereAreNoRecordsInTable(MPMapasDataBaseException):
    error_code = 8
    error_msg = 'Erro NÃO HÁ registros na tabela [{table}], esquema [{schema}], conexão [{connection}]'


class MPMapasErrorTimeoutConnServer(MPMapasDataBaseException):
    error_code = 9
    error_msg = 'Erro tempo esgotado na tentativa de conexão tabela [{table}], esquema [{schema}], conexão [{connection}]'


class MPMapasErrorPostgresqlPsycopg2(MPMapasDataBaseException):
    error_code = 10
    error_msg = 'Erro de acesso ao banco postgresql {db} com o psycopg2 no metodo {met}.'
