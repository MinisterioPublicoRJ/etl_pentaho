# -*- coding: utf-8 -*-


class MPMapasException(Exception):
    def __init__(self, **kwargs):
        error_code = kwargs.get('error_code')
        error_msg = kwargs.get('error_msg')
        error_name = kwargs.get('error_name')

        if error_code and error_msg:
            self.msg = "Error_code: %s msg: %s" % (
                error_code, error_msg.format(error_name=error_name if error_name else ''))
        elif self.error_code and self.error_msg:
            self.msg = "Error_code: {error_code} msg: {error_msg}".format(error_code=self.error_code,
                                                                          error_msg=self.error_msg.format(
                                                                              error_name=error_name if error_name else ''))
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


class MPMapasIOException(MPMapasException):
    def __init__(self, **kwargs):
        etl_name = kwargs.get('etl_name')
        error_name = kwargs.get('error_name')
        file_name = kwargs.get('file_name')
        abs_path = kwargs.get('abs_path')
        error_code = kwargs.get('error_code')
        error_msg = kwargs.get('error_msg')
        self.error_code = error_code if error_code else self.error_code
        self.error_msg = (error_msg if error_msg else self.error_msg).format(
            etl_name=etl_name if etl_name else '',
            error_name=error_name if error_name else '',
            file_name=file_name if file_name else '',
            abs_path=abs_path if abs_path else '')

        self.msg = "Error_code: {error_code} msg: {error_msg}".format(error_code=self.error_code,
                                                                      error_msg=self.error_msg)

    def __str__(self):
        return self.msg


class MPMapasSubprocessException(MPMapasException):
    def __init__(self, **kwargs):
        etl_name = kwargs.get('etl_name')
        error_name = kwargs.get('error_name')
        arg_list = kwargs.get('arg_list')
        error_code = kwargs.get('error_code')
        error_msg = kwargs.get('error_msg')
        self.error_code = error_code if error_code else self.error_code
        self.error_msg = (error_msg if error_msg else self.error_msg).format(
            etl_name=etl_name if etl_name else '',
            error_name=error_name if error_name else '',
            arg_list=','.join(list(arg_list)) if arg_list else '')

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


class MPMapasErrorEtlStillRunning(MPMapasException):
    error_code = 11
    error_msg = 'Erro: O etl {error_name} ainda esta sendo executado e não irá iniciar outra vez antes que a execução anterior finalize.'


class MPMapasExecSubProcessException(MPMapasSubprocessException):
    error_code = 12
    error_msg = 'Erro[{error_name}] no ETL[{etl_name}] ao executar chamar o subprocess.Popen com os argumentos[{arg_list}].'


class MPMapasErrorFileNotFound(MPMapasIOException):
    error_code = 13
    error_msg = 'Erro[{error_name}] o ETL[{etl_name}] precisa do arquivo[{file_name}] na pasta[{abs_path}] para continuar.'
