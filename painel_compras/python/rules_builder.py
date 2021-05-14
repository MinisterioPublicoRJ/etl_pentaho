import logging
import os

import mpmapas_commons as commons
import mpmapas_logger
from mpmapas_exceptions import MPMapasDataBaseException, MPMapasException


def build_file_rules_classify_items(file_name_input, file_name_output):
    if os.path.isfile(file_name_input):
        logger.info("let's try to build sql commands for item classification rules")
        logger.info("File input: %s" % file_name_input)
        logger.info("File output: %s" % file_name_output)
        with open(file_name_input, mode='r', encoding='utf-8') as fr:
            file_content = fr.read()
        if len(file_content) > 0:
            c1 = '_P3RC3NT_'
            c2 = '_QU0T3_'
            c3 = '_AP0STR0PH3_'
            new_file_content = file_content.replace("'", c3).replace('"', c2).replace('%', c1)
            all_lines = new_file_content.splitlines()
            new_file_content = 'delete from comprasrj.regras_classificacao_itens;\n'
            id_ordem = 100
            value_1 = 'ordem'
            value_2 = 'tp_item'
            value_3 = 'descricao'
            value_4 = 'sql_declaracao'
            for l in all_lines:
                if len(l) > 0 and '--' not in l:
                    value_1 = id_ordem
                    if 'select 1 as tp_item,' in l:
                        value_2 = 1
                        value_3 = 'produto'
                    elif 'select 2 as tp_item,' in l:
                        value_2 = 2
                        value_3 = 'servico'
                    if 'select 3 as tp_item,' in l:
                        value_2 = 3
                        value_3 = 'despesa'
                    value_4 = l.replace(';', '')
                    sql_1 = "insert into comprasrj.regras_classificacao_itens (ordem, tp_item, descricao, sql_declaracao) values (%s, %s, '%s', '%s');"
                    sql_cmd = sql_1 % (value_1, value_2, value_3, value_4)
                    new_file_content = new_file_content + sql_cmd + '\n'
                    id_ordem = id_ordem + 100
            if os.path.isfile(file_name_output):
                logger.info("The output file already exists so it will be overwritten!")
            with open(file_name_output, mode='w', encoding='utf-8') as fw:
                fw.write(new_file_content)
            if os.path.isfile(file_name_output):
                logger.info("Output file updated successfully.")
    else:
        logger.info('there is no %s sql command file with item classification rules' % file_name_input)


def main():
    try:
        logger.info('Starting %s - Main.' % configs.settings.ETL_JOB)
        build_file_rules_classify_items(file_name_input=configs.folders.ENTRADA_DIR + 'sql_2_rules_tested.sql',
                                        file_name_output=configs.folders.SAIDA_DIR + 'sql_2_insert_rules.sql')
        logger.info('Finish %s - Main.' % configs.settings.ETL_JOB)
    except MPMapasDataBaseException as c_err:
        logger.exception(c_err)
        exit(c_err.error_code)
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)


global configs, logger

if __name__ == '__main__':
    try:
        configs = commons.read_config('../etc/settings.yml')
        mpmapas_logger.Logger.config_logger(configs, logghandler_file=True)
        logger = logging.getLogger(configs.settings.ETL_JOB)
        main()
    except Exception as excpt:
        logging.exception('Fatal error in __main__')
        exit(excpt)
