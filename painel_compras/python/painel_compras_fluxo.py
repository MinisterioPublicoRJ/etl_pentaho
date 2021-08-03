import getopt
import logging
import os
import sys
from datetime import datetime, timezone

import mpmapas_commons as commons
import mpmapas_logger
from mpmapas_exceptions import MPMapasException

import gate_check_base
import itens_classificar
import itens_r_alerta_carga_csv
import painel_compras
import painel_compras_carga
import siga_download_carga

os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)


def update_dt_ult_ver_gate(configs, schema_name, table_name):
    logger.info('Updating %s.%s.DT_ULT_VER_GATE .' % (schema_name, table_name))
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    update_dt_ult_ver_gate_sql = "UPDATE \"" + schema_name + "\".\"" + table_name + "\" SET \"DT_ULT_VER_GATE\"=NOW()"
    db_opengeo.execute_select(update_dt_ult_ver_gate_sql, result_mode=None)


def update_tables_dt_ult_ver_gate(configs):
    logger.info('Update tables DT_ULT_VER_GATE.')
    update_dt_ult_ver_gate(configs=configs, schema_name='comprasrj', table_name='compra')
    update_dt_ult_ver_gate(configs=configs, schema_name='comprasrj', table_name='contrato')
    update_dt_ult_ver_gate(configs=configs, schema_name='comprasrj', table_name='item_contrato')
    update_dt_ult_ver_gate(configs=configs, schema_name='comprasrj', table_name='contratos_agregados')
    update_dt_ult_ver_gate(configs=configs, schema_name='comprasrj', table_name='compras_itens_por_contrato')


def check_atualizacao_bases_siga(truncate_stage):
    logger.info('Starting check_atualizacao_bases_siga')
    return siga_download_carga.main(truncate_stage)


def check_atualizacao_bases_gate(run_painel_compras, diff_check_table, diff_count_table):
    logger.info('Starting check_atualizacao_bases_gate')
    return gate_check_base.main(run_painel_compras, diff_check_table, diff_count_table)


def main(run_painel_compras=True, diff_check_table=False, diff_count_table=False):
    try:
        logger.info('Starting %s.' % configs.settings.ETL_JOB)
        fonte_dados = configs.settings.FONTE_DADOS

        if 'SIGA' in fonte_dados:
            run_painel_compras = check_atualizacao_bases_siga(truncate_stage=diff_check_table)
        elif 'GATE' in fonte_dados:
            run_painel_compras = check_atualizacao_bases_gate(run_painel_compras, diff_check_table, diff_count_table)

        if run_painel_compras:
            # painel_compras_check_base_structure.main()
            painel_compras_carga.main()
            painel_compras.main()
            itens_classificar.main()
            itens_r_alerta_carga_csv.main()
        update_tables_dt_ult_ver_gate(configs)
    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)
    finally:
        logger.info('Finishing %s.' % configs.settings.ETL_JOB)


global configs, logger

if __name__ == '__main__':
    try:
        configs = commons.read_config('../etc/settings.yml')
        mpmapas_logger.Logger.config_logger(configs, logghandler_file=True)
        logger = logging.getLogger(configs.settings.ETL_JOB)

        run_painel_compras = True
        diff_check_table = False
        diff_count_table = False

        try:
            opts, args = getopt.getopt(sys.argv[1:], "hrtc")
        except getopt.GetoptError:
            print('usage: ' + sys.argv[0] + ' [-h] --> for help')
            sys.exit(2)
        if len(opts) > 1:
            print('pass only one argument.')
            print('usage: ' + sys.argv[0] + ' [-h] --> for help')
            sys.exit()
        for opt, arg in opts:
            if opt == '-h':
                print('usage: ' + sys.argv[0] + ' [-r] --> to run painel_compras')
                print('usage: ' + sys.argv[0] + ' [-t] --> to check diffent table data for gate or truncate tables from stage schema for siga')
                print('usage: ' + sys.argv[0] + ' [-c] --> to check diffent row count')
                sys.exit()
            elif opt in ["-r"]:
                run_painel_compras = True
            elif opt in ["-t"]:
                run_painel_compras = False
                diff_check_table = True
            elif opt in ["-c"]:
                run_painel_compras = False
                diff_count_table = True

        main(run_painel_compras=run_painel_compras, diff_check_table=diff_check_table,
             diff_count_table=diff_count_table)
    except Exception as excpt:
        logging.exception('Fatal error in %s' % __name__)
        exit(excpt)
