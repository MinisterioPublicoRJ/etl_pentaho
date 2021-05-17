import getopt
import logging
import os
import shutil
import sys
import urllib.request
import zipfile
from os import walk
from pathlib import Path
from string import digits

import mpmapas_commons as commons
import mpmapas_logger
from mpmapas_exceptions import MPMapasException

os.environ["NLS_LANG"] = ".UTF8"


def download_file(urlstr, file_path, file_nam, mode='wb'):
    logger.info('erasing dir: %s !' % file_path)
    [file.unlink() for file in Path(file_path).glob("*") if file.is_file()]
    logger.info('downloading url: %s !' % urlstr)
    with urllib.request.urlopen(urlstr) as response, open(file=file_path + file_nam, mode=mode) as out_file:
        shutil.copyfileobj(response, out_file)
    logger.info('file saved at %s !' % file_nam)


def unzip_file(file_path, file_name):
    logger.info('unziping file: %s !' % file_name)
    with zipfile.ZipFile(file_path + file_name, 'r') as zip_ref:
        zip_ref.extractall(file_path)
    os.remove(file_path + file_name)
    logger.info('unziped file to %s !' % file_path)


def verify_files(file_path, file_arquivos_necessarios):
    logger.info('Starting verify_files.')
    retorno = True
    df_arquivos_necessarios = commons.read_csv(file_csv=file_arquivos_necessarios)
    for (dirpath, dirnames, filenames) in walk(file_path):
        for filename in filenames:
            file = filename.translate(str.maketrans('', '', digits)).replace('.csv', '')
            if file in df_arquivos_necessarios['short_filename'].values:
                logger.info('rename file: %s to: %s ' % (dirpath + filename, dirpath + file + '.csv'))
                os.rename(dirpath + filename, dirpath + file + '.csv')
            else:
                logger.info('remove file: %s ' % (dirpath + filename))
                os.remove(dirpath + filename)
    for (dirpath, dirnames, filenames) in walk(file_path):
        for arquivo in df_arquivos_necessarios['short_filename'].values:
            if arquivo + '.csv' not in filenames:
                retorno = False
                logger.info('cant find file: %s ' % arquivo)
                break
    logger.info('Finishing verify_files.')
    return retorno


def main(periodo='', nome_arquivo='', url=''):
    try:
        logger.info('Starting %s.' % configs.settings.ETL_JOB)

        if periodo:
            nome_arquivo = configs.settings.CNES_BASE_nome_parcial + periodo + '.ZIP'
            url = configs.settings.FTP_DATASUS + nome_arquivo

        download_file(urlstr=url, file_path=configs.folders.ENTRADA_DIR, file_nam=nome_arquivo, mode='wb')
        unzip_file(file_path=configs.folders.ENTRADA_DIR, file_name=nome_arquivo)
        arqs_necessarios = verify_files(file_path=configs.folders.ENTRADA_DIR,
                                        file_arquivos_necessarios=configs.folders.CONFIG_DIR +
                                                                  configs.settings.FILE_ARQUIVOS_NECESSARIOS)
        if not arqs_necessarios:
            raise MPMapasException(error_code=1,
                                   error_msg='Nem todos os arquivos necessarios estao presentes para o periodo %s.'
                                             % periodo)
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
        periodo = ''
        nome_arquivo = ''
        url = ''

        try:
            opts, args = getopt.getopt(sys.argv[1:], "hnup", ['name=', 'url=', 'periodo='])
        except getopt.GetoptError:
            print('usage: ' + sys.argv[0] + ' [-h] --> for help')
            sys.exit(2)
        if len(opts) < 1:
            print('pass at least one argument.')
            print('usage: ' + sys.argv[0] + ' [-h] --> for help')
            sys.exit()
        for opt, arg in opts:
            if opt == '-h':
                print('usage: ' + sys.argv[0] + ' [-n or --name] --> the name of the file to download')
                print('usage: ' + sys.argv[0] + ' [-u or --url] --> the url to download the file')
                print('usage: ' + sys.argv[0] + ' [-p or --periodo] --> year and month (yyyyMM) to download the file')
                sys.exit()
            elif opt in ['-n', '--name']:
                nome_arquivo = arg
            elif opt in ['-u', '--url']:
                url = arg
            elif opt in ['-p', '--periodo']:
                periodo = arg.strip(' ')

        main(periodo=periodo, nome_arquivo=nome_arquivo, url=url)
    except Exception as excpt:
        logging.exception('Fatal error in __main__')
        exit(excpt)
