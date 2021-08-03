import codecs
import csv
import hashlib
import logging
import os
import subprocess
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import cchardet
import chardet
import pandas as pd
import unidecode
import yaml
from db_utils import mpmapas_db_commons
from mpmapas_exceptions import MPMapasErrorEtlStillRunning, MPMapasErrorFileNotFound
from pyjavaproperties import Properties

os.environ["NLS_LANG"] = ".UTF8"
logging = logging.getLogger('mpmapas_commons')


class Settings:
    def __init__(self, configs: dict, jdbcproperties: Properties, settings_env: str = ''):
        if settings_env and settings_env in configs:
            for config in configs[settings_env]:
                exec("self.%s = configs['%s']['%s']" % (config, settings_env, config))
        if 'settings' in configs:
            for config in configs['settings']:
                if 'ETL_CLASSIFIED' in configs['settings'][config] and 'PROP_CLASSIFIED' in configs['settings'][config]:
                    classified_prop: Properties = Properties()
                    classified_prop.load(open(os.path.abspath(os.environ[configs['settings'][config][0]] +
                                                              os.environ[configs['settings'][config][1]])))
                    exec("self.%s = '%s'" % (config, classified_prop[config]))
                else:
                    exec("self.%s = configs['settings']['%s']" % (config, config))
            if ('JDBC_PROPERTIES_FILE' in configs['settings']) or (
                    settings_env and settings_env in configs and 'JDBC_PROPERTIES_FILE' in configs[settings_env]):
                self.JDBC_PROPERTIES: dict = Configs.dict_jdbc(jdbcproperties)


class Folders:
    def __init__(self, configs: dict, folders_env: str = ''):
        if folders_env and folders_env in configs:
            for config in configs[folders_env]:
                exec("self.%s = configs['%s']['%s']" % (config, folders_env, config))
        if 'folders' in configs:
            for config in configs['folders']:
                exec("self.%s = configs['folders']['%s']" % (config, config))
        self.verify_paths()

    def verify_paths(self):
        for attr in vars(self):
            check_dir = vars(self)[attr]
            if not Path(check_dir).is_dir():
                Path(check_dir).mkdir(parents=True, exist_ok=True)


class Configs:
    def __init__(self, configs: dict, jdbcproperties: Properties, etl_env: str = ''):
        self.settings: Settings = Settings(configs, jdbcproperties, 'settings_' + etl_env)  # Settings
        self.folders: Folders = Folders(configs, 'folders_' + etl_env)  # Folders

    @staticmethod
    def dict_jdbc(properties):
        jdbc: dict = {}  # dict
        for name, value in properties.items():
            if name.split('/')[0] not in jdbc:
                jdbc[name.split('/')[0]] = {'jndi_name': name.split('/')[0]}
            jdbc[name.split('/')[0]][name.split('/')[1]] = value
        jdbc_dict: dict = {}  # dict
        for name_jdbc, value_jdbc in jdbc.items():
            simple_jdbc = SimpleJdbc(value_jdbc)
            jdbc_dict[name_jdbc] = simple_jdbc
        return jdbc_dict


class SimpleJdbc:
    def __init__(self, jdbcproperties):
        self.jndi_name: str = jdbcproperties['jndi_name']  # str
        self.type: str = jdbcproperties['type']  # str
        self.driver: str = jdbcproperties['driver']  # str
        self.url: str = jdbcproperties['url']  # str
        self.user: str = jdbcproperties['user']  # str
        self.password: str = jdbcproperties['password']  # str
        result = urlparse(urlparse(jdbcproperties['url']).path)  # str
        self.database: str = result.path[1:]  # str
        self.sgbd: str = result.scheme  # str
        self.hostname: str = result.hostname  # str
        self.port: str = result.port  # str


def etl_status(logger, configs, status='start', msg=''):
    etl_status_file = {'pid': os.getpid(), 'etl': configs.settings.ETL_JOB, 'status': status, 'msg': msg,
                       'date': str(datetime.now(timezone.utc))}
    if 'start' in status:
        try:
            with open(configs.folders.TEMP_DIR + configs.settings.ETL_JOB + '.status', mode='r',
                      encoding='utf-8') as file:
                statusfile = yaml.load(list(file)[-1], Loader=yaml.FullLoader)
                if statusfile['status'] not in ('stop', 'exception', 'finish'):
                    raise MPMapasErrorEtlStillRunning(error_name=configs.settings.ETL_JOB)
        except FileNotFoundError:
            logger.info('Creating status file for %s.' % configs.settings.ETL_JOB)
        logger.info('%s - Starting %s.' % (datetime.now(timezone.utc), configs.settings.ETL_JOB))
    elif 'running' in status:
        logger.info('%s - Running %s: %s.' % (datetime.now(timezone.utc), configs.settings.ETL_JOB, msg))
    elif 'finish' in status:
        logger.info('%s - Finishing %s.' % (datetime.now(timezone.utc), configs.settings.ETL_JOB))
    elif 'stop' in status:
        logger.info('%s - Stopped %s.' % (datetime.now(timezone.utc), configs.settings.ETL_JOB))
    elif 'exception' in status:
        logger.info('%s - Exception at %s: %s.' % (datetime.now(timezone.utc), configs.settings.ETL_JOB, msg))
    with open(configs.folders.TEMP_DIR + configs.settings.ETL_JOB + '.status', mode='w', encoding='utf-8') as file:
        file.write('%s' % etl_status_file)


def read_config(settings_path: str):
    with open(settings_path) as file:
        configs: dict = yaml.load(file, Loader=yaml.FullLoader)  # dict
    jdbcproperties: Properties = Properties()  # Properties
    jdbc_prop: Properties = Properties()  # Properties
    try:
        etl_env: str = os.environ['ETL_ENV']  # str
        if etl_env:
            etl_env: str = etl_env.lower()  # str
            settings_env: str = 'settings_' + etl_env  # str
    except KeyError:
        etl_env: str = ''  # str
        settings_env: str = ''  # str
    except Exception:
        logging.warning('Error in read_config getting environ var')
        raise

    if 'settings' in configs and 'JDBC_PROPERTIES_FILE' in configs['settings']:
        jdbc_prop.load(open(os.path.abspath(os.environ[configs['settings']['JDBC_PROPERTIES_FILE'][0]] +
                                            os.environ[configs['settings']['JDBC_PROPERTIES_FILE'][1]])))
        jdbcproperties.load(open(jdbc_prop['JDBC']))
    elif settings_env and settings_env in configs and 'JDBC_PROPERTIES_FILE' in configs[settings_env]:
        jdbc_prop.load(open(os.path.abspath(os.environ[configs[settings_env]['JDBC_PROPERTIES_FILE'][0]] +
                                            os.environ[configs[settings_env]['JDBC_PROPERTIES_FILE'][1]])))
        jdbcproperties.load(open(jdbc_prop['JDBC']))
    return Configs(configs, jdbcproperties, etl_env)


# TODO: pesquisar onde está em uso e colocar para usar apenas esses aqui
def normalize_table_name(text):
    if text and type(text) == str:
        text = str.lower(text)
        text = normalize_text(text)
    return text


# TODO: pesquisar onde está em uso e colocar para usar apenas esses aqui
def normalize_column_name(text):
    if text and type(text) == str:
        text = str.lower(text)
        text = normalize_text(text)
        text = text.replace(' ', '_')
    return text


# TODO: pesquisar onde está em uso e colocar para usar apenas esses aqui
def normalize_text(text):
    if text and type(text) == str:
        remove_chars = '.,;:!?@#$%&*/\\<>(){}[]~^´`¨-+°ºª¹²³£¢¬\'\"'
        text = text.translate(str.maketrans('', '', remove_chars)).replace('.csv', '')
        text = unicodedata.normalize(u'NFKD', text).encode('ascii', 'ignore').decode('utf8')
        text = ''.join(ch for ch in unicodedata.normalize('NFKD', text) if not unicodedata.combining(ch))
        text = unidecode.unidecode(text.strip().strip(remove_chars).strip())
    return text


def normalize_str(text):
    if text and type(text) == str:
        text = str.upper(text)
        text = unicodedata.normalize(u'NFKD', text).encode('ascii', 'ignore').decode('utf8')
        text = ''.join(ch for ch in unicodedata.normalize('NFKD', text) if not unicodedata.combining(ch))
        text = unidecode.unidecode(text.strip().strip('.,;:!?@#$%&*/\\<>(){}[]~^´`¨-_+°ºª¹²³£¢¬\'\"').strip())
        text = str.upper(text)
    return text


# TODO: pesquisar onde está em uso e colocar para usar apenas esses aqui
def unaccent_df(df, col):
    # return df.apply(lambda x: translate_str(normalize_str(x[col])), axis=1)
    # return df.apply(lambda x: normalize_str(x[col]), axis=1)
    return df.apply(lambda x: normalize_text(x[col]), axis=1)


def sanitize_encoding(enc):
    try:
        codecs.lookup(enc)
        return enc
    except LookupError:
        try:
            enc = enc.replace('-', '')
            codecs.lookup(enc)
            return enc
        except LookupError:
            # Not a thing, either way
            return None
    except TypeError:
        return None


def detect_encoding(file_path):
    confidence_minimum = 0.7
    encoding, confidence = detect_encoding_confidently(file_path)
    if encoding == None or confidence < confidence_minimum:
        encoding, confidence = detect_simple_encoding(file_path)
        if encoding == None or confidence < confidence_minimum:
            encoding, confidence = detect_universal_encoding(file_path)
            if encoding == None or confidence < confidence_minimum:
                with open(file_path, 'rb') as f:
                    test_str = b''
                    count = 0
                    line = f.readline()
                    while line and count < 250:  # Set based on lines you'd want to check
                        test_str = test_str + line
                        count = count + 1
                        line = f.readline()
                    if isASCII(test_str):
                        encoding = 'ASCII'
                    elif isUTF8(test_str):
                        encoding = 'UTF-8'
                    elif isUTF8Strict(test_str):
                        encoding = 'UTF-8'
                    else:
                        encoding = check_bom(test_str)
                        if not encoding:
                            encoding = None
    return encoding


def detect_simple_encoding(file_path):
    with open(file_path, 'rb') as f:
        test_str = b''
        count = 0
        line = f.readline()
        while line and count < 250:  # Set based on lines you'd want to check
            test_str = test_str + line
            count = count + 1
            line = f.readline()
        result = chardet.detect(test_str)
        encoding = result['encoding']
        confidence = result["confidence"]
    return encoding, confidence


def detect_multi_encoding(file_path):
    multi_encoding = []
    with open(file_path, 'rb') as f:
        test_str = b''
        count = 0
        line = f.readline()
        while line and count < 250:  # Set based on lines you'd want to check
            test_str = test_str + line
            count = count + 1
            line = f.readline()
            multi_result = chardet.detect(line)
            multi_encoding.append(multi_result['encoding'])
        result = chardet.detect(test_str)
        file_encoding = result['encoding']
    return file_encoding, multi_encoding


def detect_universal_encoding(file_path):
    with open(file_path, 'rb') as file:
        detector = chardet.universaldetector.UniversalDetector()
        for line in file.readlines():
            detector.feed(line)
            if detector.done: break
        detector.close()
        file.close()
        encoding = detector.result['encoding']
        confidence = detector.result["confidence"]
    return encoding, confidence


def isASCII(data):
    try:
        data.decode('ASCII')
    except UnicodeDecodeError:
        return False
    else:
        return True


def isUTF8(data):
    try:
        data.decode('UTF-8')
    except UnicodeDecodeError:
        return False
    else:
        return True


def isUTF8Strict(data):
    try:
        decoded = data.decode('UTF-8')
    except UnicodeDecodeError:
        return False
    else:
        for ch in decoded:
            if 0xD800 <= ord(ch) <= 0xDFFF:
                return False
        return True


def check_bom(data):
    BOMS = (
        (codecs.BOM_UTF8, "UTF-8"),
        (codecs.BOM_UTF32_BE, "UTF-32-BE"),
        (codecs.BOM_UTF32_LE, "UTF-32-LE"),
        (codecs.BOM_UTF16_BE, "UTF-16-BE"),
        (codecs.BOM_UTF16_LE, "UTF-16-LE"),
    )
    return [encoding for bom, encoding in BOMS if data.startswith(bom)]


def detect_encoding_confidently(filename):
    """Detect encoding and return decoded text, encoding, and confidence level."""
    filepath = Path(filename)
    # We must read as binary (bytes) because we don't yet know encoding
    blob = filepath.read_bytes()
    detection = cchardet.detect(blob)
    encoding = detection["encoding"]
    confidence = detection["confidence"]
    return encoding, confidence


def detect_delimiter(file_path, encoding):
    with open(file=file_path, mode='r', encoding=encoding, errors="ignore") as f:
        test_str = ''
        count = 0
        line = f.readline()
        while line and count < 250:  # Set based on lines you'd want to check
            test_str = test_str + line
            count = count + 1
            line = f.readline()
        try:
            delimiter = csv.Sniffer().sniff(test_str).delimiter
        except:
            delimiter = ';'
    return delimiter


def detect_file_type(file_name):
    # import magic
    file_type = ''  # magic.from_file(file_name)
    # file_type = magic.detect_from_filename(file_name)
    return file_type


def get_database(simple_jdbc, api='sqlalchemy'):
    return mpmapas_db_commons.get_db(simple_jdbc, api)


def row_checksum(row):
    text = row.astype(str).values.sum()
    checksum = hashlib.md5(text.encode('utf-8')).hexdigest()
    return checksum


def generate_checksum(df):
    return df.apply(lambda x: row_checksum(x), axis=1)


def read_csv(file_csv, header=0, na_values='-', decimal='.', dayfirst=True, encoding=None, delimiter=None):
    if not encoding:
        encoding = detect_encoding(file_csv)
    if not delimiter:
        delimiter = detect_delimiter(file_csv, encoding)
    df = pd.read_csv(filepath_or_buffer=file_csv, header=header, delimiter=delimiter, encoding=encoding,
                     na_values=na_values, keep_default_na=True, dayfirst=dayfirst, decimal=decimal)
    return df


def gravar_saida(df, file_csv, sep=';', decimal='.', na_rep='', quoting=csv.QUOTE_ALL, header=True, index=False,
                 quotechar='"', encoding='utf-8', delete_file_if_exist=True):
    if delete_file_if_exist and os.path.isfile(file_csv):
        os.remove(file_csv)
    df.to_csv(path_or_buf=file_csv, sep=sep, decimal=decimal, na_rep=na_rep, quoting=quoting, quotechar=quotechar,
              encoding=encoding, header=header, index=index)


def execute_r_script(logger, configs, file_script_r, folder_name, list_param=[]):
    complete_name_file_script_r = os.path.abspath(folder_name) + os.sep + file_script_r
    if os.path.isfile(complete_name_file_script_r):
        logger.info('Starting run script R  [%s].' % complete_name_file_script_r)
        # TODO: pesquisar como passar parâmetro para script R usando list_param
        subprocess.call(['R', 'CMD', 'BATCH', complete_name_file_script_r])
        logger.info('Finish script R run...')
    else:
        raise MPMapasErrorFileNotFound(etl_name=configs.settings.ETL_JOB, error_name='Script R file not found',
                                       abs_path=folder_name, file_name=file_script_r)
