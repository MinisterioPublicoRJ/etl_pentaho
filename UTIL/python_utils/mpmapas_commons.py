import codecs
import csv
import hashlib
import os
from pathlib import Path
from urllib.parse import urlparse

import cchardet
import chardet
import pandas as pd
import yaml
from pyjavaproperties import Properties

from db_utils import mpmapas_db_commons

os.environ["NLS_LANG"] = ".UTF8"


class Settings:
    def __init__(self, configs, jdbcproperties, settings_env=''):
        if settings_env and settings_env in configs:
            for config in configs[settings_env]:
                exec("self.%s = configs['%s']['%s']" % (config, settings_env, config))
        if 'settings' in configs:
            for config in configs['settings']:
                exec("self.%s = configs['settings']['%s']" % (config, config))
            if ('JDBC_PROPERTIES_FILE' in configs['settings']) or (settings_env and settings_env in configs and 'JDBC_PROPERTIES_FILE' in configs[settings_env]):
                self.JDBC_PROPERTIES = Configs.dict_jdbc(jdbcproperties)


class Folders:
    def __init__(self, configs, folders_env=''):
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
            if Path(check_dir).is_dir():
                Path(check_dir).mkdir(parents=True, exist_ok=True)


class Configs:
    def __init__(self, configs, jdbcproperties, etl_env=''):
        self.settings = Settings(configs, jdbcproperties, 'settings_'+etl_env)
        self.folders = Folders(configs, 'folders_'+etl_env)

    @staticmethod
    def dict_jdbc(properties):
        jdbc = {}
        for name, value in properties.items():
            if name.split('/')[0] not in jdbc:
                jdbc[name.split('/')[0]] = {'jndi_name': name.split('/')[0]}
            jdbc[name.split('/')[0]][name.split('/')[1]] = value
        jdbc_dict = {}
        for name_jdbc, value_jdbc in jdbc.items():
            simple_jdbc = SimpleJdbc(value_jdbc)
            jdbc_dict[name_jdbc] = simple_jdbc
        return jdbc_dict


class SimpleJdbc:
    def __init__(self, jdbcproperties):
        self.jndi_name = jdbcproperties['jndi_name']
        self.type = jdbcproperties['type']
        self.driver = jdbcproperties['driver']
        self.url = jdbcproperties['url']
        self.user = jdbcproperties['user']
        self.password = jdbcproperties['password']
        result = urlparse(urlparse(jdbcproperties['url']).path)
        self.database = result.path[1:]
        self.sgbd = result.scheme
        self.hostname = result.hostname
        self.port = result.port
        

def read_config(settings_path):
    with open(settings_path) as file:
        configs = yaml.load(file, Loader=yaml.FullLoader)
    jdbcproperties = Properties()
    try:
        etl_env = os.environ['ETL_ENV']
        if etl_env:
            etl_env = etl_env.lower()
            settings_env = 'settings_'+etl_env
    except:
        etl_env = ''
        settings_env = ''
    if 'settings' in configs and 'JDBC_PROPERTIES_FILE' in configs['settings']:
        jdbcproperties.load(open(configs['settings']['JDBC_PROPERTIES_FILE']))
    elif settings_env and settings_env in configs and 'JDBC_PROPERTIES_FILE' in configs[settings_env]:
        jdbcproperties.load(open(configs[settings_env]['JDBC_PROPERTIES_FILE']))
    return Configs(configs, jdbcproperties, etl_env)


def sanitize_encoding(enc):
    try:
        codecs.lookup(enc)
        return enc
    except LookupError:
        try:
            enc = enc.replace('-','')
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
    print('detect_encoding_confidently:  ')
    print('encoding:  ')
    print(encoding)
    print('confidence:  ')
    print(confidence)
    if encoding == None or confidence < confidence_minimum:
        encoding, confidence = detect_simple_encoding(file_path)
        print('detect_simple_encoding:  ')
        print('encoding:  ')
        print(encoding)
        print('confidence:  ')
        print(confidence)
        if encoding == None or confidence < confidence_minimum:
            encoding, confidence = detect_universal_encoding(file_path)
            print('detect_universal_encoding:  ')
            print('encoding:  ')
            print(encoding)
            print('confidence:  ')
            print(confidence)
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
                        print('isASCII: ')
                        print(encoding)
                    elif isUTF8(test_str):
                        encoding = 'UTF-8'
                        print('isUTF8: ')
                        print(encoding)
                    elif isUTF8Strict(test_str):
                        encoding = 'UTF-8'
                        print('isUTF8Strict: ')
                        print(encoding)
                    else:
                        encoding = check_bom(test_str)
                        print('check_bom: ')
                        print(encoding)
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
        delimiter = csv.Sniffer().sniff(test_str).delimiter
    return delimiter


def detect_file_type(file_name):
    # import magic
    file_type = '' #magic.from_file(file_name)
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


def read_csv(file_csv, header=0, na_values='-', decimal='.', dayfirst=True):
    file_encoding = detect_encoding(file_csv)
    delimiter = detect_delimiter(file_csv, file_encoding)
    df = pd.read_csv(filepath_or_buffer=file_csv, header=header, delimiter=delimiter, encoding=file_encoding, na_values=na_values, keep_default_na=True, dayfirst=dayfirst, decimal=decimal)
    return df


def gravar_saida(df, file_csv, sep=';', decimal='.', na_rep='', quoting=csv.QUOTE_ALL, quotechar='"', encoding='utf-8'):
    df.to_csv(path_or_buf=file_csv, sep=sep, decimal=decimal, na_rep=na_rep, quoting=quoting, quotechar=quotechar, encoding=encoding)
