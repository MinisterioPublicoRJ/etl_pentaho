# import sys
# import os
# os.environ["NLS_LANG"] = ".UTF8"
from enum import Enum
# from sqlalchemy import types
import datetime
from psycopg2 import extensions as psyex


class ConvertTypes(Enum):

    def __new__(cls, label, value):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.label = label
        return obj

    DEFAULT     = ('DEFAULT',   {'python_type': [str],             'python': ['str'],          							            'postgresql': ['text'],									                'oracle': ['NVARCHAR2'],  """'sqlalchemy': [types.String],"""     'csv': ['STRING']})
    STRING      = ('STRING',    {'python_type': [str],             'python': ['str'],          							            'postgresql': ['text'],									                'oracle': ['NVARCHAR2'],  """'sqlalchemy': [types.NVARCHAR],"""   'csv': ['STRING']})
    CHAR        = ('CHAR',      {'python_type': [str],             'python': ['str'],          							            'postgresql': ['char'],									                'oracle': ['CHAR'],       """'sqlalchemy': [types.CHAR],"""       'csv': ['CHAR']})
    INTEGER     = ('NUMBER',    {'python_type': [int],             'python': ['int'],          							            'postgresql': ['int8', 'int4', 'int', 'bigint', 'integer'],			    'oracle': ['NUMBER'],     """'sqlalchemy': [types.INTEGER],"""    'csv': ['INTEGER']})
    DECIMAL     = ('DECIMAL',   {'python_type': [float],           'python': ['float'],        							            'postgresql': ['numeric', 'float8', 'float4', 'float'],	                'oracle': ['NUMBER'],     """'sqlalchemy': [types.Numeric],"""    'csv': ['DECIMAL']})
    BOOL        = ('BOOL',      {'python_type': [str],             'python': ['str'],          							            'postgresql': ['bool', 'boolean'],						                'oracle': ['CHAR'],       """'sqlalchemy': [types.CHAR],"""       'csv': ['STRING']})
    DATE        = ('DATE',      {'python_type': [datetime.date],   'python': ['str', 'period[D]', 'period[D]', 'str'],				'postgresql': ['date'],									                'oracle': ['DATE'],       """'sqlalchemy': [types.DATE],"""       'csv': ['DATE']})
    DATETIME    = ('DATETIME',  {'python_type': [datetime],        'python': ['datetime64[ns]', 'datetime64[ns, UTC]', 'str'],    	'postgresql': ['timestamp', 'time', 'timestamp without time zone'],     'oracle': ['TIMESTAMP'],  """'sqlalchemy': [types.TIMESTAMP],"""  'csv': ['TIMESTAMP']})

    def describe(self):
        return self.name, self.value

    @classmethod
    def get_name(cls, value, lang=None):
        result = cls.DEFAULT
        for conv_type in cls:
            if (lang is None and conv_type.value == value) or (lang is not None and conv_type.value[lang] == value):
                return conv_type.name
        return result

    @classmethod
    def get_value(cls, name, lang=None):
        result = cls.DEFAULT
        for conv_type in cls:
            if conv_type.name == name:
                return conv_type.value if lang is None else conv_type.value[lang]
        return result

    @classmethod
    def get_by_label(cls, label):
        result = cls.DEFAULT
        for conv_type in cls:
            if conv_type.label == label:
                return conv_type
        return result

    @classmethod
    def convert_from_to(cls, datatype, lang_from, lang_to):
        result = (datatype, cls.DEFAULT)
        for conv_type in cls:
            if datatype in conv_type.value[lang_from]:
                return conv_type.value[lang_to][0]
        return result
