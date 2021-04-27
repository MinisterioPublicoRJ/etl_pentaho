# -*- coding: utf-8 -*-

import os, time

class logger2x:
    def __init__(self):
        self.dict_log_text = {'fn':'', 'dt':'', 'dl': 'error', 'ti': '', 'msg': ''}
        self.debug_console = True
        self.debug_level = 0
        self.log_levels = ['debug', 'info', 'warning', 'error', 'critical']
        self.folder_log = '.'
        self.prefix_file_name = 'log_'
        self.suffix_file_name = time.strftime("%Y_%m_%d_%H_%M_%S").lower()
        self.file_name_extension = '.txt'
        self.initial_file_name = '%s%s%s' % (self.prefix_file_name, self.suffix_file_name, self.file_name_extension)

    def setting(self, key, text_value):
        self.dict_log_text[key] = text_value

    def logging(self, key, text_value):
        HORA_INICIO = time.strftime("%Y_%m_%d_%H_%M_%S").lower()
        file_name = '%s%s%s' % (self.prefix_file_name, HORA_INICIO, self.file_name_extension)
        self.dict_log_text[key] = text_value                
        if 'fn' not in self.dict_log_text:                      
            self.dict_log_text['fn'] = self.initial_file_name
        else:
            if not self.dict_log_text['fn']:
                self.dict_log_text['fn'] = file_name                # file_name
            else:
                file_name = self.dict_log_text['fn']
        if 'dt' not in self.dict_log_text:
            self.dict_log_text['dt'] = HORA_INICIO                  # datetime
        else:
            if not self.dict_log_text['dt']:
                self.dict_log_text['dt'] = HORA_INICIO
        if 'dl' not in self.dict_log_text:
            self.dict_log_text['dl'] = 'debug'                      # debug_level
        if 'ti' not in self.dict_log_text:
            self.dict_log_text['ti'] = '17'                         # task_id
        if 'msg' not in self.dict_log_text:
            self.dict_log_text['msg'] = 'A verdade está lá fora'    # message log
        # TODO: rever declaração para DEBUG_LEVEL
        if self.dict_log_text['dl'] not in self.log_levels:
            print("debug_level= %s and not in %s" % (self.dict_log_text['dl'], self.log_levels))
            print(self.dict_log_text)
        else:
            if self.debug_level <= self.log_levels.index(self.dict_log_text['dl']):
                if self.debug_console:
                    print(self.dict_log_text)
                # TODO: se não encontrar a pasta vamos usar e pasta corrente
                if os.path.isdir(self.folder_log):
                    with open(self.folder_log + file_name, 'a') as log:
                        log.write("%s\n" % self.dict_log_text)
                else:
                    print("'%s' não foi encontrada, vamos usar a pasta corrente" % self.folder_log)
                    print(self.dict_log_text)
                    with open(file_name, 'a') as log:
                        log.write("%s\n" % self.dict_log_text)
