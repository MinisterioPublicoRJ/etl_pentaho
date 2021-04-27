# -*- coding: utf-8 -*-

import io, os, time, shutil, chardet, pathlib, datetime, subprocess
from urllib.parse import urlparse
import pandas as pd

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import anp_cartel_funcs as cartel

# Projeto - Painel Consumidor - Alertas dos Municípios onde há indício conluio de preços de revenda de combustíveis em postos
# Esse script vai baixar, conferir, acertar e atualizar o banco de dados...

# http://www.anp.gov.br/images/dadosabertos/precos/metadados-levantamento-precos.pdf
# https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/Metodologia_adotada_pela_Agencia_Nacional_do_Petroleo_Gas_Natural_e_Biocombustiveis_para_deteccao_de_carteis.pdf

# wget https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis --no-check-certificate
# wget https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/serie-historica-do-levantamento-de-precos --no-check-certificate

# https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis

# https://stackoverflow.com/questions/24153519/how-to-read-html-from-a-url-in-python-3


# Memento mori
def main(task_id=0):
    config = cartel.load_configs()
    anp = cartel.AnpFile(config=config, link='https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis')
    while True:
        if task_id == 0:            # TODO: precisamos conferir utilitários e biblotecas
            next_task_id = 10
            #TODO: podemos instalar no servidor do PENTAHO?  pip install requests for installation
            try:
                import requests
            except Exception:
                cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg": "pip install requests for installation"' % (task_id))
                next_task_id = 17
            # TODO: precisamos conferir se temos o wget instalado por aqui dependendo do SO
        elif task_id == 10:         # efetuar requisição para a url
            next_task_id = 100
            try:
                anp.request_response = requests.get(anp.link_url_1, verify=False, timeout=10)
                anp.lines_text = anp.request_response.text.split('\n')      # TODO: Se não me falha a memória IIS usa
                # '\r', Apache usa '\n' conferir depois atenção ao terminador de paragráfo
                anp.number_of_lines = len(anp.lines_text)
                number_of_bytes = len(anp.request_response.text)
                if 56335 > number_of_bytes:        # 56335 era o tamanho do header da página em 2021-03-01
                    cartel.print_and_log('"debug": "%s", "Suspeito 56335 > len(anp.request_response.text)": "%s"' % (task_id, number_of_bytes))
            except Exception:
                next_task_id = 150
        elif task_id == 17:     # as coisas não acabaram bem, vamos decidir depois se precisamos registrar mais sobre
            break
        elif task_id == 100:    # construir a lista de arquivos de interesse da url
            next_task_id = 200
            if anp.number_of_lines > 0:
                try:            # Atenção aos produtos desejados "ETANOL", "GASOLINA"
                    for anp.line_content in anp.lines_text:
                        line_content_lower = anp.line_content.lower()
                        if "href" in line_content_lower and ".csv" in line_content_lower and ("_ca.csv" in line_content_lower or "gasolina" in line_content_lower or "etanol" in line_content_lower):
                            if '= "' in anp.line_content:
                                anp.line_content = anp.line_content.replace('= "', '="')
                            if 'href =' in anp.line_content:
                                anp.line_content = anp.line_content.replace('href =', 'href=')
                            i1 = anp.line_content.find('href="') + len('href="')
                            anp.line_content = anp.line_content[i1:]
                            anp.url_link_file_to_check = anp.line_content[:anp.line_content.find('"')]
                            if anp.url_link_file_to_check not in anp.list_1_url_links_to_check:
                                anp.list_1_url_links_to_check.append(anp.url_link_file_to_check)
                    anp.number_1_files_to_check = len(anp.list_1_url_links_to_check)
                    anp.url_link_file_to_check  = ''
                    anp.file_to_check       = ''
                    anp.index_1             = -1
                    if anp.number_1_files_to_check == 0:      # não temos mais nenhum arquivo a verificar
                        next_task_id = 152
                except Exception:
                    next_task_id = 151
            else:
                next_task_id = 153
            cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "len(anp.lines_text)": "%s"' % (task_id, anp.number_of_lines))
        elif task_id == 150:
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Não foi possível obter a resposta do request"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "len(anp.lines_text)": "%s"' % (task_id, anp.number_of_lines))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.link_url_1": "%s"' % (task_id, anp.link_url_1))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg": "Teste se a URL anp.link_url_1 está acessivel no navegador Internet"' % task_id)
            if anp.task_attempt_number < anp.max_number_retry_task:
                anp.task_attempt_number = anp.task_attempt_number + 1
                next_task_id = 10
                cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.task_attempt_number": "%s"' % (task_id, anp.task_attempt_number))
                cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.interval_between_attempts": "%s"' % (task_id, anp.interval_between_attempts))
                time.sleep(anp.interval_between_attempts)
                anp.interval_between_attempts = anp.interval_between_attempts + (anp.interval_between_attempts * anp.task_attempt_number)
        elif task_id == 151:
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Não foi possível encontrar links href na resposta do request"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.link_url_1": "%s"' % (task_id, anp.link_url_1))
        elif task_id == 152:
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Não há links href na resposta do request"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.link_url_1": "%s"' % (task_id, anp.link_url_1))
        elif task_id == 153:
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Não há linhas após o tratamento do request"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.link_url_1": "%s"' % (task_id, anp.link_url_1))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.number_of_lines": "%s"' % (task_id, anp.number_of_lines))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg": "Teste se a URL anp.link_url_1 está acessivel no navegador Internet"' % task_id)
        elif task_id == 200:     # obter o índice do arquivo na lista vamos verificar a lista de arquivos a transferir um a um
            next_task_id = 1000
            if (anp.index_1 + 1) < anp.number_1_files_to_check:   # esse é o crítério para saber se verificamos todos
                next_task_id = 210
                anp.index_1 = anp.index_1 + 1
                try:
                    anp.url_link_file_to_check = anp.list_1_url_links_to_check[anp.index_1]
                except Exception:
                    next_task_id = 250
        elif task_id == 210:     # vamos obter o nome do arquivo e conferir a data da última transmissão
            next_task_id = 220
            try:
                # TODO: !!! verificar possibilidade de substituir por metodo generico
                cartel.check_file_name_lastdate(anp)
                # a = urlparse(anp.url_link_file_to_check)
                # anp.file_to_check = os.path.basename(a.path)
                # if anp.file_to_check in os.listdir(anp.fold_transferred):
                #     anp.file_fold_verify = pathlib.Path(anp.fold_transferred + anp.file_to_check)
                #     anp.mtime_1 = datetime.datetime.fromtimestamp(anp.file_fold_verify.stat().st_mtime)
            except Exception:
                next_task_id = 251
        elif task_id == 220:        # verificar se o arquivo precisa ser transferido novamente, vamos conferir se o arquivo já está em anp.fold_transferred
            next_task_id = 221
            if anp.file_to_check in os.listdir('./'):
                os.remove(anp.file_to_check)
                cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "msg": "%s"' % (task_id, "Tem um arquivo anp.file_to_check aqui... talvez de alguma operação não concluída anterioremente"))
                cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "anp.file_to_check": "%s"' % (task_id, anp.file_to_check))
                cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "msg": "%s"' % (task_id, "Arquivo foi excluído e será transferido novamente."))
                anp.mtime_0 = 0
            if not anp.file_to_check in os.listdir('./'):
                # TODO: !!! verificar alternativa para o wget
                result = cartel.download_file_urllib(anp, mode='wb')
                use_wget = False
                #result = subprocess.call(["wget", "--no-check-certificate", anp.url_link_file_to_check])
                cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "anp.url_link_file_to_check": "%s"' % (task_id, anp.url_link_file_to_check))
                cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "result": "%s"' % (task_id, result))
                if use_wget and result > 0:     # algo deu errado
                    next_task_id = 252
                else:
                    if anp.file_to_check in os.listdir('./') and anp.file_to_check in os.listdir(anp.fold_transferred):
                        anp.file_fold_tranferred = pathlib.Path(anp.file_to_check)
                        anp.mtime_0 = datetime.datetime.fromtimestamp(anp.file_fold_tranferred.stat().st_mtime)
                        cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "msg": "%s"' % (task_id, "Arquivo anp.file_to_check transferido com sucesso."))
                        if anp.mtime_0 == anp.mtime_1:
                            cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "anp.file_to_check": "%s"' % (task_id, anp.file_to_check))
                            cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "datetime": "%s"' % (task_id, anp.mtime_0))
                            cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "msg": "%s"' % (task_id, "Arquivo anp.file_to_check já transferido antes."))
                            os.remove(anp.file_to_check)
                            cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "msg": "%s"' % (task_id, "Arquivo foi excluído e vamos ao próximo da lista."))
                            anp.mtime_0 = 0
                            next_task_id = 200  # vamos voltar para verificar o próximo arquivo da lista
        elif task_id == 221:        # vamos mover o arquivo para anp.fold_transferred
            next_task_id = 222
            # TODO: !!! verificar possibilidade de substituir por metodo generico
            if os.path.isfile(anp.file_to_check):
                try:
                    shutil.move(anp.file_to_check, anp.fold_transferred + anp.file_to_check)
                    cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "anp.file_to_check": "%s"' % (task_id, anp.file_to_check))
                    cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "anp.fold_transferred": "%s"' % (task_id, anp.fold_transferred))
                    cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "msg": "%s"' % (task_id, "Arquivo anp.file_to_check movido para anp.fold_transferred"))
                except Exception:
                    next_task_id = 253
        elif task_id == 222:        # vamos copiar o arquivo de anp.fold_transferred para anp.fold_verify
            next_task_id = 300
            # TODO: !!! verificar possibilidade de substituir por metodo generico
            if os.path.isfile(anp.fold_transferred + anp.file_to_check):
                try:
                    shutil.copy(anp.fold_transferred + anp.file_to_check, anp.fold_verify + anp.file_to_check)
                    cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "anp.file_to_check": "%s"' % (task_id, anp.file_to_check))
                    cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "anp.fold_verify": "%s"' % (task_id, anp.fold_verify))
                    cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "msg": "%s"' % (task_id, "Arquivo anp.file_to_check copiado para anp.fold_verify"))
                except Exception:
                    next_task_id = 254
        elif task_id == 250:    # verificar se o arquivo precisa ser transferido novamente, definir parâmetro para 30 dias para transferir de novo
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.index_1":"%s", "msg":"anp.index_1 inválido em anp.list_1_url_links_to_check"' % (task_id, anp.index_1))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.number_1_files_to_check":"%s", "msg":"anp.number_1_files_to_check deveria ter o mesmo tamanho anp.list_1_url_links_to_check"' % (task_id, anp.number_1_files_to_check))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "len(anp.list_1_url_links_to_check)":"%s", "msg":"anp.index_1 inválido em anp.list_1_url_links_to_check"' % (task_id, len(anp.list_1_url_links_to_check)))
        elif task_id == 251:
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Falha em obter a data de criação do arquivo em anp.fold_transferred"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.fold_transferred":"%s"' % (task_id, anp.fold_transferred))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.file_fold_verify":"%s"' % (task_id, anp.file_fold_verify))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.file_to_check":"%s"' % (task_id, anp.file_to_check))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.url_link_file_to_check":"%s"' % (task_id, anp.url_link_file_to_check))
        elif task_id == 252:
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Falha ao tentar transferir o arquivo anp.url_link_file_to_check"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.url_link_file_to_check":"%s"' % (task_id, anp.url_link_file_to_check))
        elif task_id == 253:
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Falha ao tentar MOVER o arquivo anp.file_to_check para anp.fold_transferred"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.file_to_check":"%s"' % (task_id, anp.file_to_check))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.fold_transferred":"%s"' % (task_id, anp.fold_transferred))
        elif task_id == 254:
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Falha ao tentar COPIAR o arquivo anp.file_to_check para anp.fold_verify"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.file_to_check":"%s"' % (task_id, anp.file_to_check))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.fold_transferred":"%s"' % (task_id, anp.fold_transferred))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.fold_verify":"%s"' % (task_id, anp.fold_verify))
        elif task_id == 300:     # vamos carregar o conteúdo do arquivo da pasta 2 para a memória a fim de efetuar as devidas correções
            next_task_id = 310
            # TODO: !!! verificar possibilidade de substituir por metodo generico
            if os.path.isfile(anp.fold_verify + anp.file_to_check):
                try:    # atenção Thalita avisou: arquivos CSV da ANP codificados em UTF-16 "Codificação em UCS-2 Little Endian"
                    cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "anp.file_to_check":"%s"' % (task_id, anp.file_to_check))
                    cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "anp.fold_verify":"%s"' % (task_id, anp.fold_verify))
                    #df, file_encoding = read_file(anp.fold_verify + anp.file_to_check)
                    cartel.read_file(anp, file_extension='', header=0, na_values='-', decimal=',', dayfirst=True)
                    df = anp.dict_csv_files[anp.file_to_check]
                    if len(df) == 0:
                        next_task_id = 351
                    if 'pandas.core.frame.DataFrame' not in "%s" % type(df):
                        next_task_id = 352
                except Exception:
                    next_task_id = 353
            else:
                next_task_id = 350
        elif task_id == 310:        # Alterações no nome dos campos:
            next_task_id = 320      # retirar acentos, caso das letras em minúsculas, trocar "ESPAÇO" por "TRAÇO EMBAIXO"
            # TODO: !!! verificar possibilidade de substituir por metodo generico
            try:
                df.rename(cartel.ascii_normalizer, axis='columns')
                # for c in df.columns:
                #     new_name_field = remove_accented_chars(c)
                #     new_name_field = new_name_field.replace('-', '')
                #     new_name_field = new_name_field.replace('  ', ' ')
                #     new_name_field = new_name_field.replace(' ', '_')
                #     new_name_field = new_name_field.lower()
                #     df = df.rename(columns={c:new_name_field})
            except Exception:
                    next_task_id = 353
        elif task_id == 320:        # alterações nos registros em formato de campos
            next_task_id = 330      # data_da_coleta
            # TODO: !!! verificar possibilidade de substituir por metodo generico
            try:                    # valor_de_venda, valor_de_compra
                df['data_da_coleta'] = pd.to_datetime(df['data_da_coleta'], errors='coerce').dt.date # TODO: testar se der erro na conversao, com o coerce, se funciona o '.dt.date'
                df['valor_de_venda'] = df['valor_de_venda'].astype(float)
                df['valor_de_compra'] = df['valor_de_compra'].astype(float)
                # for anp.recno_id, row in df.iterrows():
                #     if 'data_da_coleta' in df.columns:
                #         old_value = row['data_da_coleta']
                #         if len(old_value) == 10 and old_value.count('/') == 2:
                #             new_value = old_value.split('/')
                #             new_value = "%s-%s-%s" % (new_value[2], new_value[1], new_value[0])
                #             df.at[anp.recno_id, 'data_da_coleta'] = new_value
                #             # TODO: revisar pois talvez não precise fazer isso e gravar como NÚMERO no PostGreSQL
                #             if 'valor_de_venda' in df.columns:
                #                 old_value = "%s" % row['valor_de_venda']
                #                 old_value = old_value.replace('.','').replace(',','')
                #                 if old_value.isdigit():
                #                     new_value = "%s" % row['valor_de_venda']
                #                     new_value = new_value.replace('.',',')
                #                     df.at[anp.recno_id, 'valor_de_venda'] = new_value
                #             if 'valor_de_compra' in df.columns:
                #                 old_value = "%s" % row['valor_de_compra']
                #                 old_value = old_value.replace('.','').replace(',','')
                #                 if old_value.isdigit():
                #                     new_value = "%s" % row['valor_de_compra']
                #                     new_value = new_value.replace('.',',')
                #                     df.at[anp.recno_id, 'valor_de_compra'] = new_value
            except Exception:
                next_task_id = 354
        elif task_id == 330:        # aplicar filtros no arquivo, filtrar registros, "estado_sigla" = "RJ"
            next_task_id = 340
            # TODO: !!! verificar possibilidade de substituir por metodo generico
            try:                    # Atenção aos produtos desejados "ETANOL", "GASOLINA"
                df_filtered = cartel.filter_dataframe(df=df, estado_sigla=['RJ'], produtos=['ETANOL','GASOLINA'])
                cartel.gravar_saida(df=df_filtered, file_pat=anp.fold_verified+anp.file_to_check, sep=';', decimal=',')
                # df_filtered = df[(df['estado_sigla']=='RJ') & ( (df['produto']=='ETANOL') | (df['produto']=='GASOLINA') )]
                # new_file = anp.fold_verified + anp.file_to_check
                # df_filtered.to_csv(new_file)    # gravar o arquivo trabalhado na pasta 3
            except Exception:
                next_task_id = 355
        elif task_id == 350:
            next_task_id = 17   # TODO: next_task_id = 200      # vamos avisar sobre o erro e ir para o próximo
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Não existe o arquivo anp.file_to_check na pasta anp.fold_verify"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.file_to_check":"%s"' % (task_id, anp.file_to_check))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.fold_verify":"%s"' % (task_id, anp.fold_verify))
        elif task_id == 351:
            next_task_id = 17   # TODO: next_task_id = 200      # vamos avisar sobre o erro e ir para o próximo
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Estranho caso de tamanho ZERO do arquivo anp.file_to_check na pasta anp.fold_verify"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.file_to_check":"%s"' % (task_id, anp.file_to_check))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.fold_verify":"%s"' % (task_id, anp.fold_verify))
        elif task_id == 352:
            next_task_id = 17   # TODO: next_task_id = 200      # vamos avisar sobre o erro e ir para o próximo
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Não foi possível ler arquivo anp.file_to_check na pasta anp.fold_verify"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Conteúdo não foi convertido em objeto <pandas.core.frame.DataFrame>"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.file_to_check":"%s"' % (task_id, anp.file_to_check))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.fold_verify":"%s"' % (task_id, anp.fold_verify))
        elif task_id == 353:
            next_task_id = 17   # TODO: next_task_id = 200      # vamos avisar sobre o erro e ir para o próximo
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Não foi possível abrir o arquivo anp.file_to_check na pasta anp.fold_verify"' % task_id)
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.file_to_check":"%s"' % (task_id, anp.file_to_check))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "anp.fold_transferred":"%s"' % (task_id, anp.fold_verify))
        elif task_id == 354:
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Erro na alteração de valores no registro %s "' % (task_id, anp.recno_id))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Verifique os campos data_da_coleta, valor_de_venda, valor_de_compra na linha %s"' % (task_id, anp.recno_id))
        elif task_id == 355:
            next_task_id = 17
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Erro na gravação do arquivo %s na pasta anp.fold_verified"' % (task_id, anp.file_to_check))
            cartel.print_and_log('"debug_level": "error", "task_id": "%s", "msg":"Verifique %s na pasta %s"' % (task_id, anp.file_to_check, anp.fold_verified))
        elif task_id == 340:     # gerar arquivo SQL para inserir no banco de dados
            next_task_id = 341
        elif task_id == 341:     # efetuar conexao com o banco de dados e atualizar o arquivo
            next_task_id = 342

        elif task_id == 342:     # conferir antes se o registro está diferente
                                 # AUTO POSTO IGARAPE PRETO LTDA - ME  34711457000126  GASOLINA     2019-08-06           5,07          4,2436

            next_task_id = 343
        elif task_id == 343:     # confirmar como registrar os históricos de atualização pois
            next_task_id = 1000

        elif task_id == 1000:
            cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "msg":"Tempos melhores virão! :-)"' % task_id)
            break
        else:
            cartel.print_and_log('"debug_level": "debug", "task_id":  "%s", "msg":"Parece que não há o estado"' % task_id)
            break
        cartel.print_and_log('"debug_level": "debug", "task_id": "%s", "next_task_id":"%s", "anp.file_to_check":"%s"' % (task_id, next_task_id, anp.file_to_check))
        task_id = next_task_id


if __name__ == '__main__':
    main()

