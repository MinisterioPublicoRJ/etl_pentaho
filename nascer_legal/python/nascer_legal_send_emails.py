import logging
import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import mpmapas_commons as commons
import mpmapas_logger
import pandas as pd
from db_utils import mpmapas_db_commons as dbcommons
from mpmapas_exceptions import MPMapasException

os.environ["NLS_LANG"] = ".UTF8"
dt_now = datetime.now(timezone.utc)


class Destinatario:
    def __init__(self, nome: str, email: str):
        self.nome = nome
        self.email = email


class Estabelecimento:
    def __init__(self, tipo: str, codigo: int, unidade: str, destinatarios: list[Destinatario]):
        self.tipo = tipo
        self.codigo = codigo
        self.unidade = unidade
        self.destinatarios = destinatarios

    @staticmethod
    def get_all_estabelecimentos(df_estabelec: pd.DataFrame):
        dict_estabelec: dict[str: dict[int: Estabelecimento]] = {}
        dfg_dest_tipos = df_estabelec.groupby(by=['tipo'], axis='index')
        for key_tipo, df_dest in dfg_dest_tipos:
            for dest_index, row_dest in df_dest.iterrows():
                dfg_estab = df_dest.groupby(by=['codigo'], axis='index')
                dict_estab: dict[int: Estabelecimento] = {}
                for key_codigo, df_estab in dfg_estab:
                    for estab_index, row_estab in df_estab.iterrows():
                        list_dest: list[Destinatario] = []
                        dfg_dests = df_estab.groupby(by=['email'], axis='index')
                        for key_email, df_nome in dfg_dests:
                            for nome_index, row_nome in df_nome.iterrows():
                                list_dest.append(Destinatario(nome=row_nome['destinatario'], email=row_nome['email']))
                        dict_estab[row_estab['codigo']] = Estabelecimento(row_estab['tipo'], row_estab['codigo'],
                                                                          row_estab['unidade'], list_dest)
                dict_estabelec[row_dest['tipo']] = dict_estab
        return dict_estabelec


class Pergunta:
    def __init__(self, cod_pergunta: str, ordem_pergunta: int, desc_pergunta: str, resposta: str):
        self.ordem_pergunta = ordem_pergunta
        self.cod_pergunta = cod_pergunta
        self.desc_pergunta = desc_pergunta
        self.resposta = resposta


class Questionario:
    def __init__(self, tipo: str, send_to: list[str], perguntas: dict[str: Pergunta]):
        self.tipo = tipo
        self.send_to = send_to
        self.perguntas: dict[str: Pergunta] = perguntas

    def ordenar_perguntas(self):
        self.perguntas = dict(sorted(self.perguntas.items(), key=lambda item: item[1].ordem_pergunta))

    @staticmethod
    def get_all_questionarios(df_surv: pd.DataFrame):
        dict_quests: dict[str: Questionario] = {}
        dfg_surv_tipos = df_surv.groupby(by=['tp_estalec'], axis='index')
        for key_tipo, df_tipo_surv in dfg_surv_tipos:
            for tipo_surv_index, row_tipo_surv in df_tipo_surv.iterrows():
                dict_pergs: dict[str: Pergunta] = {}
                dfg_pergs = df_tipo_surv.groupby(by=['cod_pergunta'], axis='index')
                for key_perg, df_perg in dfg_pergs:
                    for perg_index, row_perg in df_perg.iterrows():
                        dict_pergs[row_perg['cod_pergunta']] = Pergunta(ordem_pergunta=row_perg['ordem_pergunta'],
                                                                        cod_pergunta=row_perg['cod_pergunta'],
                                                                        desc_pergunta=row_perg['desc_pergunta'],
                                                                        resposta='')
                dict_quests[row_tipo_surv['tp_estalec']] = Questionario(tipo=row_tipo_surv['tp_estalec'],
                                                                        send_to=row_tipo_surv['send_to'].split(','),
                                                                        perguntas=dict_pergs)
        return dict_quests


class Email:
    def __init__(self, mail_from: str, mail_to: list[str], mail_cc: list[str], mail_bcc: list[str], mail_subject: str,
                 mail_body_html: str):
        self.email_msg = MIMEMultipart()
        self.email_msg['From'] = mail_from
        self.email_msg['To'] = ','.join(mail_to)
        self.email_msg['Cc'] = ','.join(mail_cc)
        self.email_msg['Bcc'] = ','.join(mail_bcc)
        self.email_msg['Subject'] = mail_subject
        self.email_msg.attach(MIMEText(mail_body_html, 'html'))


class Survey:
    def __init__(self, tipo: str, estabelecimento: Estabelecimento, questionario: Questionario, respondente: str,
                 data_resposta: datetime, estab_cc: list[Estabelecimento], survey):
        self.neg_num = self.preencher_neg_num(questionario)
        self.tipo = tipo
        self.estabelecimento = estabelecimento
        self.survey = survey
        self.questionario = self.preencher_questionario(questionario, survey)
        self.respondente = respondente
        self.data_resposta = data_resposta
        self.estab_cc = estab_cc
        self.email: Email = self.preencher_email()

    @staticmethod
    def preencher_questionario(questionario, survey):
        for pergunta in questionario.perguntas.values():
            pergunta.resposta = survey[pergunta.cod_pergunta] if survey[pergunta.cod_pergunta] != '' else 'Sem Resposta'
        return questionario

    @staticmethod
    def preencher_neg_num(questionario):
        neg_num = False
        for pergunta in questionario.perguntas.values():
            try:
                resp = int(pergunta.resposta)
                if resp < 0:
                    neg_num = True
                    logger.info('neg_num: {neg} -> {perg} => {resp}'.format(neg=neg_num, perg=pergunta.cod_pergunta, resp=pergunta.resposta))
            except ValueError:
                resp = 0
        return neg_num

    def preencher_email(self):
        email_to = [dests.email for dests in self.estabelecimento.destinatarios if
                    configs.settings.MAIL_TO]
        email_cc = [des.email for cc in self.estab_cc for des in cc.destinatarios if
                    cc.codigo == 999999 and configs.settings.MAIL_TO]
        email_bcc = [des.email for cc in self.estab_cc for des in cc.destinatarios if
                     cc.codigo == 0]
        mail_from = configs.settings.MAIL_SENDER
        email_subject = '{neg_num}Survey estabelecimento: {unidade} respondente: {respondente} data da resposta: {data_resposta}'.format(
            neg_num='NEG_NUM NAO PODE <> ' if self.neg_num else '', unidade=self.estabelecimento.unidade,
            respondente=self.respondente, data_resposta=self.data_resposta.strftime('%d/%m/%Y %H:%M:%S %z'))

        if self.neg_num:
            with open(configs.folders.CONFIG_DIR + 'template_email_neg_num.html', mode='r', encoding='utf-8') as file:
                template = file.readlines()
            email_template = ''.join(template)
        else:
            with open(configs.folders.CONFIG_DIR + 'template_email.html', mode='r', encoding='utf-8') as file:
                template = file.readlines()
            email_template = ''.join(template)
        with open(configs.folders.CONFIG_DIR + 'template_pergunta.html', mode='r', encoding='utf-8') as file:
            template = file.readlines()
        perguntas_template = ''.join(template)
        with open(configs.folders.CONFIG_DIR + 'template_resposta.html', mode='r', encoding='utf-8') as file:
            template = file.readlines()
        respostas_template = ''.join(template)

        self.questionario.ordenar_perguntas()

        perguntas = ''.join(
            [perguntas_template.format(pergunta=pergunta.desc_pergunta) +
             respostas_template.format(resposta=str(pergunta.resposta))
             for pergunta in self.questionario.perguntas.values()]
        )
        email_body_html = email_template.format(estabelecimento=self.estabelecimento.unidade,
                                                respondente=self.respondente,
                                                data_resposta=self.data_resposta.strftime('%d/%m/%Y %H:%M:%S %z'),
                                                pergunstas_respostas=perguntas)
        return Email(mail_from=mail_from, mail_to=email_to, mail_cc=email_cc, mail_bcc=email_bcc,
                     mail_subject=email_subject, mail_body_html=email_body_html)

    @staticmethod
    def get_surveys(list_surveys: list, tipo_survey: str, df_survey: pd.DataFrame,
                    dict_dest: dict[str: dict[int: Estabelecimento]], quest: Questionario):
        dict_est = {cd: est for val in dict_dest.values() for cd, est in val.items()}
        for index, row_survey in df_survey.iterrows():
            if 'det' in tipo_survey:
                campo_codigo = 'detran'
            elif 'cart' in tipo_survey:
                campo_codigo = 'cartorio'
            else:
                campo_codigo = 'cnes_estabelec'
            if ('det' in tipo_survey and str(row_survey[campo_codigo]) in dict_est) or \
                    ('det' not in tipo_survey and int(row_survey[campo_codigo]) in dict_est):
                estab = dict_est[int(row_survey[campo_codigo])]
                estab_cods: list[int] = [int(row_survey[campo_codigo])]
                if 'cart' in tipo_survey:
                    data_resp = row_survey['data1']
                else:
                    data_resp = row_survey['data_resp']
                estab_cc = [est for est in dict_est.values() if
                            (est.tipo in quest.send_to and est.tipo not in estab.tipo) and (
                                    est.tipo not in tipo_survey or (
                                    est.tipo in tipo_survey and est.codigo in estab_cods))]
                list_surveys.append(Survey(tipo=tipo_survey, estabelecimento=estab, questionario=quest,
                                           respondente=row_survey['respondente'], data_resposta=data_resp,
                                           estab_cc=estab_cc, survey=row_survey))
        return list_surveys


def preencher_survey(df_survey_dest: pd.DataFrame, df_survey_perguntas: pd.DataFrame,
                     dict_tipo_survey: dict[str: pd.DataFrame]):
    dict_estabelecimentos: dict[str: list[Estabelecimento]] = Estabelecimento.get_all_estabelecimentos(
        df_estabelec=df_survey_dest)
    dict_questionarios: dict[str: list[Questionario]] = Questionario.get_all_questionarios(df_surv=df_survey_perguntas)
    list_estalec_tipos = [key for key, _ in df_survey_perguntas.groupby(by=['tp_estalec'], axis='index')]
    list_surveys: list[Survey] = []
    for tipo in list_estalec_tipos:
        send_to = dict_questionarios[tipo].send_to
        dest_cart = {key: dict_estabelecimentos[key] for key in dict_estabelecimentos if key in send_to}
        quest_cart = dict_questionarios[tipo]
        df_survey = dict_tipo_survey[tipo]
        Survey.get_surveys(list_surveys=list_surveys, tipo_survey=str(tipo), df_survey=df_survey, dict_dest=dest_cart,
                           quest=quest_cart)
    return list_surveys


def load_surveys():
    logger.info('Starting %s - load_surveys.' % configs.settings.ETL_JOB)
    df_survey_perguntas = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GISDB_DS_NAME].jndi_name, schema_name='assistencia',
                                               table_name='survey_nascer_legal_perguntas')['table']
    df_survey_dest = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GISDB_DS_NAME].jndi_name, schema_name='assistencia', table_name='survey_email_dest')[
        'table']
    # filtrar fl_ativo true
    df_survey_dest = df_survey_dest.loc[df_survey_dest['fl_ativo']]
    df_survey_cart = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GISDB_DS_NAME].jndi_name, schema_name='assistencia',
                                          table_name='survey_nascer_legal_cart_3')['table'].fillna(value='')
    df_survey_detran = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GISDB_DS_NAME].jndi_name, schema_name='assistencia',
                                            table_name='survey_nascer_legal_detran')['table'].fillna(value='')
    df_survey_hosp = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GISDB_DS_NAME].jndi_name, schema_name='assistencia',
                                          table_name='survey_nascer_legal_hosp')['table'].fillna(value='')
    df_survey_enviados = dbcommons.load_table(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GISDB_DS_NAME].jndi_name, schema_name='assistencia',
                                              table_name='survey_nascer_legal_emails_enviados')['table']
    # filtrar emails ja enviados
    df_survey_hosp = df_survey_hosp[~df_survey_hosp['objectid'].isin(
        df_survey_enviados.loc[df_survey_enviados['tp_survey'] == 'cnes']['id_survey'].values)]
    df_survey_detran = df_survey_detran[~df_survey_detran['objectid'].isin(
        df_survey_enviados.loc[df_survey_enviados['tp_survey'] == 'det']['id_survey'].values)]
    df_survey_cart = df_survey_cart[~df_survey_cart['objectid'].isin(
        df_survey_enviados.loc[df_survey_enviados['tp_survey'] == 'cart']['id_survey'].values)]

    list_surveys: list[Survey] = preencher_survey(df_survey_dest, df_survey_perguntas,
                                                  {'cart': df_survey_cart, 'cnes': df_survey_hosp,
                                                   'det': df_survey_detran})

    logger.info('Finish %s - load_surveys.' % configs.settings.ETL_JOB)
    return list_surveys


def send_emails(list_surveys: list[Survey]):
    logger.info('Starting %s - send_emails.' % configs.settings.ETL_JOB)
    host = configs.settings.MAIL_SMTP_SERVER
    port = configs.settings.MAIL_SMTP_PORT
    list_enviados = []
    if configs.settings.MAIL_TO:
        with smtplib.SMTP(host, port) as smtp:
            smtp.ehlo('mprj.mp.br')
            for survey in list_surveys:
                if (not survey.neg_num) or (survey.neg_num and configs.settings.NEG_NUM_SEND_MAIL):
                    smtp.sendmail(survey.email.email_msg['From'],
                                  ','.join([survey.email.email_msg['To'], survey.email.email_msg['Cc'],
                                            survey.email.email_msg['Bcc']]).split(','),
                                  survey.email.email_msg.as_string())
                    nm_table = 'survey_nascer_legal_cart_3' if survey.tipo == 'cart' else 'survey_nascer_legal_hosp' \
                        if survey.tipo == 'cnes' else 'survey_nascer_legal_detran' if survey.tipo == 'det' else ''
                    list_enviados.append(
                        {'id_survey': survey.survey['objectid'], 'tp_survey': survey.tipo, 'nome_tabela': nm_table})
    else:
        for survey in list_surveys:
            if survey.neg_num and configs.settings.NEG_NUM_SEND_MAIL:
                logger.info('email from: {efrom} to: {eto} msg: {emsg}'.format(efrom=survey.email.email_msg['From'],
                                                                               eto=','.join([survey.email.email_msg['To'],
                                                                                             survey.email.email_msg['Cc'],
                                                                                             survey.email.email_msg[
                                                                                                 'Bcc']]).split(','),
                                                                               emsg=survey.email.email_msg.as_string()))
                nm_table = 'survey_nascer_legal_cart_3' if survey.tipo == 'cart' else 'survey_nascer_legal_hosp' \
                    if survey.tipo == 'cnes' else 'survey_nascer_legal_detran' if survey.tipo == 'det' else ''
                list_enviados.append(
                    {'id_survey': survey.survey['objectid'], 'tp_survey': survey.tipo, 'nome_tabela': nm_table})
            # else:
                # logger.info('pos_num -> {quest}'.format(quest=survey.tipo))
    logger.info('Finish %s - send_emails.' % configs.settings.ETL_JOB)
    return list_enviados


def update_enviados(list_enviados: list):
    logger.info('Starting %s - update_enviados.' % configs.settings.ETL_JOB)
    db_gisdb = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_GISDB_DS_NAME], api=None)
    db_opengeo = commons.get_database(configs.settings.JDBC_PROPERTIES[configs.settings.DB_OPENGEO_DS_NAME], api=None)
    server_encoding = dbcommons.show_server_encoding(configs=configs, jndi_name=configs.settings.JDBC_PROPERTIES[
        configs.settings.DB_GISDB_DS_NAME].jndi_name)
    df_enviados = pd.DataFrame(list_enviados)
    list_flds_enviados = df_enviados.columns.values
    insert_sql_enviados, insert_template_enviados = db_gisdb.insert_values_sql(schema_name='assistencia',
                                                                               table_name='survey_nascer_legal_emails_enviados',
                                                                               list_flds=list_flds_enviados,
                                                                               unique_field='objectid',
                                                                               pk_field='objectid')
    db_gisdb.execute_values_insert(sql=insert_sql_enviados,
                                   template=insert_template_enviados,
                                   df_values_to_execute=df_enviados,
                                   fetch=True, server_encoding=server_encoding)

    insert_sql_enviados, insert_template_enviados = db_opengeo.insert_values_sql(schema_name='assistencia',
                                                                                 table_name='survey_nascer_legal_emails_enviados',
                                                                                 list_flds=list_flds_enviados,
                                                                                 unique_field='objectid',
                                                                                 pk_field='objectid')
    db_opengeo.execute_values_insert(sql=insert_sql_enviados,
                                     template=insert_template_enviados,
                                     df_values_to_execute=df_enviados,
                                     fetch=True, server_encoding=server_encoding)
    logger.info('Finish %s - update_enviados.' % configs.settings.ETL_JOB)


def main():
    try:
        logger.info('Starting %s.' % 'nascer_legal_send_emails')
        list_surveys: list[Survey] = load_surveys()
        list_enviados = send_emails(list_surveys)
        update_enviados(list_enviados)

    except MPMapasException as c_err:
        logger.exception(c_err)
        exit(c_err.msg)
    except Exception as c_err:
        logger.exception('Fatal error in main')
        exit(c_err)
    finally:
        logger.info('Finishing %s.' % 'nascer_legal_send_emails')


global configs, logger

if __name__ == '__main__' or __name__ == 'nascer_legal_send_emails':
    try:
        configs = commons.read_config('../etc/settings.yml')
        if __name__ == '__main__':
            mpmapas_logger.Logger.config_logger(configs, logghandler_file=True)
        logger = logging.getLogger(configs.settings.ETL_JOB)
        if __name__ == '__main__':
            main()
    except Exception as excpt:
        logging.exception('Fatal error in %s' % __name__)
        exit(excpt)
