### TRABALHO RH - ANÁLISE DE DADOS CARGA DE TRABALHO
## INFORMAÇÕES IMPORTANTES:
# PARA CALCULO DE FEITOS POR SERVIDORES CONSIDERAMOS SOMA: total_servidor=servidor+cc+estatutario+extraqua_cedido
# PARA CALCULO DE CARGA DE TRABALHO DE PROMOTORIAS SEM SERVIDORES (ZERO SERVIDOR) == 200% (CARGA MUITO ACIMA DA MÉDIA)
# NÃO É POSSÍVEL FAZER SIMULAÇÃO COMO NA PLANILHA ANTERIOR (LIMITAÇÃO R E TABLEAU) - LOGO TERÃO A SITUAÇÃO DO HOJE
# IDEIA PRINCIPAL É TRAZER INFORMAÇÕES EM TEMPO REAL, LOGO O SCRIPT SERÁ RODADO TODO DIA 01 DO MÊS (LEVAR EM CONSIDERAÇÃO SERVIDORES DO MÊS ANTERIOR E OS FEITOS, SOMA, DOS 12 MESES ANTERIORES)
#A QUERY JÁ DEVE CONTER ESTA OPÇÃO DE TRAZER OS DADOS DOS ÚLTIMOS 12 MESES - SCRIPT NÃO TEM DATA, POIS NÃO VEIO A COLUNA DATA, LOGO A QUERY DEVE FAZER ESTE FILTRO.


##limpar workspace 
rm(list=ls())

library(plyr)
#library(tidyverse)
library(magrittr)
library(zoo)
library(dplyr)
library(purrr)
library(tidyr)
library(outliers)
library(data.table)
library(stringr)
library(futile.logger)
#library(optparse)
library(yaml)
#library(here)

options(encoding = "UTF-8")

#ler aquivo de configuracao
config = yaml.load_file(normalizePath("../etc/settings.yml"))
flog.appender(appender.file(file.path(normalizePath(paste0(config$folders$LOG_DIR ,config$settings$LOG_FILE_NAME)))), name='logger.painel_rh')
flog.threshold(INFO, name='logger.painel_rh')
flog.info('Starting Rscript %s.....', config$settings$ETL_JOB, name='logger.painel_rh')

base_dir <- config$folders$ETL_DIR
entrada_dir <- config$folders$ENTRADA_DIR
saida_dir <- config$folders$SAIDA_DIR
file_in_estrutura_rh <- 'estrutura_rh.csv'
file_in_pjs_feitos <- 'pjs_feitos_2.csv'
file_out_carga_trab <- 'carga_trabalho_rh.csv'

#Definindo Diretório ----  setar diretório de onde estará a tabela em csv para consuta deste script
# setwd(file.path(normalizePath(entrada_dir)))

#Pacotes --- caso dê algum problema ao chamar a biblioteca, por favor utilizar nome do pacote entre aspas e função install.packages("nome do pacote que não está instalado e deu erro")


#Chamando arquivo ---- chamar duas tabelas (RH e Feitos corresponde sempre ao último dia do mês anterior - ideia rodar primeiro dia do mês, sendo que a query pega mes hoje -1)
flog.info('lendo o arquivo %s%s .....', entrada_dir, file_in_estrutura_rh, name='logger.painel_rh')
base_rh<-read.csv2(file.path(normalizePath(paste0(entrada_dir, file_in_estrutura_rh))),encoding = 'UTF-8',header=TRUE, sep = ";")
flog.info('lendo o arquivo %s%s .....', entrada_dir, file_in_pjs_feitos, name='logger.painel_rh')
base_feitos <- read.csv2(file.path(normalizePath(paste0(entrada_dir, file_in_pjs_feitos))),encoding = 'UTF-8')


names(base_rh)
#Mudando as ordens das colunas
base_rh = base_rh %>% select("cdorg_p", "promotoria", "pacote_2020", "craai", "orgao_superior_nm", "valor_serv",
                             "ocup_excl_de_cargo_em_comissao",
                             "extraquadro_cedido", "estatutario_ativo", "estagiario_nao_forense", "estagiario_forense",
                             "assessor")
base_feitos = base_feitos %>%  select("pacote_2020", "cdorgao", "nome_orgao", "craai", "comarca", "qt_feitos")

#renomear colunas e fazer join das tabelas rh e feitos (nesta ordem)
colnames(base_rh) <- c("cod_orgao","promotoria","pacote","craai", "orgao_sup", "servidor",
                       "cc","extraqua_cedido","estatutario","estagiario_nao_forense","estagiario_forense",
                       "assessor")
colnames(base_feitos) <- c("pacote","cod_orgao","promotoria","craai","comarca","qtd_feitos")


#organizando os dados
base_join <- left_join(base_rh,base_feitos) #unindo os bancos
df<- base_join%>%
  filter(pacote!= "") #elininando as linhas sem valores de pacote
as.data.frame(df)

class(base_rh$promotoria) #verificando a classe da coluna promotoria
class(base_feitos$cod_orgao)


df_1 <- df%>%arrange(df$pacote) #ordenando por pacote
df_2 <- df_1 %>% group_by(pacote) %>% tally() #criando coluna com o numero de linhas iguais em 'pacote'
df_3 <- full_join(df_1,df_2) #unindo os df criados anteriormente

#colocar ordem descrescente
detach("package:plyr", unload=TRUE)
df_4 <- df_3 %>% 
  arrange(pacote, desc(qtd_feitos)) %>% 
  group_by(pacote) %>% 
  mutate(rank=row_number())

#inserir coluna posição - promotoria/pacote
df_5<-df_4%>%
  mutate(paste(rank, n, sep="/"))

#somar estagiários
df_6 <- df_5%>%
  mutate(total_estagiario= estagiario_nao_forense + estagiario_forense)
#somar servidores (servidor, cc, estatutário, extraquadrocedido)
#df_7<- df_6%>%
# mutate(total_servidor=servidor+cc+estatutario+extraqua_cedido)
#calculo de feitos por servidor
df_8<- df_6%>%
  mutate(feitos_por_servidor=round(qtd_feitos/servidor))


######    Substituindo os NAs em "0"
df_8$feitos_por_servidor[is.na(df_8$feitos_por_servidor)] <- 0


#### calculo dos quartis 1 e 3 por pacote de atribuição
quartis <- df_8%>%
  select(pacote,feitos_por_servidor)


library(plyr)

indicadores_todos <- ddply(quartis,
                           c("pacote"),
                           summarise,
                           quartil_um = round(quantile(feitos_por_servidor, c(0.25))),
                           quartil_tres = round(quantile(feitos_por_servidor, c(0.75))))
df_9 <- full_join(df_8,indicadores_todos)
#adicionar coluna para comparação com q1 e q3, se num de feitos por servidor for menor que q1 então (numfeitoserv/q1 -1), se num de feitos por serv. >q3 então numfeitoserv/q3-1, logo se num feitos q1<=q3 (entre) então zero. Dentro do esperado

df_10 <- df_9%>%
  mutate(situacao_atual_perc = ifelse(df_9$feitos_por_servidor<df_9$quartil_um, paste0(round(((df_9$feitos_por_servidor/df_9$quartil_um-1)*100),2)),
                                      ifelse(df_9$feitos_por_servidor>df_9$quartil_tres, paste0(round(((df_9$feitos_por_servidor/df_9$quartil_tres-1)*100),2)),
                                             ifelse(df_9$servidor=="0",paste0("200"),"0"))))

#adicionar coluna com descritivo da situação carga de trabalho
#130%	500% ----- CT muito acima da média
#60%	130% ----- CT acima da média 
# 1%	60% ----- CT levemente acima da média
#0%	0%    ----- CT média
#-1%	-30% ----- CT levemente abaixo da média
#-30%	-70% ----- CT abaixo da média
#-70%	-200% ----- CT muito abaixo da média


class(df_10$situacao_atual_perc)
df_10$situacao_atual_perc <- as.numeric(df_10$situacao_atual_perc)

library(dplyr)

df_11 <- df_10%>%
  mutate(carga_trabalho = case_when(
    df_10$situacao_atual_perc == 0 ~ paste0("carga de trabalho média"),
    df_10$situacao_atual_perc > 0 & df_10$situacao_atual_perc < 60 ~ paste0("carga de trabalho levemente acima da média"),
    df_10$situacao_atual_perc >= 60 & df_10$situacao_atual_perc < 130 ~ paste0("carga de trabalho acima da média"),
    df_10$situacao_atual_perc >= 130 ~ paste0("carga de trabalho muito acima da da média"),
    df_10$situacao_atual_perc < 0 & df_10$situacao_atual_perc > -30 ~ paste0("carga de trabalho levemente abaixo da média"),
    df_10$situacao_atual_perc <= -30 & df_10$situacao_atual_perc > -70 ~ paste0("carga de trabalho abaixo da média"),
    df_10$situacao_atual_perc <= -70 ~ paste0("carga de trabalho muito abaixo da média")))


#escolha das colunas que irão na tabela
tabela_rh_calculo <- df_11%>%
  select(craai, orgao_sup, cod_orgao,promotoria,pacote,comarca,qtd_feitos,n,rank,`paste(rank, n, sep = "/")`,feitos_por_servidor,quartil_um,quartil_tres,situacao_atual_perc,carga_trabalho)
colnames(tabela_rh_calculo) <- c("craai","orgao_sup","cod_orgao","promotoria","pacote","comarca","total_feitos","qtd_no_pct","ordem","posicao","feitos_por_servidor","q_1","q_3","perc_situacao_atual","carga_trabalho_atual")

tabela_completa <- left_join(tabela_rh_calculo,base_rh)
tabela_final <- left_join(tabela_completa,base_feitos)

tabela_final$posicao <- as.character(tabela_final$posicao)

#inserir coluna de feitos por servidor por orgão superior
detach("package:plyr", unload=TRUE)
tab_orgao_sup<- tabela_final %>% 
  arrange(orgao_sup, desc(qtd_feitos)) %>% 
  group_by(orgao_sup)%>% 
  mutate(rank_orgao_sup=row_number())

#quantidade de promotorias por orgão superior
df_orgao_sup <- tab_orgao_sup%>%
  group_by(orgao_sup)%>%
  mutate(qtd_por_orgao_sup= n())


#posição por orgão superior e número de feitos do maior para o menor
entrega_rh <- df_orgao_sup%>%
  mutate(posicao_orgao_sup = paste(rank_orgao_sup, qtd_por_orgao_sup, sep="/"))

#adicionar coluna da data de atualização
entrega_rh %<>% mutate(data_atualizacao = Sys.time())

#alterar classe da coluna de data
entrega_rh$data_atualizacao<- strftime(entrega_rh$data_atualizacao, format = "%Y-%m-%d %H:%M")

entrega_final <- entrega_rh
class(entrega_final)
entrega_final <- as.data.frame(entrega_final)
entrega_final %<>% replace(.=="Inf","")


#Local onde a tabela será salva
setwd(file.path(normalizePath(saida_dir)))

#salvando no diretório
flog.info('Gravando o arquivo %s%s .....', saida_dir, file_out_carga_trab, name='logger.painel_rh')
write.csv2(entrega_final,paste0(file_out_carga_trab),  fileEncoding = "UTF-8",  na = "", row.names = FALSE)

