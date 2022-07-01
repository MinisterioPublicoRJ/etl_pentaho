# 2022/07/01 15h30 version 0.8
# Autoras: Mariana Casemiro/Thalita Nascimento/Rebecca Souza


rm(list=ls())

#install.packages(c("tidyverse","magrittr","zoo","dplyr","purrr","tidyr","outliers","data.table",
#                  "stringr", "spatstat"), dependencies = TRUE)

library(tidyverse)
library(magrittr)
library(zoo)
library(dplyr)
library(purrr)
library(tidyr)
library(outliers)
library(data.table)
library(stringr)
library(spatstat)

# TODO: Colocar diretorio do Pentaho 
setwd("E:/pentaho/etl/ALERTA_COMPRAS_RJ")

# abrindo view de itens classificados pela equipe de Engenharia de Dados
df <- read.csv2("itens_contratos_filtrados.csv", header = TRUE, sep = ';',dec = ".", encoding = 'UTF-8')

# ajustando nome das colunas para minusculo
colnames(df) <- str_to_lower(colnames(df))

# Limpezas 
df <- df %>%
  select(contratacao,status,orgao,processo,objeto,tipo_aquisicao,criterio_julgamento,
         fornecedor,cpf_cnpj,dt_contratacao,dt_inicio,dt_fim,vl_estimado,vl_empenhado,
         vl_executado,vl_pago,id_item,item,tp_item,qtd,qtd_aditiv_supr,vl_unit,vl_unit_aditiv_supr) %>%
  mutate(vl_estimado = as.numeric(vl_estimado),
         vl_empenhado = as.numeric(vl_empenhado),
         vl_executado = as.numeric(df$vl_executado),
         vl_pago = as.numeric(vl_pago),
         vl_unit = as.numeric(vl_unit),
         vl_unit_aditiv_supr = as.numeric(vl_unit_aditiv_supr),
         qtd = as.numeric(qtd),
         qtd_aditiv_supr = as.numeric(qtd_aditiv_supr)) %>%
  filter(status != c("CANCELADO","REJEITADO") & 
           tp_item == "produto" &
           vl_unit != 0.00)

######################## Metodologia para analise de precos ###################
# Considera contratos similares(analise feito pelo id_item) dos ultimos 180 dias
# Gerar preco medio de id_item
# msg de alerta quando o vl_unit for maior que 30% do preco medio

#Filtrando para contratos dos ultimos 180 dias
df <- df %>%
  mutate(dt_contratacao = as.Date(dt_contratacao))

ultimodia <- max(df$dt_contratacao) 
d180 <- ultimodia - 180 
df %<>% filter(dt_contratacao<= ultimodia, dt_contratacao >= d180)

#--------Proposta de pegar o ultimo dia dentro de cada item-----
#df.ultimodia <- df %>%
#  group_by(id_item) %>%
#  summarise(
#    ultimodia = max(dt_contratacao)
#    )
#df <- left_join(df, df.ultimodia)
#df %<>% filter(dt_contratacao <= ultimodia, dt_contratacao >= ultimodia-180)


#----- Calculando estatísticas para os alertas por id_item -----

df.summary <- df %>%
  group_by(id_item) %>%
  summarise(
    media_preco=round(mean(vl_unit),digits =  2),
    preco_minimo=min(vl_unit),
    preco_maximo=max(vl_unit),
    amplitude_preco=max(vl_unit)-min(vl_unit),
    quartil_um = quantile(vl_unit, c(0.25)),
    quartil_dois = quantile(vl_unit, c(0.5)),
    quartil_tres = quantile(vl_unit, c(0.75)),
    limite_inferior = quartil_um - 1.5*(quartil_tres - quartil_um),
    limite_superior = quartil_tres + 1.5*(quartil_tres - quartil_um),
    n = length(id_item),
    qt_fornecedor = n_distinct(fornecedor))

df = left_join(df, df.summary)

#---- Analisar apenas os itens com mais de 5 compras e mais de 2 fornecedores----
df %<>% filter(n > 5 & qt_fornecedor > 2)

df.summary2 <- df %>%
  group_by(id_item) %>%
  summarise(quantil_90_densi = as.numeric(quantile(density(vl_unit,
                                                           kernel="cosine"),prob=0.90)))

df = left_join(df, df.summary2)


#----- Criando alerta pela Media ------
df <- df %>%
  mutate(var_perc=round((vl_unit/media_preco-1)*100,2),
         contrato_id_item = paste0(contratacao,"-",id_item)) %>%
  arrange(desc(var_perc))

df$alerta_media <- ifelse(df$var_perc >= 30 , T, F)

#----- Criando alerta pelo intervalo interquartil ------
df$alerta_iq <- ifelse(df$vl_unit >= df$limite_superior , T, F)


#----- Criando alerta pelo quantil 90 da densidade --------
df$alerta_densidade <- ifelse(df$vl_unit >= df$quantil_90_densi , T, F)


#----- Mensagens de alerta vermelho e alerta amarelo ------

df$alerta_msg <- ifelse(df$alerta_densidade + df$alerta_media + df$alerta_iq == 3,
                        paste0("Alerta Vermelho"), 
                        ifelse(df$alerta_densidade + df$alerta_media + df$alerta_iq == 2,
                               paste0("Alerta Laranja"),
                               ifelse(df$alerta_media == 1,
                                      paste0("Alerta Amarelo"), "")))
df$mensagem <- ifelse(df$var_perc >= 30 , paste0("O valor unitário deste item é ", df$var_perc, "% maior que o menor preço!"), "")
options(scipen = 999) #

rm(df.summary)
rm(df.summary2)
#rm(df.ultimodia)

#------- adicionar coluna informando período --------

df %<>% mutate(periodo_analise = paste(d180,"até",ultimodia))

# selecionando colunas para a tabela final
# contratacao,status,orgao,processo,objeto,tipo_aquisicao,criterio_julgamento,fornecedor,cpf_cnpj,
# dt_contratacao,vl_estimado,vl_empenhado,vl_executado,vl_pago,id_item,item,tp_item,qtd,qtd_aditiv_supr,
# vl_unit, vl_unit_aditiv_supr,preco_minimo,preco_maximo,quartil_um,quartil_dois,quartil_tres,n,var_perc,
# mensagem, contrato_id_item,periodo_analise

df %<>% select("contratacao","status"  ,"orgao","processo","objeto",
               "tipo_aquisicao","criterio_julgamento", "fornecedor","cpf_cnpj" ,
               "dt_contratacao" ,"vl_estimado","vl_empenhado","vl_executado",
               "vl_pago","id_item", "item", "tp_item","qtd","qtd_aditiv_supr",
               "vl_unit","vl_unit_aditiv_supr","media_preco",  "preco_minimo" ,"preco_maximo",
               "quartil_um","quartil_dois","quartil_tres","n","var_perc",
               "limite_superior", "quantil_90_densi",
               "alerta_media", "alerta_iq","alerta_densidade", "alerta_msg",
               "mensagem",
               "contrato_id_item",
               "periodo_analise")

write.csv2(df, "alertas_contratos_produtos.csv", row.names=F, fileEncoding = "UTF-8", na="")

####################################### SALVANDO TABELA DE HISTORICO ###########################
write.csv2(df, "alertas_historico.csv", row.names=F, fileEncoding = "UTF-8", na="")

#################### FIM DO SCRIPT ###############################################################
