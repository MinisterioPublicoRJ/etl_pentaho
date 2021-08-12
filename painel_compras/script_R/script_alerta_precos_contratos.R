# 2021/08/10 18h09 version 0.4
# Autoras: Mariana Casemiro/Thalita Nascimento

rm(list=ls())

#install.packages(c("tidyverse","magrittr","zoo","dplyr","purrr","tidyr","outliers","data.table",
#                  "stringr"), dependencies = TRUE)

library(tidyverse)
library(magrittr)
library(zoo)
library(dplyr)
library(purrr)
library(tidyr)
library(outliers)
library(data.table)
library(stringr)

setwd("E:/pentaho/etl/painel_compras/var/tmp/saida")

# abrindo view de itens classificados pela equipe de Engenharia de Dados
df <- read.csv2("itens_contratos_filtrados.csv", header = TRUE, sep = ';', dec = '.', encoding = 'UTF-8')

# abrindo historico de alertas para ver contratacoes que já foram consideradas suspeitas
suspeitos <- read.csv2("alertas_historico.csv", header = TRUE, sep = ';', dec = '.', encoding = 'UTF-8')

# ajustando nome das colunas para minísculo
colnames(df) <- str_to_lower(colnames(df))

# selecionando colunas e colocando em ordem
df <- select(df,c("contratacao", "status", "orgao", "processo", "objeto", "tipo_aquisicao", "criterio_julgamento",
  "fornecedor", "cpf_cnpj", "dt_contratacao", "dt_inicio", "dt_fim", "vl_estimado", "vl_empenhado", "vl_executado",
  "vl_pago", "id_item", "item","tp_item" ,"qtd", "qtd_aditiv_supr", "vl_unit", "vl_unit_aditiv_supr")) 


# alterando classes
df$vl_estimado <- as.numeric(df$vl_estimado)
df$vl_empenhado <- as.numeric(df$vl_empenhado)
df$vl_executado <- as.numeric(df$vl_executado)
df$vl_pago <- as.numeric(df$vl_pago)
df$vl_unit <- as.numeric(df$vl_unit)
df$vl_unit_aditiv_supr <- as.numeric(df$vl_unit_aditiv_supr)
df$qtd <- as.numeric(df$qtd)
df$qtd_aditiv_supr <- as.numeric(df$qtd_aditiv_supr)

# Retirando contratacoes Canceladas ou Encerradas
df %<>% filter(status != "CANCELADO")
df %<>% filter(status != "REJEITADO")
df %<>% filter(tp_item == "produto")

# Retirando linhas com valor do item 0
df %<>% filter(vl_unit != 0.00)

######################## Metodologia para analise de preços ###################
# Considera contratos similares(análise feito pelo id_item) dos últimos 180 dias
# Primeiro passo: gerar analise
# Segundo passso: gerar dados para alimentar tabela de alertas
# Terceiro passo: chamar tabela de alertas para verificar contratações que foram alertas
# Quarto passo: Para casos em que o preço mínimo ja tenha sido um alerta separar em 3 tabelas


#Filtrando para contratos dos últimos 180 dias
df$dt_contratacao <- as.Date(df$dt_contratacao)
ultimodia <- max(df$dt_contratacao)
d180 <- ultimodia - 180 
df %<>% filter(dt_contratacao<= ultimodia, dt_contratacao >= d180)


#CALCULANDO MÉTRICAS ESTATÍSTICAS PARA O ALERTA, SEGUNDO CADA ID_ITEM

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
    
    outlier_inferior = quartil_um - 1.5*(quartil_tres - quartil_um),
    
    outlier_superior = quartil_tres + 1.5*(quartil_tres - quartil_um),
    
    n = length(id_item) )


df = left_join(df, df.summary)

######################## JOIN tabela com suspeitos ################################
# criar condição: se tabela suspeitos estiver vazia não realizar join


# fazendo join para trazer coluna informando se contratacao já foi considerada suspeita
if(is_empty(suspeitos$contratacao) == 'FALSE'){
  
  join <- suspeitos %>% select(contratacao, id_item, cpf_cnpj, alerta) %>% filter(alerta == "Preço Anormal")
  colnames(join) <- c("contratacao", "id_item", "cpf_cnpj","suspeitos")
  df <- left_join(df,join, by = c("contratacao", "id_item", "cpf_cnpj"))
  
  df %<>% mutate(preco_suspeito = ifelse(vl_unit == preco_minimo & suspeitos == "Preço Anormal",
   "Avaliar contratações deste item","" ))
  
  x <- ifelse("Avaliar contratações deste item" %in% df$preco_suspeito, 1, 0)
  if(x == 1 ){
    tb_precos_alertas  <- df %>% filter(preco_suspeito == "Avaliar contratações deste item") %>%
      select(id_item,preco_suspeito)
    tb_precos_alertas <- distinct(tb_precos_alertas)
    tb <- df[,-35]
    tb_precos_alertas <- left_join(tb,tb_precos_alertas, by = "id_item")
    tb_precos_alertas %<>% filter(preco_suspeito == "Avaliar contratações deste item")
    tb_precos_alertas %<>% mutate(periodo_analise = paste(d180,"até",ultimodia))
    tb_precos_alertas %<>% select("contratacao",  "status",             "orgao",                "processo",         
                        "objeto",                 "tipo_aquisicao",     "criterio_julgamento",  "fornecedor",         
                        "cpf_cnpj",               "dt_contratacao",     "vl_estimado",          "vl_empenhado", 
                        "vl_executado",           "vl_pago",            "id_item",              "item",   
                        "tp_item",                "qtd",                "qtd_aditiv_supr",      "vl_unit", 
                        "vl_unit_aditiv_supr",    "preco_minimo",       "preco_maximo",         "quartil_um",   
                        "quartil_dois",           "quartil_tres",       "n" , "preco_suspeito", "periodo_analise")

    write.csv2(tb_precos_alertas, "alertas_contratos_avaliacao.csv", row.names=F, dec = '.', fileEncoding = "UTF-8", na="")
  }


}


# Seguir tabela normal de analise

df %<>% dplyr::mutate(var_perc=round(((vl_unit/preco_minimo)-1)*100,2))

df$alerta <- ifelse(df$var_perc >= 30 , "Preço Anormal", "")

df$mensagem <- ifelse(df$var_perc >= 30 , paste0("O valor unitário deste item é ", df$var_perc,
                       "% maior que o menor preço!"), "")

df %<>% mutate(contrato_id_item = paste0(contratacao,"-",id_item))

df<- arrange(df, desc(var_perc))
df %<>% filter(n > 3)

options(scipen = 999) #

rm(df.summary, join, suspeitos)


# adicionar coluna de data

df %<>% mutate(periodo_analise = paste(d180,"até",ultimodia))

# selecionando colunas para a tabela final

# contratacao,status,orgao,processo,objeto,tipo_aquisicao,criterio_julgamento,fornecedor,cpf_cnpj,
# dt_contratacao,vl_estimado,vl_empenhado,vl_executado,vl_pago,id_item,item,tp_item,qtd,qtd_aditiv_supr,
# vl_unit, vl_unit_aditiv_supr,preco_minimo,preco_maximo,quartil_um,quartil_dois,quartil_tres,n,var_perc,
# mensagem, contrato_id_item,periodo_analise

names(df)
df %<>% select("contratacao",           "status",            "orgao",               "processo",         
               "objeto"  ,              "tipo_aquisicao",    "criterio_julgamento", "fornecedor",         
               "cpf_cnpj" ,             "dt_contratacao" ,   "vl_estimado",         "vl_empenhado", 
               "vl_executado",          "vl_pago",           "id_item",             "item",   
               "tp_item",               "qtd",               "qtd_aditiv_supr",     "vl_unit", 
               "vl_unit_aditiv_supr",   "preco_minimo",      "preco_maximo",        "quartil_um",   
               "quartil_dois",          "quartil_tres",      "n",                   "var_perc",      
               "alerta","mensagem",     "contrato_id_item",  "periodo_analise")


write.csv2(df, "alertas_contratos_produtos.csv", row.names=F, dec = '.', fileEncoding = "UTF-8", na="")


####################################### SALVANDO TABELA DE HISTORICO ###########################

historico <- df %>% filter(alerta == "Preço Anormal")
write.csv2(historico, "alertas_historico.csv", row.names=F, dec = '.', fileEncoding = "UTF-8", na="")

#################### FIM DO SCRIPT ###############################################################

