REM passar -c para o python pra checar se existe difenca de no numero de linhas das tabelas do gate, e rodar esse script com agendamento para toda hora
cd %KETTLE_HOME%
%KETTLE_DRIVE%
Kitchen.bat /file:%ETL_DIR%painel_compras\executa_painel_compras.kjb "-param:PYTHON_PARAM=c"

REM para fornecer parâmetros para o job, acrescentar "-param:NOME_PARAM=VALOR_PARAM"
REM ## IMPORTANTE ## é necessário acrescentar o parâmetro na tela de configuração do job com um valor default.
REM Esse valor default será substituido pelo definido no .bat
