REM passar -r para o python pra rodar esse script normal com agendamento todo dia as 13:30 pm
cd %KETTLE_HOME%
%KETTLE_DRIVE%
Kitchen.bat /file:%ETL_DIR%painel_compras\executa_painel_compras.kjb "-param:PYTHON_PARAM=r"

REM para fornecer parâmetros para o job, acrescentar "-param:NOME_PARAM=VALOR_PARAM"
REM ## IMPORTANTE ## é necessário acrescentar o parâmetro na tela de configuração do job com um valor default.
REM Esse valor default será substituido pelo definido no .bat
