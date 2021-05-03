REM passar -t para o python pra checar se existe difenca de dados na tabela do gate, e rodar esse script com agendamento todo dia as 03:30 am
cd "C:\pentaho\data-integration"
c:
Kitchen.bat /file:"E:\pentaho\etl\painel_compras\executa_painel_compras.kjb" "-param:PYTHON_PARAM=t"

REM para fornecer parâmetros para o job, acrescentar "-param:NOME_PARAM=VALOR_PARAM"
REM ## IMPORTANTE ## é necessário acrescentar o parâmetro na tela de configuração do job com um valor default.
REM Esse valor default será substituido pelo definido no .bat