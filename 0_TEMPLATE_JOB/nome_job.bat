
cd "C:\pentaho\data-integration"
c:
Kitchen.bat /file:"E:\pentaho\etl\XXXX\XXXX.kjb"

REM para fornecer parâmetros para o job, acrescentar "-param:NOME_PARAM=VALOR_PARAM"
REM ## IMPORTANTE ## é necessário acrescentar o parâmetro na tela de configuração do job com um valor default.
REM Esse valor default será substituido pelo definido no .bat