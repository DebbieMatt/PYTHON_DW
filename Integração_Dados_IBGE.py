import pandas as pd
from datetime import datetime
from pathlib import Path
import sqlite3 as sql 

# COLETA DE DADOS DO SITE IBGE

# Coletando dados do IBGE

url = 'https://www.ibge.gov.br/explica/codigos-dos-municipios.php#MT'

DadosIBGE = pd.DataFrame (pd.read_html(url, match= "Municípios de Mato Grosso")[0])

# RENOMEAR COLUNA DO DF
DadosIBGE = DadosIBGE.rename(columns={'Municípios de Mato Grosso': 'Munic', 'Códigos': 'Cod'} )

# ALTERANDO NOME DO INDEX
DadosIBGE.index.name = 'ID'


# ALTERANDO INDEX PARA COMEÇAR DO 1
DadosIBGE.index = DadosIBGE.index + 1

# ADICIONAR UMA COLUNA DE DATA E HORA DE CARGA
DadosIBGE['dataCarga'] = datetime.today().strftime('%d/%m/%y %H:%M')

# DadosIBGE

# CRIANDO O BANCO DE DADOS ODS
# Manipulando o sistema de arquivos
endereco = Path('C:\\\\Users\\\\debor\\\\OneDrive\\\\Documentos\\\\PYTHON DW\\\\')
                
BDODS = endereco / "ODS.db"
BDDW = endereco / "DW.db"

if endereco.exists():
    if(BDODS.exists() and BDDW.exists()):
        print('Banco de dados já existem !')
    else:
        BDODS.touch()
        BDDW.touch()
        print('Bancos de dados criados!')
else:
    print('Endereço não existe! Favor, verificar!')
    
# MANIPULANDO OS BANCO DE DADOS CRIADOS

# Conectar no BDODS
conexaoODS = sql.connect(BDODS)

# Criar a tabela tbLogMunic e carregar os dados do DF DadosIBGE
DadosIBGE.to_sql('tabelaLogMunic', conexaoODS, if_exists="append")

#Confirmar a transação
conexaoODS.commit()

#Fechar a conexão
conexaoODS.close()

print('Carga do BDODS concluída!')

#Conectar no BDDW
conexaoDW = sql.connect(BDDW)

#Selecionar somente as colunas para criação da dMunicipio
DadosIBGE = DadosIBGE[['Munic','Cod']]

#Criar a tabela dMunicipio e carregar os dados do DF DadosIBGE
DadosIBGE.to_sql('dMunicipio',conexaoDW,if_exists="replace")