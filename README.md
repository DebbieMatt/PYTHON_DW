# Data Warehouse com Integração de Dados e Atualização Retroativa

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-07405E?style=for-the-badge&logo=sqlite&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white)
![Status](https://img.shields.io/static/v1?label=STATUS&message=EM%20ANDAMENTO&color=green&style=for-the-badge)

> Sistema de Data Warehouse desenvolvido em Python com integração ao SQLite, utilizando técnicas de Web Scraping, ETL e modelagem dimensional. O projeto implementa extração automatizada de dados do IBGE com suporte a atualização retroativa e análise de indicadores econômicos.

## 📋 Índice

- [Sobre o Projeto](#-sobre-o-projeto)
- [Arquitetura](#-arquitetura)
- [Tecnologias Utilizadas](#-tecnologias-utilizadas)
- [Estrutura do Repositório](#-estrutura-do-repositório)
- [Instalação](#-instalação)
- [Como Usar](#-como-usar)
- [Modelagem de Dados](#-modelagem-de-dados)
- [Funcionalidades](#-funcionalidades)
- [Exemplos de Uso](#-exemplos-de-uso)
- [Contribuindo](#-contribuindo)
- [Licença](#-licença)

## 🎯 Sobre o Projeto

Este Data Warehouse foi desenvolvido como solução para análise de dados econômicos do estado de Mato Grosso, integrando informações do IBGE (Instituto Brasileiro de Geografia e Estatística). O sistema implementa conceitos avançados de Engenharia de Dados, incluindo:

- **Web Scraping**: Extração automatizada de dados do IBGE
- **ETL Pipeline**: Processos de Extração, Transformação e Carga
- **Modelagem Dimensional**: Esquema estrela (Star Schema)
- **Atualização Retroativa**: SCD Type 2 (Slowly Changing Dimensions)
- **ODS (Operational Data Store)**: Camada intermediária de dados operacionais

### Objetivos do Projeto

- Centralizar dados econômicos do estado de Mato Grosso
- Permitir análises históricas com rastreamento de mudanças
- Fornecer interface para consultas analíticas (OLAP)
- Automatizar a coleta e integração de dados do IBGE
- Implementar boas práticas de Data Warehousing

## 🏗️ Arquitetura

### Arquitetura em Camadas

```
┌─────────────────────────────────────────────────────┐
│              Fonte de Dados (IBGE)                  │
│        APIs REST | Web Pages | CSV Files            │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│           Camada de Extração (ETL)                  │
│   Web Scraping | Requests | BeautifulSoup           │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│        ODS - Operational Data Store                 │
│         (Dados Brutos - ODS.db)                     │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│      Camada de Transformação (ETL)                  │
│  Limpeza | Validação | Normalização | Agregação     │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│         Data Warehouse (DW.db)                      │
│    Modelagem Dimensional | Star Schema              │
│    Fato PIB + Dimensões (Tempo, Setor, Local)      │
└─────────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│         Camada de Apresentação                      │
│   Jupyter Notebook | Análises | Visualizações       │
└─────────────────────────────────────────────────────┘
```

## 🛠️ Tecnologias Utilizadas

### Linguagem e Ambiente
- **Python 3.8+**: Linguagem principal
- **Google Colab**: Ambiente de desenvolvimento Jupyter
- **Jupyter Notebook**: Interface de análise interativa

### Bibliotecas Python

#### Web Scraping e Requisições
```python
requests==2.28.0          # Requisições HTTP
beautifulsoup4==4.11.0    # Parse de HTML/XML
lxml==4.9.0               # Parser XML
selenium==4.4.0           # Automação web (se necessário)
```

#### Manipulação de Dados
```python
pandas==1.5.0             # Análise e manipulação de dados
numpy==1.23.0             # Computação numérica
```

#### Banco de Dados
```python
sqlite3                   # Banco de dados (built-in)
sqlalchemy==1.4.40        # ORM e abstração SQL
```

#### Visualização (Opcional)
```python
matplotlib==3.6.0         # Gráficos e visualizações
seaborn==0.12.0           # Visualizações estatísticas
plotly==5.10.0            # Gráficos interativos
```

### Banco de Dados
- **SQLite**: Sistema de gerenciamento de banco de dados relacional leve

## 📁 Estrutura do Repositório

```
Data-Warehouse-IBGE/
├── Integração_Dados_IBGE.ipynb    # Notebook principal do projeto
├── DW.db                          # Data Warehouse (modelagem dimensional)
├── ODS.db                         # Operational Data Store (staging)
├── PIB-MT.csv                     # Dataset: PIB do Mato Grosso
├── PIB-SETOR-MT.csv              # Dataset: PIB por setor econômico
├── requirements.txt               # Dependências Python
├── README.md                      # Documentação principal
├── CONTRIBUTING.md                # Guia de contribuição
├── LICENSE.md                     # Licença do projeto
└── docs/                          # Documentação adicional
    ├── modelo_dimensional.png     # Diagrama do modelo estrela
    ├── dicionario_dados.md        # Dicionário de dados
    └── guia_queries.md            # Exemplos de consultas SQL
```

## 🚀 Instalação

### Pré-requisitos

- Python 3.8 ou superior
- Conta Google (para usar Google Colab)
- Git instalado

### Opção 1: Google Colab (Recomendado)

1. **Acesse o Google Colab**: [colab.research.google.com](https://colab.research.google.com)

2. **Clone o repositório diretamente no Colab**:
```python
# Execute esta célula no Colab
!git clone https://github.com/DebbieMatt/Data-Warehouse-IBGE.git
%cd Data-Warehouse-IBGE
```

3. **Instale as dependências**:
```python
!pip install -r requirements.txt
```

4. **Abra o notebook principal**:
   - Navegue até `Integração_Dados_IBGE.ipynb`
   - Execute as células sequencialmente

### Opção 2: Ambiente Local

1. **Clone o repositório**:
```bash
git clone https://github.com/DebbieMatt/Data-Warehouse-IBGE.git
cd Data-Warehouse-IBGE
```

2. **Crie um ambiente virtual**:
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/macOS
python3 -m venv venv
source venv/bin/activate
```

3. **Instale as dependências**:
```bash
pip install -r requirements.txt
```

4. **Inicie o Jupyter Notebook**:
```bash
jupyter notebook
```

5. **Abra o arquivo** `Integração_Dados_IBGE.ipynb`

## 📖 Como Usar

### Pipeline ETL Completo

#### 1. Extração de Dados (Extract)

```python
import requests
import pandas as pd
from bs4 import BeautifulSoup

def extrair_dados_ibge(url_ibge):
    """
    Extrai dados do IBGE via web scraping
    """
    response = requests.get(url_ibge)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Parse dos dados
    dados = []
    # ... lógica de extração ...
    
    return pd.DataFrame(dados)

# Exemplo de uso
url = "https://sidra.ibge.gov.br/tabela/5938"
df_pib = extrair_dados_ibge(url)
print(f"Extraídos {len(df_pib)} registros")
```

#### 2. Carregamento no ODS (Load to Staging)

```python
import sqlite3

def carregar_ods(df, tabela_nome):
    """
    Carrega dados brutos no Operational Data Store
    """
    conn = sqlite3.connect('ODS.db')
    df.to_sql(tabela_nome, conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    print(f"Dados carregados na tabela ODS: {tabela_nome}")

# Carregar dados extraídos
carregar_ods(df_pib, 'staging_pib_mt')
```

#### 3. Transformação (Transform)

```python
def transformar_dados(df):
    """
    Limpa, valida e transforma dados
    """
    # Remover valores nulos
    df = df.dropna(subset=['valor_pib'])
    
    # Converter tipos
    df['ano'] = pd.to_datetime(df['ano'], format='%Y')
    df['valor_pib'] = pd.to_numeric(df['valor_pib'], errors='coerce')
    
    # Normalizar nomes
    df['municipio'] = df['municipio'].str.strip().str.title()
    
    # Criar chaves surrogate
    df['id_tempo'] = df['ano'].dt.year * 10000 + df['ano'].dt.month * 100
    
    return df

df_transformado = transformar_dados(df_pib)
```

#### 4. Carregamento no DW (Load to Warehouse)

```python
def criar_dimensoes_fatos():
    """
    Cria schema dimensional no Data Warehouse
    """
    conn = sqlite3.connect('DW.db')
    cursor = conn.cursor()
    
    # Dimensão Tempo
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_tempo (
            id_tempo INTEGER PRIMARY KEY,
            ano INTEGER NOT NULL,
            trimestre INTEGER,
            mes INTEGER,
            ano_mes TEXT
        )
    ''')
    
    # Dimensão Localização
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_localizacao (
            id_localizacao INTEGER PRIMARY KEY AUTOINCREMENT,
            municipio TEXT NOT NULL,
            codigo_ibge TEXT,
            regiao TEXT,
            uf TEXT DEFAULT 'MT'
        )
    ''')
    
    # Dimensão Setor Econômico
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dim_setor (
            id_setor INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_setor TEXT NOT NULL,
            categoria TEXT,
            descricao TEXT
        )
    ''')
    
    # Tabela Fato PIB
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fato_pib (
            id_fato INTEGER PRIMARY KEY AUTOINCREMENT,
            id_tempo INTEGER,
            id_localizacao INTEGER,
            id_setor INTEGER,
            valor_pib REAL NOT NULL,
            variacao_percentual REAL,
            valor_per_capita REAL,
            data_carga DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_tempo) REFERENCES dim_tempo(id_tempo),
            FOREIGN KEY (id_localizacao) REFERENCES dim_localizacao(id_localizacao),
            FOREIGN KEY (id_setor) REFERENCES dim_setor(id_setor)
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Schema dimensional criado com sucesso!")

criar_dimensoes_fatos()
```

## 🗄️ Modelagem de Dados

### Esquema Estrela (Star Schema)

```
                  ┌─────────────────┐
                  │   dim_tempo     │
                  ├─────────────────┤
                  │ id_tempo (PK)   │
                  │ ano             │
                  │ trimestre       │
                  │ mes             │
                  └────────┬────────┘
                           │
                           │
       ┌───────────────────┼───────────────────┐
       │                   │                   │
┌──────▼─────────┐  ┌─────▼──────────┐  ┌────▼──────────────┐
│ dim_localizacao│  │   fato_pib     │  │   dim_setor       │
├────────────────┤  ├────────────────┤  ├───────────────────┤
│id_localizacao  │◄─┤id_tempo (FK)   │─►│ id_setor (PK)     │
│municipio       │  │id_localizacao  │  │ nome_setor        │
│codigo_ibge     │  │id_setor (FK)   │  │ categoria         │
│regiao          │  │valor_pib       │  │ descricao         │
│uf              │  │variacao_%      │  └───────────────────┘
└────────────────┘  │valor_per_capita│
                    │data_carga      │
                    └────────────────┘
```

### Dicionário de Dados

#### Tabela: `fato_pib`
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| id_fato | INTEGER | Chave primária |
| id_tempo | INTEGER | FK para dim_tempo |
| id_localizacao | INTEGER | FK para dim_localizacao |
| id_setor | INTEGER | FK para dim_setor |
| valor_pib | REAL | Valor do PIB em milhões de reais |
| variacao_percentual | REAL | Variação em relação ao período anterior |
| valor_per_capita | REAL | PIB per capita |
| data_carga | DATETIME | Timestamp da carga |

#### Tabela: `dim_tempo`
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| id_tempo | INTEGER | Chave primária (formato: YYYYMMDD) |
| ano | INTEGER | Ano de referência |
| trimestre | INTEGER | Trimestre (1-4) |
| mes | INTEGER | Mês (1-12) |
| ano_mes | TEXT | Formato texto YYYY-MM |

## ✨ Funcionalidades

### 1. Web Scraping Automatizado
- Extração de dados do SIDRA/IBGE
- Parsing de tabelas HTML
- Tratamento de paginação
- Retry automático em caso de falha

### 2. Atualização Retroativa (SCD Type 2)
```python
def atualizar_dimensao_scd2(df_novos_dados, tabela_dim):
    """
    Implementa Slowly Changing Dimension Type 2
    Mantém histórico de mudanças
    """
    conn = sqlite3.connect('DW.db')
    
    # Adicionar colunas de versionamento
    df_novos_dados['data_inicio'] = pd.Timestamp.now()
    df_novos_dados['data_fim'] = pd.Timestamp('2999-12-31')
    df_novos_dados['versao_atual'] = True
    
    # Desativar registros antigos
    cursor = conn.cursor()
    cursor.execute(f'''
        UPDATE {tabela_dim}
        SET data_fim = ?, versao_atual = 0
        WHERE versao_atual = 1
    ''', (pd.Timestamp.now(),))
    
    # Inserir novos registros
    df_novos_dados.to_sql(tabela_dim, conn, if_exists='append', index=False)
    
    conn.commit()
    conn.close()
```

### 3. Análises Pré-Configuradas

#### Análise Temporal
```python
def analise_evolucao_pib(ano_inicio, ano_fim):
    """
    Analisa evolução do PIB no período
    """
    query = f'''
        SELECT 
            t.ano,
            t.trimestre,
            SUM(f.valor_pib) as pib_total,
            AVG(f.variacao_percentual) as variacao_media
        FROM fato_pib f
        JOIN dim_tempo t ON f.id_tempo = t.id_tempo
        WHERE t.ano BETWEEN {ano_inicio} AND {ano_fim}
        GROUP BY t.ano, t.trimestre
        ORDER BY t.ano, t.trimestre
    '''
    
    conn = sqlite3.connect('DW.db')
    df_resultado = pd.read_sql_query(query, conn)
    conn.close()
    
    return df_resultado
```

#### Análise por Setor
```python
def analise_pib_por_setor(ano):
    """
    Analisa contribuição de cada setor no PIB
    """
    query = f'''
        SELECT 
            s.nome_setor,
            s.categoria,
            SUM(f.valor_pib) as valor_total,
            ROUND(SUM(f.valor_pib) * 100.0 / 
                  (SELECT SUM(valor_pib) FROM fato_pib 
                   WHERE id_tempo LIKE '{ano}%'), 2) as percentual
        FROM fato_pib f
        JOIN dim_setor s ON f.id_setor = s.id_setor
        JOIN dim_tempo t ON f.id_tempo = t.id_tempo
        WHERE t.ano = {ano}
        GROUP BY s.id_setor
        ORDER BY valor_total DESC
    '''
    
    conn = sqlite3.connect('DW.db')
    df_resultado = pd.read_sql_query(query, conn)
    conn.close()
    
    return df_resultado
```

## 💡 Exemplos de Uso

### Exemplo 1: Pipeline ETL Completo

```python
# 1. Extração
print("Iniciando extração de dados...")
df_pib = extrair_dados_ibge("https://sidra.ibge.gov.br/tabela/5938")
df_setor = pd.read_csv('PIB-SETOR-MT.csv')

# 2. Carga no ODS
print("Carregando dados no ODS...")
carregar_ods(df_pib, 'staging_pib')
carregar_ods(df_setor, 'staging_setor')

# 3. Transformação
print("Transformando dados...")
df_pib_limpo = transformar_dados(df_pib)
df_setor_limpo = transformar_dados(df_setor)

# 4. Carga no DW
print("Carregando no Data Warehouse...")
criar_dimensoes_fatos()
carregar_dimensoes(df_pib_limpo, df_setor_limpo)
carregar_fatos(df_pib_limpo)

print("Pipeline ETL concluído com sucesso!")
```

### Exemplo 2: Consulta Analítica

```python
import matplotlib.pyplot as plt

# Análise de evolução do PIB
df_evolucao = analise_evolucao_pib(2010, 2023)

# Visualização
plt.figure(figsize=(12, 6))
plt.plot(df_evolucao['ano'], df_evolucao['pib_total'], marker='o')
plt.title('Evolução do PIB de Mato Grosso (2010-2023)')
plt.xlabel('Ano')
plt.ylabel('PIB (Milhões de R$)')
plt.grid(True)
plt.show()

# Análise setorial
df_setores = analise_pib_por_setor(2023)
print("\nParticipação por Setor (2023):")
print(df_setores)
```

### Exemplo 3: Atualização de Dados

```python
# Buscar novos dados do IBGE
df_novos = extrair_dados_ibge_atualizados()

# Atualizar dimensões com histórico
atualizar_dimensao_scd2(df_novos, 'dim_localizacao')

# Inserir novos fatos
carregar_fatos(df_novos)

print("Dados atualizados mantendo histórico!")
```

## 🤝 Contribuindo

Contribuições são muito bem-vindas! Este projeto pode ser expandido de várias formas.

### Como Contribuir

1. **Fork este repositório**

2. **Clone seu fork:**
   ```bash
   git clone https://github.com/seu-usuario/Data-Warehouse-IBGE.git
   cd Data-Warehouse-IBGE
   ```

3. **Crie uma branch:**
   ```bash
   git checkout -b feature/nova-funcionalidade
   ```

4. **Faça suas alterações e commit:**
   ```bash
   git add .
   git commit -m 'feat: adiciona análise de correlação entre setores'
   ```

5. **Push para sua branch:**
   ```bash
   git push origin feature/nova-funcionalidade
   ```

6. **Abra um Pull Request**

### Áreas para Contribuição

- 📊 **Novas Análises**: Implementar queries analíticas adicionais
- 🌐 **Mais Fontes de Dados**: Integrar outros datasets do IBGE
- 📈 **Visualizações**: Criar dashboards com Plotly/Dash
- 🔄 **Automação**: Implementar agendamento de ETL (Airflow, Cron)
- 🧪 **Testes**: Adicionar testes unitários e de integração
- 📝 **Documentação**: Melhorar documentação e tutoriais
- 🚀 **Performance**: Otimizar queries e processos ETL
- 🔐 **Segurança**: Implementar validação e sanitização de dados

### Diretrizes de Código

```python
# Padrão de nomenclatura
def extrair_dados_fonte():  # snake_case para funções
    """
    Docstring explicando a função
    
    Returns:
        DataFrame com os dados extraídos
    """
    pass

class GerenciadorETL:  # PascalCase para classes
    """Docstring da classe"""
    pass

CONSTANTE_GLOBAL = "valor"  # UPPER_CASE para constantes
```

Para mais detalhes, consulte [CONTRIBUTING.md](CONTRIBUTING.md)

## 📚 Recursos Adicionais

### Documentação IBGE
- [SIDRA - Sistema IBGE de Recuperação Automática](https://sidra.ibge.gov.br/)
- [API SIDRA](https://apisidra.ibge.gov.br/)
- [Metadados das Pesquisas](https://www.ibge.gov.br/estatisticas/economicas/)

### Tutoriais e Guias
- [Modelagem Dimensional - Kimball](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/)
- [Slowly Changing Dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
- [ETL Best Practices](https://www.stitchdata.com/etldatabase/etl-best-practices/)

### Ferramentas Complementares
- **Apache Airflow**: Orquestração de workflows ETL
- **DBT (Data Build Tool)**: Transformações SQL
- **Metabase**: BI e visualização open-source
- **Great Expectations**: Validação de qualidade de dados

## 📝 Licença

Este projeto está sob a licença MIT. Consulte o arquivo [LICENSE.md](LICENSE.md) para mais informações.

---

<div align="center">

**[⬆ Voltar ao topo](#data-warehouse-com-integração-de-dados-e-atualização-retroativa)**

Desenvolvido com 💙 por [Débora Mateus](https://github.com/DebbieMatt)

*"Transformando dados em conhecimento, um pipeline por vez"* 📊

</div>
