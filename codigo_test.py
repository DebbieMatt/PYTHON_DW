"""
Data Warehouse IBGE - Pipeline ETL Melhorado
Autor: Débora Mateus
Descrição: Sistema completo de ETL para dados do IBGE com validação,
           logging e tratamento de erros robusto
"""

import pandas as pd
import sqlite3 as sql
from datetime import datetime
from pathlib import Path
import logging
from typing import Optional, Dict, Tuple
import sys

# ==================== CONFIGURAÇÃO DE LOGGING ====================
def configurar_logging(nivel=logging.INFO):
    """
    Configura sistema de logging para rastreamento do pipeline
    """
    logging.basicConfig(
        level=nivel,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('etl_pipeline.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)

logger = configurar_logging()


# ==================== CONFIGURAÇÕES GLOBAIS ====================
class ConfigETL:
    """Classe de configuração centralizada"""
    
    # Caminhos
    BASE_DIR = Path('/content/drive/MyDrive/Colab Notebooks')
    BDODS_PATH = BASE_DIR / "ODS.db"
    BDDW_PATH = BASE_DIR / "DW.db"
    
    # URLs
    URL_IBGE_MUNICIPIOS = 'https://www.ibge.gov.br/explica/codigos-dos-municipios.php#MT'
    
    # Arquivos CSV
    CSV_PIB_SETOR = "PIB-SETOR-MT.csv"
    CSV_PIB_MT = "PIB-MT.csv"
    
    # Nomes de tabelas
    TABELA_LOG_MUNIC = "tabelaLogMunic"
    TABELA_DIM_MUNIC = "dMunicipio"
    TABELA_LOG_PIB_SETOR = "tbLogPIBsetor"
    TABELA_LOG_PIB_MT = "tbLogPIBmt"
    
    @staticmethod
    def obter_timestamp():
        """Retorna timestamp formatado para carga"""
        return datetime.today().strftime('%d/%m/%Y %H:%M:%S')


# ==================== EXTRAÇÃO DE DADOS ====================
class ExtractorIBGE:
    """Classe responsável pela extração de dados do IBGE"""
    
    @staticmethod
    def extrair_municipios(url: str) -> Optional[pd.DataFrame]:
        """
        Extrai dados de municípios do IBGE via web scraping
        
        Args:
            url: URL da página do IBGE
            
        Returns:
            DataFrame com dados dos municípios ou None em caso de erro
        """
        try:
            logger.info(f"Iniciando extração de municípios: {url}")
            
            df_municipios = pd.read_html(url, match="Municípios de Mato Grosso")[0]
            
            # Renomear colunas
            df_municipios = df_municipios.rename(columns={
                'Municípios de Mato Grosso': 'Munic',
                'Códigos': 'Cod'
            })
            
            # Configurar índice
            df_municipios.index.name = 'ID'
            df_municipios.index = df_municipios.index + 1
            
            # Adicionar timestamp de carga
            df_municipios['dataCarga'] = ConfigETL.obter_timestamp()
            
            logger.info(f"✓ Extraídos {len(df_municipios)} municípios com sucesso")
            return df_municipios
            
        except Exception as e:
            logger.error(f"✗ Erro ao extrair municípios: {str(e)}")
            return None
    
    @staticmethod
    def extrair_csv(caminho_arquivo: str) -> Optional[pd.DataFrame]:
        """
        Extrai dados de arquivo CSV
        
        Args:
            caminho_arquivo: Caminho completo do arquivo CSV
            
        Returns:
            DataFrame com dados do CSV ou None em caso de erro
        """
        try:
            logger.info(f"Iniciando leitura do CSV: {caminho_arquivo}")
            
            if not Path(caminho_arquivo).exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")
            
            df = pd.read_csv(caminho_arquivo, encoding='utf-8')
            df['dtCarga'] = ConfigETL.obter_timestamp()
            
            logger.info(f"✓ CSV carregado: {len(df)} registros")
            return df
            
        except Exception as e:
            logger.error(f"✗ Erro ao ler CSV: {str(e)}")
            return None


# ==================== TRANSFORMAÇÃO DE DADOS ====================
class TransformadorDados:
    """Classe responsável pela transformação e validação de dados"""
    
    @staticmethod
    def validar_dataframe(df: pd.DataFrame, colunas_obrigatorias: list) -> bool:
        """
        Valida se DataFrame possui todas as colunas obrigatórias
        
        Args:
            df: DataFrame a ser validado
            colunas_obrigatorias: Lista de colunas que devem existir
            
        Returns:
            True se válido, False caso contrário
        """
        colunas_faltantes = set(colunas_obrigatorias) - set(df.columns)
        
        if colunas_faltantes:
            logger.error(f"Colunas obrigatórias faltantes: {colunas_faltantes}")
            return False
        
        return True
    
    @staticmethod
    def limpar_dados_municipios(df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e valida dados de municípios
        
        Args:
            df: DataFrame com dados brutos
            
        Returns:
            DataFrame limpo
        """
        try:
            # Remover valores nulos
            df = df.dropna(subset=['Munic', 'Cod'])
            
            # Normalizar nomes de municípios
            df['Munic'] = df['Munic'].str.strip().str.title()
            
            # Validar códigos IBGE (devem ter 7 dígitos)
            df['Cod'] = df['Cod'].astype(str).str.zfill(7)
            
            # Remover duplicatas
            df = df.drop_duplicates(subset=['Cod'])
            
            logger.info(f"✓ Dados de municípios limpos: {len(df)} registros válidos")
            return df
            
        except Exception as e:
            logger.error(f"✗ Erro ao limpar dados: {str(e)}")
            return df
    
    @staticmethod
    def preparar_dimensao(df: pd.DataFrame, colunas: list) -> pd.DataFrame:
        """
        Prepara dados para carregamento na dimensão
        
        Args:
            df: DataFrame original
            colunas: Lista de colunas a manter
            
        Returns:
            DataFrame preparado para dimensão
        """
        return df[colunas].copy()


# ==================== CARGA DE DADOS ====================
class LoaderDW:
    """Classe responsável pela carga de dados nos bancos"""
    
    @staticmethod
    def criar_bancos() -> Tuple[bool, bool]:
        """
        Cria bancos de dados ODS e DW se não existirem
        
        Returns:
            Tupla (ods_criado, dw_criado)
        """
        try:
            if not ConfigETL.BASE_DIR.exists():
                logger.error(f"✗ Diretório base não existe: {ConfigETL.BASE_DIR}")
                return False, False
            
            ods_existe = ConfigETL.BDODS_PATH.exists()
            dw_existe = ConfigETL.BDDW_PATH.exists()
            
            if not ods_existe:
                ConfigETL.BDODS_PATH.touch()
                logger.info("✓ Banco ODS.db criado")
            
            if not dw_existe:
                ConfigETL.BDDW_PATH.touch()
                logger.info("✓ Banco DW.db criado")
            
            if ods_existe and dw_existe:
                logger.info("✓ Bancos de dados já existem")
            
            return True, True
            
        except Exception as e:
            logger.error(f"✗ Erro ao criar bancos: {str(e)}")
            return False, False
    
    @staticmethod
    def carregar_ods(df: pd.DataFrame, tabela: str) -> bool:
        """
        Carrega dados no ODS (Operational Data Store)
        
        Args:
            df: DataFrame com dados a carregar
            tabela: Nome da tabela de destino
            
        Returns:
            True se bem-sucedido, False caso contrário
        """
        conexao = None
        try:
            logger.info(f"Carregando {len(df)} registros na tabela ODS: {tabela}")
            
            conexao = sql.connect(ConfigETL.BDODS_PATH)
            df.to_sql(tabela, conexao, if_exists="append", index=True)
            conexao.commit()
            
            logger.info(f"✓ Carga ODS concluída: {tabela}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Erro ao carregar ODS: {str(e)}")
            if conexao:
                conexao.rollback()
            return False
            
        finally:
            if conexao:
                conexao.close()
    
    @staticmethod
    def carregar_dw(df: pd.DataFrame, tabela: str, if_exists: str = "replace") -> bool:
        """
        Carrega dados no Data Warehouse
        
        Args:
            df: DataFrame com dados a carregar
            tabela: Nome da tabela de destino
            if_exists: Comportamento se tabela existir ('replace', 'append', 'fail')
            
        Returns:
            True se bem-sucedido, False caso contrário
        """
        conexao = None
        try:
            logger.info(f"Carregando {len(df)} registros na tabela DW: {tabela}")
            
            conexao = sql.connect(ConfigETL.BDDW_PATH)
            df.to_sql(tabela, conexao, if_exists=if_exists, index=True)
            conexao.commit()
            
            logger.info(f"✓ Carga DW concluída: {tabela}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Erro ao carregar DW: {str(e)}")
            if conexao:
                conexao.rollback()
            return False
            
        finally:
            if conexao:
                conexao.close()


# ==================== PIPELINE ETL PRINCIPAL ====================
class PipelineETL:
    """Orquestrador principal do pipeline ETL"""
    
    def __init__(self):
        self.extractor = ExtractorIBGE()
        self.transformer = TransformadorDados()
        self.loader = LoaderDW()
        self.metricas = {
            'inicio': datetime.now(),
            'registros_processados': 0,
            'erros': 0
        }
    
    def executar_pipeline_municipios(self) -> bool:
        """
        Executa pipeline completo para dados de municípios
        
        Returns:
            True se bem-sucedido, False caso contrário
        """
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE: Municípios IBGE")
        logger.info("=" * 60)
        
        try:
            # 1. EXTRACT
            df_municipios = self.extractor.extrair_municipios(
                ConfigETL.URL_IBGE_MUNICIPIOS
            )
            if df_municipios is None:
                return False
            
            # 2. TRANSFORM
            df_limpo = self.transformer.limpar_dados_municipios(df_municipios)
            df_dimensao = self.transformer.preparar_dimensao(
                df_limpo, ['Munic', 'Cod']
            )
            
            # 3. LOAD
            # Carregar no ODS (dados completos com log)
            sucesso_ods = self.loader.carregar_ods(
                df_limpo, ConfigETL.TABELA_LOG_MUNIC
            )
            
            # Carregar no DW (apenas dimensão)
            sucesso_dw = self.loader.carregar_dw(
                df_dimensao, ConfigETL.TABELA_DIM_MUNIC
            )
            
            self.metricas['registros_processados'] += len(df_municipios)
            
            return sucesso_ods and sucesso_dw
            
        except Exception as e:
            logger.error(f"✗ Erro no pipeline de municípios: {str(e)}")
            self.metricas['erros'] += 1
            return False
    
    def executar_pipeline_pib(self) -> bool:
        """
        Executa pipeline completo para dados de PIB
        
        Returns:
            True se bem-sucedido, False caso contrário
        """
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE: PIB Mato Grosso")
        logger.info("=" * 60)
        
        try:
            # Pipeline PIB por Setor
            caminho_pib_setor = str(ConfigETL.BASE_DIR / ConfigETL.CSV_PIB_SETOR)
            df_pib_setor = self.extractor.extrair_csv(caminho_pib_setor)
            
            if df_pib_setor is not None:
                sucesso_setor = self.loader.carregar_ods(
                    df_pib_setor, ConfigETL.TABELA_LOG_PIB_SETOR
                )
                self.metricas['registros_processados'] += len(df_pib_setor)
            else:
                sucesso_setor = False
            
            # Pipeline PIB MT
            caminho_pib_mt = str(ConfigETL.BASE_DIR / ConfigETL.CSV_PIB_MT)
            df_pib_mt = self.extractor.extrair_csv(caminho_pib_mt)
            
            if df_pib_mt is not None:
                sucesso_mt = self.loader.carregar_ods(
                    df_pib_mt, ConfigETL.TABELA_LOG_PIB_MT
                )
                self.metricas['registros_processados'] += len(df_pib_mt)
            else:
                sucesso_mt = False
            
            return sucesso_setor and sucesso_mt
            
        except Exception as e:
            logger.error(f"✗ Erro no pipeline de PIB: {str(e)}")
            self.metricas['erros'] += 1
            return False
    
    def executar_completo(self) -> Dict:
        """
        Executa pipeline ETL completo
        
        Returns:
            Dicionário com métricas de execução
        """
        logger.info("🚀 INICIANDO PIPELINE ETL COMPLETO")
        logger.info(f"Timestamp: {ConfigETL.obter_timestamp()}")
        
        # Criar bancos se necessário
        self.loader.criar_bancos()
        
        # Executar pipelines
        sucesso_municipios = self.executar_pipeline_municipios()
        sucesso_pib = self.executar_pipeline_pib()
        
        # Calcular métricas finais
        self.metricas['fim'] = datetime.now()
        self.metricas['duracao'] = (
            self.metricas['fim'] - self.metricas['inicio']
        ).total_seconds()
        self.metricas['sucesso_geral'] = sucesso_municipios and sucesso_pib
        
        # Log final
        logger.info("=" * 60)
        logger.info("PIPELINE ETL CONCLUÍDO")
        logger.info("=" * 60)
        logger.info(f"Status: {'✓ SUCESSO' if self.metricas['sucesso_geral'] else '✗ FALHA'}")
        logger.info(f"Registros processados: {self.metricas['registros_processados']}")
        logger.info(f"Erros encontrados: {self.metricas['erros']}")
        logger.info(f"Duração: {self.metricas['duracao']:.2f} segundos")
        logger.info("=" * 60)
        
        return self.metricas


# ==================== EXECUÇÃO ====================
if __name__ == "__main__":
    """
    Ponto de entrada principal do pipeline
    """
    try:
        # Inicializar e executar pipeline
        pipeline = PipelineETL()
        metricas = pipeline.executar_completo()
        
        # Exibir resumo
        print("\n" + "=" * 60)
        print("RESUMO DA EXECUÇÃO")
        print("=" * 60)
        print(f"✓ Registros processados: {metricas['registros_processados']}")
        print(f"✓ Tempo total: {metricas['duracao']:.2f}s")
        print(f"✓ Status: {'SUCESSO' if metricas['sucesso_geral'] else 'FALHA'}")
        print("=" * 60)
        
    except KeyboardInterrupt:
        logger.warning("⚠ Pipeline interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"✗ Erro crítico no pipeline: {str(e)}")
        sys.exit(1)
