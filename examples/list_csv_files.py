#!/usr/bin/env python3
"""
Script para listar arquivos CSV do ONS.
"""

import logging
from app.data.aws_manager import ONSDataManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Função principal para listar arquivos CSV."""
    try:
        # Inicializar ONSDataManager com o caminho base correto
        ons_manager = ONSDataManager(
            bucket_name='ons-aws-prod-opendata',
            base_path='dataset/geracao_usina_2_ho/',
            cache_dir='./cache'
        )
        
        # Forçar atualização da lista de arquivos
        logger.info("Listando arquivos disponíveis...")
        files = ons_manager.list_available_files(refresh=True)
        
        # Filtrar apenas arquivos CSV
        csv_files = [f for f in files if f['key'].lower().endswith('.csv')]
        
        if csv_files:
            logger.info("\nArquivos CSV encontrados:")
            for file in csv_files:
                logger.info(f"- {file['key']}")
                logger.info(f"  Tamanho: {file['size']} bytes")
                logger.info(f"  Última modificação: {file['last_modified']}")
        else:
            logger.warning("Nenhum arquivo CSV encontrado no caminho especificado!")
            
        # Mostrar todos os arquivos para debug
        logger.info("\nTodos os arquivos encontrados (para referência):")
        for file in files:
            logger.info(f"- {file['key']}")

    except Exception as e:
        logger.error(f"Erro ao listar arquivos: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 