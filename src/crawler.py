import requests
import json
import time
import yaml
from datetime import datetime
from loguru import logger
from pathlib import Path
import os

# Carrega configuração do YAML
def load_config(config_file='../config.yaml'):
    with open(config_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

# Headers para requisições
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:141.0) Gecko/20100101 Firefox/141.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.chavesnamao.com.br/apartamentos-a-venda/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}

def save_batch(estado, cidade, bairro, properties):
    """Salva os dados coletados na estrutura ../data/raw/chavesnamao/"""
    if not properties:
        return None

    # Cria estrutura de diretórios
    today = datetime.now().strftime("%Y-%m-%d")
    base_dir = Path(f"../data/raw/chavesnamao/dt={today}")
    base_dir.mkdir(parents=True, exist_ok=True)

    # Nome do arquivo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = base_dir / f"{estado}_{cidade}_{bairro}_{timestamp}.json"

    # Estrutura de dados mantendo o formato original
    data = {
        'estado': estado,
        'cidade': cidade,
        'bairro': bairro,
        'total_collected': len(properties),
        'collection_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'search_params': {
            'level1': 'apartamentos-a-venda',
            'level2': f'{estado}-{cidade}',
            'level3': bairro
        },
        'properties': properties,
        'crawled_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # Salva arquivo
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.success(f"Salvo: {filename}")
    return filename

def scrape_bairro(estado, cidade, bairro):
    """Coleta dados de um bairro específico"""
    url = "https://www.chavesnamao.com.br/api/realestate/listing/items/"

    # Parâmetros da API
    params = {
        'level1': 'apartamentos-a-venda',
        'level2': f"{estado}-{cidade}",
        'level3': bairro
    }

    all_items = []
    page = 1

    try:
        # Primeira página
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            logger.error(f"Erro HTTP {response.status_code}")
            return []

        data = response.json()
        items = [item for item in data.get('items', []) if not item.get('pagination')]

        if not items:
            logger.warning(f"Nenhum imóvel encontrado")
            return []

        all_items.extend(items)

        # Metadata
        metadata = data.get('metadata', {})
        total_pages = metadata.get('totalPages', 1)
        total_results = metadata.get('realResults', 0)

        logger.info(f"Encontrados: {total_results} imóveis em {total_pages} páginas")

        # Páginas seguintes
        for page in range(2, total_pages + 1):
            params['pg'] = page

            response = requests.get(url, headers=HEADERS, params=params)
            if response.status_code != 200:
                logger.error(f"Erro na página {page}")
                break

            data = response.json()
            items = [item for item in data.get('items', []) if not item.get('pagination')]

            if not items:
                break

            all_items.extend(items)
            logger.info(f"  Página {page}/{total_pages} - {len(items)} itens")

            time.sleep(1)  # Rate limiting

        logger.success(f"Total coletado: {len(all_items)} imóveis")

        # Adiciona metadados em cada item (opcional, mantém compatibilidade)
        for item in all_items:
            item['meta_estado'] = estado
            item['meta_cidade'] = cidade
            item['meta_bairro'] = bairro

        # Salva o batch
        save_batch(estado, cidade, bairro, all_items)

        return all_items

    except Exception as e:
        logger.error(f"Erro: {e}")
        return []

def main():
    """Função principal"""
    logger.info("=" * 60)
    logger.info("INICIANDO COLETA CHAVES NA MÃO")
    logger.info("=" * 60)

    # Carrega configuração
    config = load_config()

    # Contadores totais
    total_collected = 0
    start_time = time.time()

    # Calcula totais para acompanhamento
    total_estados = len(config)
    total_cidades = 0
    total_bairros = 0

    for estado, cidades in config.items():
        total_cidades += len(cidades)
        for cidade, bairros in cidades.items():
            total_bairros += len(bairros)

    logger.info(f"Total a processar: {total_estados} estados, {total_cidades} cidades, {total_bairros} bairros")
    logger.info("=" * 60)

    estado_num = 0
    cidade_global = 0
    bairro_global = 0

    # Itera por estados
    for estado, cidades in config.items():
        estado_num += 1
        logger.info(f"\n[ESTADO {estado_num}/{total_estados}] {estado.upper()}")
        logger.info("-" * 40)

        cidade_num = 0
        # Itera por cidades
        for cidade, bairros in cidades.items():
            cidade_num += 1
            cidade_global += 1
            logger.info(f"\n[CIDADE {cidade_global}/{total_cidades}] {cidade.upper()} ({cidade_num}/{len(cidades)} no estado)")
            logger.info(f"Bairros nesta cidade: {len(bairros)}")

            bairro_num = 0
            # Itera por bairros
            for bairro in bairros:
                bairro_num += 1
                bairro_global += 1

                logger.info(f"\n[BAIRRO {bairro_global}/{total_bairros}] {bairro.upper()} ({bairro_num}/{len(bairros)} na cidade)")

                properties = scrape_bairro(estado, cidade, bairro)
                total_collected += len(properties)

                if properties:
                    logger.info(f"✓ {len(properties)} imóveis coletados")
                else:
                    logger.info(f"✗ Nenhum imóvel encontrado")

                # Mostra progresso
                faltam_bairros = total_bairros - bairro_global
                if faltam_bairros > 0:
                    logger.info(f"→ Faltam {faltam_bairros} bairros")

                time.sleep(2)  # Rate limiting entre bairros

    # Estatísticas finais
    duration = (time.time() - start_time) / 60
    logger.info("\n" + "=" * 60)
    logger.success(f"COLETA FINALIZADA!")
    logger.info(f"Estados processados: {total_estados}")
    logger.info(f"Cidades processadas: {total_cidades}")
    logger.info(f"Bairros processados: {total_bairros}")
    logger.info(f"Total de imóveis coletados: {total_collected}")
    logger.info(f"Tempo total: {duration:.1f} minutos")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
