import requests
import json
import time
from datetime import datetime
from loguru import logger
from random import randint
import os

BAIRROS = [
    'tres-marias',
    'centro',
    'aristocrata',
    'bom-jesus',
    'pedro-moro',
    'ouro-fino',
    'sao-pedro',
    'sao-domingos',
    'silveira-da-motta',
    'braga'
]

today = datetime.now().strftime("%d-%m-%Y")

directory = f"results/{today}"
os.makedirs(directory, exist_ok=True)


headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:141.0) Gecko/20100101 Firefox/141.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.chavesnamao.com.br/apartamentos-a-venda/pr-sao-jose-dos-pinhais/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Cookie': 'PHPSESSID=qos6s2tp3kdu9c7dip7rc5p043; cnm_loc=%7B%22id_cidade%22%3A%226015%22%2C%22cidade%22%3A%22Curitiba%22%2C%22uf_estado%22%3A%22PR%22%2C%22latitude%22%3A%22-25.432957%22%2C%22longitude%22%3A%22-49.271847%22%2C%22id_cidade_landing_page%22%3A%226015%22%2C%22nome_cidade_landing_page%22%3A%22Curitiba%22%2C%22url_cidade_landing_page%22%3A%22curitiba%22%2C%22uf_cidade_landing_page%22%3A%22PR%22%7D'
}

def save_bairro_batch(bairro, properties):
    if not properties:
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"results/{today}/imoveis_{bairro}_{timestamp}.json"

    data = {
        'bairro': bairro,
        'total_collected': len(properties),
        'collection_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'search_params': {
            'level1': 'apartamentos-a-venda',
            'level2': 'pr-sao-jose-dos-pinhais',
            'level3': bairro
        },
        'properties': properties,
        'crawled_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.success(f"Batch salvo: {filename}")
    return filename

def scrape_bairro(bairro):
    url = "https://www.chavesnamao.com.br/api/realestate/listing/items/"

    base_params = {
        'level1': 'apartamentos-a-venda',
        'level2': 'pr-sao-jose-dos-pinhais',
        'level3': bairro
    }

    logger.info(f"COLETANDO BAIRRO: {bairro.upper().replace('-', ' ')}")
    logger.info("-" * 50)

    bairro_properties = []
    page = 1

    try:
        response = requests.get(url, headers=headers, params=base_params)

        if response.status_code != 200:
            logger.error(f"Erro HTTP {response.status_code} para {bairro}")
            return []

        data = response.json()
        items = [item for item in data.get('items', []) if not item.get('pagination')]

        if not items:
            logger.warning(f"Nenhum imovel encontrado em {bairro}")
            return []

        bairro_properties.extend(items)

        metadata = data.get('metadata', {})
        total_results = metadata.get('realResults', 0)
        total_pages = metadata.get('totalPages', 1)

        logger.info(f"{bairro}: {total_results} imoveis em {total_pages} paginas")
        logger.info(f"Pagina {page}/{total_pages} - {len(items)} itens")

        for page in range(2, total_pages + 1):
            params = base_params.copy()
            params['pg'] = page

            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                logger.error(f"Erro HTTP {response.status_code} na pagina {page} de {bairro}")
                break

            data = response.json()
            items = [item for item in data.get('items', []) if not item.get('pagination')]

            if len(items) == 0:
                logger.warning(f"Pagina {page} vazia em {bairro}, finalizando")
                break

            bairro_properties.extend(items)
            logger.info(f"Pagina {page}/{total_pages} - {len(items)} itens")

            if total_results > 0 and len(bairro_properties) >= total_results:
                break

            time.sleep(randint(1,2))

        logger.success(f"{bairro}: coletados {len(bairro_properties)} imoveis")

        for prop in bairro_properties:
            prop['bairro_coleta'] = bairro

        save_bairro_batch(bairro, bairro_properties)

        return bairro_properties

    except Exception as e:
        logger.error(f"Erro coletando {bairro}: {e}")
        return bairro_properties

def scrape_all_bairros():
    logger.info("INICIANDO COLETA DE TODOS OS BAIRROS")
    logger.info("=" * 60)
    logger.info(f"Bairros: {', '.join([b.replace('-', ' ').title() for b in BAIRROS])}")

    all_properties = []
    bairros_summary = {}

    for i, bairro in enumerate(BAIRROS, 1):
        logger.info(f"[{i}/{len(BAIRROS)}]")

        bairro_properties = scrape_bairro(bairro)

        if bairro_properties:
            all_properties.extend(bairro_properties)
            bairros_summary[bairro] = len(bairro_properties)
        else:
            bairros_summary[bairro] = 0

        if i < len(BAIRROS):
            time.sleep(1)

    return all_properties, bairros_summary

def save_properties(properties, bairros_summary):
    if not properties:
        logger.error("Nenhum imovel para salvar")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"results/imoveis_sao_jose_pinhais_{timestamp}.json"

    data = {
        'total_collected': len(properties),
        'collection_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'search_params': {
            'level1': 'apartamentos-a-venda',
            'level2': 'pr-sao-jose-dos-pinhais',
            'bairros': BAIRROS
        },
        'bairros_summary': bairros_summary,
        'properties': properties,
        'crawled_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.success(f"Arquivo salvo: {filename}")
    return filename

def show_statistics(properties, bairros_summary):
    if not properties:
        return

    logger.info("=" * 60)
    logger.info("RESUMO FINAL")
    logger.info("=" * 60)

    logger.info("IMOVEIS POR BAIRRO:")
    logger.info("-" * 40)
    total_bairros_com_imoveis = 0
    for bairro, count in bairros_summary.items():
        status = "OK" if count > 0 else "VAZIO"
        if count > 0:
            total_bairros_com_imoveis += 1
        logger.info(f"{status} {bairro.replace('-', ' ').title()}: {count} imoveis")

    logger.info(f"Bairros com imoveis: {total_bairros_com_imoveis}/{len(BAIRROS)}")

    logger.info("ESTATISTICAS DE PRECOS:")
    logger.info("-" * 40)
    prices = [p.get('prices', {}).get('rawPrice') for p in properties if p.get('prices', {}).get('rawPrice')]
    if prices:
        logger.info(f"Menor preco: R$ {min(prices):,.2f}")
        logger.info(f"Maior preco: R$ {max(prices):,.2f}")
        logger.info(f"Preco medio: R$ {sum(prices)/len(prices):,.2f}")
        logger.info(f"Total de imoveis com preco: {len(prices)}")

    logger.info("DISTRIBUICAO DE QUARTOS:")
    logger.info("-" * 40)
    bedrooms = [p.get('properties', {}).get('bedrooms') for p in properties if p.get('properties', {}).get('bedrooms')]
    if bedrooms:
        bedroom_counts = {}
        for bed in bedrooms:
            bedroom_counts[bed] = bedroom_counts.get(bed, 0) + 1

        for bed_count in sorted(bedroom_counts.keys()):
            logger.info(f"{bed_count} quarto(s): {bedroom_counts[bed_count]} imoveis")

    logger.success(f"TOTAL GERAL: {len(properties)} imoveis coletados")

if __name__ == "__main__":
    start_time = time.time()

    properties, bairros_summary = scrape_all_bairros()

    end_time = time.time()
    duration = end_time - start_time

    if properties:
        logger.success("COLETA FINALIZADA!")
        logger.info(f"Tempo total: {duration/60:.1f} minutos")

        filename = save_properties(properties, bairros_summary)

        show_statistics(properties, bairros_summary)

    else:
        logger.error("Nenhum imovel coletado em nenhum bairro")
