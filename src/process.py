import json
import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyarrow as pa  # noqa: F401
import pyarrow.parquet as pq  # noqa: F401
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Text, DateTime, Float, BigInteger, Boolean
from dotenv import load_dotenv, find_dotenv
from loguru import logger

load_dotenv(find_dotenv())

POSTGRESQL_USER = os.getenv('POSTGRESQL_USER')
POSTGRESQL_PASSWORD = os.getenv('POSTGRESQL_PASSWORD')
POSTGRESQL_HOST = os.getenv('POSTGRESQL_HOST')
POSTGRESQL_PORT = os.getenv('POSTGRESQL_PORT')
POSTGRESQL_DATABASE = os.getenv('POSTGRESQL_DATABASE')


def get_postgres_engine():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRESQL_USER}:{POSTGRESQL_PASSWORD}@"
            f"{POSTGRESQL_HOST}:{POSTGRESQL_PORT}/{POSTGRESQL_DATABASE}"
        )
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {e}")
        return None


def extract_property_features(prop):
    return {
        'id': prop.get('id'),
        'title': prop.get('title'),
        'reference': prop.get('reference'),
        'description': prop.get('description'),
        'category': prop.get('category'),
        'total_area': float(prop.get('area', {}).get('total')) if prop.get('area', {}).get('total') else None,
        'useful_area': float(prop.get('area', {}).get('useful')) if prop.get('area', {}).get('useful') else None,
        'price': prop.get('prices', {}).get('rawPrice'),
        'bedrooms': prop.get('properties', {}).get('bedrooms'),
        'bathrooms': prop.get('bathrooms', {}).get('count'),
        'suites': prop.get('suites', {}).get('count'),
        'garages': prop.get('garages', {}).get('count'),
        'street_name': prop.get('location', {}).get('street', {}).get('name'),
        'address_number': prop.get('location', {}).get('street', {}).get('addressNumber'),
        'address_comp': prop.get('location', {}).get('street', {}).get('addressComp'),
        'neighborhood': prop.get('location', {}).get('neighborhood', {}).get('name'),
        'city': prop.get('location', {}).get('city', {}).get('name'),
        'state': prop.get('location', {}).get('state', {}).get('name'),
        'latitude': float(prop.get('location', {}).get('geoposition', {}).get('lat')) if prop.get('location', {}).get('geoposition', {}).get('lat') else None,
        'longitude': float(prop.get('location', {}).get('geoposition', {}).get('lon')) if prop.get('location', {}).get('geoposition', {}).get('lon') else None,
        'publisher_name': prop.get('publisher', {}).get('name'),
        'publisher_phone': prop.get('publisher', {}).get('phones', {}).get('cellphone', {}).get('number'),
        'privative_items': json.dumps([item.get('name') for item in prop.get('privativeItems', []) if item.get('name')]),
        'estado': prop.get('estado'),
        'cidade': prop.get('cidade'),
        'bairro': prop.get('bairro'),
        'updated_at': pd.to_datetime(prop.get('updatedAt'), errors='coerce')
    }


def process_json_to_parquet(json_path, output_base: Path):
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        estado = data.get('estado')
        cidade = data.get('cidade')
        bairro = data.get('bairro')
        updated_at = data.get('updated_at')
        crawled_at = data.get('crawled_at')

        dt = pd.to_datetime(crawled_at).strftime('%Y-%m-%d')

        properties = []
        for prop in data.get('properties', []):
            row = extract_property_features(prop)
            row['crawled_at'] = crawled_at
            properties.append(row)

        if not properties:
            logger.warning(f"No properties in {json_path.name}")
            return None

        df = pd.DataFrame(properties).drop_duplicates(subset=['id'], keep='last')

        output_path = output_base / f"estado={estado}" / f"municipio={cidade}" / f"bairro={bairro}" / f"dt={dt}"
        output_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_file = output_path / f"data_{timestamp}.parquet"

        df.to_parquet(parquet_file, engine='pyarrow', compression='snappy', index=False)

        logger.success(f"Saved {len(df)} properties to {parquet_file}")
        return df

    except Exception as e:
        logger.error(f"Error processing {json_path}: {e}")
        return None


def process_all_raw_data():
    raw_base = Path("../data/raw/chavesnamao")
    trusted_base = Path("../data/trusted/chavesnamao")

    if not raw_base.exists():
        logger.error(f"Raw directory not found: {raw_base}")
        return []

    json_files = list(raw_base.glob("dt=*/**/*.json"))
    if not json_files:
        logger.warning("No JSON files found")
        return []

    logger.info(f"Found {len(json_files)} files to process")

    all_dataframes = []
    for i, json_path in enumerate(json_files, 1):
        logger.info(f"[{i}/{len(json_files)}] Processing {json_path.name}")
        df = process_json_to_parquet(json_path, trusted_base)
        if df is not None:
            all_dataframes.append(df)

    return all_dataframes


def detect_changes(all_dataframes):
    if not all_dataframes or len(all_dataframes) < 2:
        return pd.DataFrame()

    full_df = pd.concat(all_dataframes, ignore_index=True)
    full_df['crawled_at'] = pd.to_datetime(full_df['crawled_at'])
    full_df = full_df.sort_values(['id', 'crawled_at'])

    changes = []
    monitor_fields = [
        'price', 'title', 'description', 'total_area', 'useful_area',
        'bedrooms', 'bathrooms', 'suites', 'garages',
        'street_name', 'address_number', 'neighborhood',
        'publisher_name', 'publisher_phone'
    ]

    for prop_id, prop_history in full_df.groupby('id'):
        if pd.isna(prop_id):
            continue
        prop_history = prop_history.reset_index(drop=True)
        for i in range(1, len(prop_history)):
            prev_row = prop_history.iloc[i - 1]
            curr_row = prop_history.iloc[i]
            for field in monitor_fields:
                prev_value = prev_row.get(field) if field in prev_row else None
                curr_value = curr_row.get(field) if field in curr_row else None
                prev_str = '' if pd.isna(prev_value) else str(prev_value)
                curr_str = '' if pd.isna(curr_value) else str(curr_value)
                if prev_str != curr_str and prev_str != '' and curr_str != '':
                    changes.append({
                        'id': int(prop_id),
                        'field': field,
                        'old_value': prev_str,
                        'new_value': curr_str,
                        'change_date': pd.to_datetime(curr_row['crawled_at'])
                    })

    changes_df = pd.DataFrame(changes)
    if not changes_df.empty:
        changes_df = changes_df.drop_duplicates()
        changes_df['id'] = changes_df['id'].astype('int64')
        changes_df['change_date'] = pd.to_datetime(changes_df['change_date'])
        logger.info(f"Detected {len(changes_df)} changes across {changes_df['id'].nunique()} properties")
        field_counts = changes_df['field'].value_counts()
        logger.info("Top changes by field:")
        for field, count in field_counts.head(5).items():
            logger.info(f"{field}: {count}")
    return changes_df


def prepare_for_database(all_dataframes):
    if not all_dataframes:
        return pd.DataFrame()

    full_df = pd.concat(all_dataframes, ignore_index=True)
    full_df['crawled_at'] = pd.to_datetime(full_df['crawled_at'])
    full_df = full_df.sort_values('crawled_at')

    latest_df = full_df.drop_duplicates(subset=['id'], keep='last').copy()
    latest_df.loc[:, 'active'] = True

    first_seen = full_df.groupby('id', as_index=False)['crawled_at'].min().rename(columns={'crawled_at': 'first_crawled_at'})
    latest_df = latest_df.merge(first_seen, on='id', how='left')
    latest_df['crawled_at'] = latest_df['first_crawled_at']
    latest_df = latest_df.drop(columns=['first_crawled_at'])

    daily_ids = full_df.groupby(full_df['crawled_at'].dt.date)['id'].apply(set).to_dict()
    dates = sorted(daily_ids.keys())

    if len(dates) >= 2:
        last_day_ids = daily_ids[dates[-1]]
        for prop_id in latest_df['id'].unique():
            days_present = sum(1 for date_ids in daily_ids.values() if prop_id in date_ids)
            if days_present >= 2 and prop_id not in last_day_ids:
                latest_df.loc[latest_df['id'] == prop_id, 'active'] = False

    logger.info(f"Total de dias analisados: {len(dates)}")
    if dates:
        logger.info(f"Período: {dates[0]} até {dates[-1]}")
    logger.info(f"Total properties: {len(latest_df)}")
    logger.info(f"Active: {int(latest_df['active'].sum())}")
    logger.info(f"Inactive: {int((~latest_df['active']).sum())}")

    if len(dates) >= 2:
        last_day_count = len(daily_ids[dates[-1]])
        prev_day_count = len(daily_ids[dates[-2]]) if len(dates) >= 2 else 0
        logger.info(f"Imóveis no último dia: {last_day_count}")
        logger.info(f"Imóveis no penúltimo dia: {prev_day_count}")
        if prev_day_count > 0:
            change_pct = ((last_day_count - prev_day_count) / prev_day_count) * 100
            logger.info(f"Variação: {change_pct:+.1f}%")

    return latest_df


def save_to_database(df, table_name='properties', schema='chavesnamao'):
    engine = get_postgres_engine()
    if not engine:
        logger.error("No database connection")
        return False

    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()

        dtype_map = None

        if table_name == 'changes' and set(df.columns) >= {'id', 'field', 'old_value', 'new_value', 'change_date'}:
            df = df.copy()
            df['id'] = df['id'].apply(lambda x: int(x) if pd.notna(x) else None)
            df['old_value'] = df['old_value'].astype(str)
            df['new_value'] = df['new_value'].astype(str)
            df['change_date'] = pd.to_datetime(df['change_date'])
            dtype_map = {
                'id': BigInteger(),
                'field': Text(),
                'old_value': Text(),
                'new_value': Text(),
                'change_date': DateTime()
            }

        if table_name == 'info':
            dtype_map = {
                'id': BigInteger(),
                'title': Text(),
                'reference': Text(),
                'description': Text(),
                'category': Text(),
                'total_area': Float(),
                'useful_area': Float(),
                'price': Float(),
                'bedrooms': Float(),
                'bathrooms': Float(),
                'suites': Float(),
                'garages': Float(),
                'street_name': Text(),
                'address_number': Text(),
                'address_comp': Text(),
                'neighborhood': Text(),
                'city': Text(),
                'state': Text(),
                'latitude': Float(),
                'longitude': Float(),
                'publisher_name': Text(),
                'publisher_phone': Text(),
                'privative_items': Text(),
                'estado': Text(),
                'cidade': Text(),
                'bairro': Text(),
                'updated_at': DateTime(),
                'crawled_at': DateTime(),
                'active': Boolean()
            }

        df.to_sql(
            table_name,
            engine,
            if_exists='replace' if table_name in ('info', 'changes') else 'append',
            index=False,
            schema=schema,
            method=None,
            chunksize=1000,
            dtype=dtype_map
        )

        logger.success(f"Saved {len(df)} records to {schema}.{table_name}")
        return True

    except Exception as e:
        logger.error(f"Database error: {e}")
        return False

    finally:
        try:
            engine.dispose()
        except Exception:
            pass


def main():
    logger.info("=" * 60)
    logger.info("PROCESSAMENTO CHAVES NA MÃO - RAW TO TRUSTED")
    logger.info("=" * 60)

    all_dataframes = process_all_raw_data()
    if not all_dataframes:
        logger.warning("No data processed")
        return

    logger.info("=" * 60)
    logger.info("DETECTANDO MUDANÇAS")
    logger.info("=" * 60)

    changes_df = detect_changes(all_dataframes)

    logger.info("=" * 60)
    logger.info("PREPARANDO DADOS PARA O BANCO")
    logger.info("=" * 60)

    properties_df = prepare_for_database(all_dataframes)

    if not properties_df.empty:
        save_to_database(properties_df, table_name='info', schema='imoveis')

    if not changes_df.empty:
        save_to_database(changes_df, table_name='changes', schema='imoveis')

    logger.info("=" * 60)
    logger.success("PROCESSAMENTO CONCLUÍDO!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
