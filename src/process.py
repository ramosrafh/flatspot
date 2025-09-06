import json
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from loguru import logger
import os

load_dotenv(find_dotenv())

POSTGRESQL_USER = os.getenv('POSTGRESQL_USER')
POSTGRESQL_PASSWORD = os.getenv('POSTGRESQL_PASSWORD')
POSTGRESQL_HOST = os.getenv('POSTGRESQL_HOST')
POSTGRESQL_PORT = os.getenv('POSTGRESQL_PORT')
POSTGRESQL_DATABASE = os.getenv('POSTGRESQL_DATABASE')

def get_postgres_engine():
    """Create PostgreSQL engine connection"""
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{POSTGRESQL_USER}:{POSTGRESQL_PASSWORD}@"
            f"{POSTGRESQL_HOST}:{POSTGRESQL_PORT}/"
            f"{POSTGRESQL_DATABASE}"
        )
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {e}")
        return None

engine = get_postgres_engine()

def load_json_file(file_path):
    """Load JSON file with proper encoding"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def extract_property_data(data):
    """Extract property data from JSON structure"""
    extracted = []

    for prop in data.get('properties', []):
        row = {
            'id': prop.get('id'),
            'title': prop.get('title'),
            'reference': prop.get('reference'),
            'active': prop.get('active'),
            'description': prop.get('description'),
            'total_area': float(prop.get('area', {}).get('total', 0)) if prop.get('area', {}).get('total') else None,
            'useful_area': float(prop.get('area', {}).get('useful', 0)) if prop.get('area', {}).get('useful') else None,
            'publisher_name': prop.get('publisher', {}).get('name'),
            'publisher_phone': prop.get('publisher', {}).get('phones', {}).get('cellphone', {}).get('number'),
            'publisher_landline': prop.get('publisher', {}).get('phones', {}).get('landline', {}).get('number'),
            'publisher_commercial': prop.get('publisher', {}).get('phones', {}).get('commercial', {}).get('number'),
            'category': prop.get('category'),
            'price': prop.get('prices', {}).get('rawPrice'),
            'commercial_rooms': prop.get('commercialRooms', {}).get('count'),
            'bathrooms': prop.get('bathrooms', {}).get('count'),
            'suites': prop.get('suites', {}).get('count'),
            'garages': prop.get('garages', {}).get('count'),
            'street_name': prop.get('location', {}).get('street', {}).get('name'),
            'address_number': prop.get('location', {}).get('street', {}).get('addressNumber'),
            'address_comp': prop.get('location', {}).get('addressComp'),
            'neighborhood_name': prop.get('location', {}).get('neighborhood', {}).get('name'),
            'city_name': prop.get('location', {}).get('city', {}).get('name'),
            'state_name': prop.get('location', {}).get('state', {}).get('name'),
            'latitude': float(prop.get('location', {}).get('geoposition', {}).get('lat')) if prop.get('location', {}).get('geoposition', {}).get('lat') else None,
            'longitude': float(prop.get('location', {}).get('geoposition', {}).get('lon')) if prop.get('location', {}).get('geoposition', {}).get('lon') else None,
            'privative_items': json.dumps([item.get('name') for item in prop.get('privativeItems', []) if item.get('name')]),
            'updated_at': pd.to_datetime(prop.get('updatedAt'), errors='coerce'),
            'crawled_at': pd.to_datetime(data.get('crawled_at'), errors='coerce')
        }
        extracted.append(row)

    return extracted

def process_all_json_files(directory):
    """Process all JSON files and track changes"""
    all_data = []
    changes = []
    json_files = sorted(list(Path(directory).glob('*.json')))

    if not json_files:
        logger.warning("No JSON files found in directory")
        return pd.DataFrame(), pd.DataFrame()

    logger.info(f"Processing {len(json_files)} files")

    if len(json_files) < 2:
        # If only one file, process it normally
        if len(json_files) == 1:
            try:
                data = load_json_file(json_files[0])
                extracted_data = extract_property_data(data)
                df = pd.DataFrame(extracted_data)
                if not df.empty:
                    df = df.drop_duplicates(subset=['id'], keep='last')
                    df = df.dropna(subset=['id'])
                return df, pd.DataFrame()
            except Exception as e:
                logger.error(f"Error processing {json_files[0].name}: {e}")
                return pd.DataFrame(), pd.DataFrame()
        else:
            return pd.DataFrame(), pd.DataFrame()

    previous_data = {}
    first_seen = {}

    for i, file_path in enumerate(json_files):
        try:
            data = load_json_file(file_path)
            extracted_data = extract_property_data(data)
            current_df = pd.DataFrame(extracted_data)

            if not current_df.empty:
                current_df = current_df.drop_duplicates(subset=['id'], keep='last')

                for _, row in current_df.iterrows():
                    prop_id = row['id']

                    if pd.isna(prop_id):
                        continue

                    # Track first time we see this property
                    if prop_id not in first_seen:
                        first_seen[prop_id] = row['crawled_at']

                    # Compare with previous version if exists
                    if prop_id in previous_data:
                        old_row = previous_data[prop_id]

                        for column in current_df.columns:
                            if column in ['crawled_at', 'updated_at']:
                                continue

                            old_value = old_row[column]
                            new_value = row[column]

                            # Convert to string for comparison, handle NaN values
                            old_str = str(old_value) if pd.notna(old_value) else ''
                            new_str = str(new_value) if pd.notna(new_value) else ''

                            if old_str != new_str:
                                changes.append({
                                    'id': prop_id,
                                    'field': column,
                                    'old_value': old_value,
                                    'new_value': new_value,
                                    'old_date': old_row['crawled_at'],
                                    'new_date': row['crawled_at'],
                                })

                    previous_data[prop_id] = row

        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {e}")

    # Create final dataset with original crawled_at dates
    final_data = []
    for prop_id, row in previous_data.items():
        row_dict = dict(row)
        row_dict['crawled_at'] = first_seen[prop_id]
        final_data.append(row_dict)

    final_df = pd.DataFrame(final_data)
    changes_df = pd.DataFrame(changes)

    return final_df, changes_df

def create_schemas(schemas):
    """Create database schemas if they don't exist"""
    try:
        conn = engine.raw_connection()
        cur = conn.cursor()

        for schema in schemas:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            conn.commit()

        cur.close()
        conn.close()
        logger.info(f"Schemas created/verified: {schemas}")
    except Exception as e:
        logger.error(f"Error creating schemas: {e}")

def save_to_database(df, table_name, schema='imoveis'):
    """Save DataFrame to PostgreSQL database"""
    try:
        create_schemas([schema])

        # Remove rows with null IDs
        df_clean = df.dropna(subset=['id'])

        if df_clean.empty:
            logger.warning(f"No valid data to save to {schema}.{table_name}")
            return False

        df_clean.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False,
            schema=schema,
            method='multi'
        )

        logger.success(f"Saved {len(df_clean)} records to {schema}.{table_name}")
        return True

    except Exception as e:
        logger.error(f"Database error saving to {schema}.{table_name}: {e}")
        return False

def main():
    """Main processing function"""
    directory = os.getenv('DATA_DIRECTORY', 'results/')
    schema = os.getenv('POSTGRES_SCHEMA', 'imoveis')

    if not engine:
        logger.error("Could not establish database connection")
        return

    # Process all JSON files and detect changes
    final_df, changes_df = process_all_json_files(directory)

    if final_df.empty:
        logger.warning("No property data to process")
    else:
        logger.info(f"Processed {len(final_df)} properties")

        # Save properties to 'info' table
        if save_to_database(final_df, 'info', schema):
            logger.success("Properties data saved successfully")
        else:
            logger.error("Failed to save properties data")

    if changes_df.empty:
        logger.info("No changes detected between files")
    else:
        logger.info(f"Found {len(changes_df)} property changes")

        # Save changes to 'changes' table
        if save_to_database(changes_df, 'changes', schema):
            logger.success("Changes data saved successfully")
        else:
            logger.error("Failed to save changes data")

    logger.success("Processing completed")

if __name__ == "__main__":
    main()
