#Be done by Kaan 

import duckdb
import csv
import sys
import os
import logging
from datetime import datetime

# Configuration
CONFIG = {
    'batch_size': 1000,
    'required_columns': ['timestamp', 'uuid', 'message'],
    'db_path': 'sup-san-reviews.db',
    'log_file': 'ingestion.log'
}

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(CONFIG['log_file']),
        logging.StreamHandler()
    ]
)

def validate_csv_header(reader):
    required = set(CONFIG['required_columns'])
    if not required.issubset(reader.fieldnames):
        missing = required - set(reader.fieldnames)
        logging.error(f"Missing required columns: {', '.join(missing)}")
        sys.exit(1)

def validate_timestamp(ts):
    try:
        datetime.fromisoformat(ts)
        return True
    except ValueError:
        return False

def main():
    if len(sys.argv) != 2:
        logging.error("Usage: python ingestion.py <filename>")
        sys.exit(1)

    input_file = sys.argv[1]

    try:
        conn = duckdb.connect(CONFIG['db_path'])
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_messages (
                timestamp TIMESTAMP,
                uuid VARCHAR PRIMARY KEY,
                message VARCHAR
            )
        ''')
        conn.commit()

        with open(input_file, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            validate_csv_header(csv_reader)

            batch = []
            total = {'processed': 0, 'inserted': 0, 'invalid': 0, 'duplicates': 0}

            for row in csv_reader:
                total['processed'] += 1

                if not all(row.get(col) for col in CONFIG['required_columns']):
                    logging.warning(f"Skipping incomplete row: {row}")
                    total['invalid'] += 1
                    continue

                if not validate_timestamp(row['timestamp']):
                    logging.warning(f"Invalid timestamp: {row['timestamp']}")
                    total['invalid'] += 1
                    continue

                batch.append((row['timestamp'], row['uuid'], row['message']))

                if len(batch) >= CONFIG['batch_size']:
                    cursor.executemany('''
                        INSERT INTO raw_messages 
                        VALUES (?, ?, ?)
                        ON CONFLICT (uuid) DO NOTHING
                    ''', batch)
                    total['inserted'] += cursor.execute("SELECT COUNT(*) FROM raw_messages").fetchone()[0]  # FIXED
                    batch = []

            if batch:
                cursor.executemany('''
                    INSERT INTO raw_messages 
                    VALUES (?, ?, ?)
                    ON CONFLICT (uuid) DO NOTHING
                ''', batch)
                total['inserted'] += cursor.execute("SELECT COUNT(*) FROM raw_messages").fetchone()[0]  # FIXED

            conn.commit()

            valid_rows = total['processed'] - total['invalid']
            total['duplicates'] = valid_rows - total['inserted']

            logging.info(
                f"Processing complete:\n"
                f"- Total rows processed: {total['processed']}\n"
                f"- Successfully inserted: {total['inserted']}\n"
                f"- Invalid rows skipped: {total['invalid']}\n"
                f"- Duplicate UUIDs skipped: {total['duplicates']}"
            )

    except FileNotFoundError:
        logging.error(f"File not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Critical error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
