#Be done by Ana
import json
import duckdb
import sys
import logging
from datetime import datetime

CONFIG = {
    'db_path': 'sup-san-reviews.db',
    'log_file': 'read.log',
    'output_file': 'messages.json'
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(CONFIG['log_file']),
        logging.StreamHandler()
    ]
)

def validate_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        logging.error("Invalid date format. Please use YYYY-MM-DD.")
        sys.exit(1)

def fetch_processed_messages(date_str):
    try:
        conn = duckdb.connect(CONFIG['db_path'])
        cursor = conn.cursor()

        cursor.execute('''
            SELECT uuid, timestamp, message, category, num_lemm, num_char
            FROM proc_messages
            WHERE timestamp >= CAST(? AS TIMESTAMP)
            ORDER BY timestamp ASC
        ''', [date_str])
        
        messages = cursor.fetchall()
        conn.close()
        return messages
    
    except Exception as e:
        logging.error(f"Database error: {str(e)}", exc_info=True)
        sys.exit(1)

def save_to_json(messages):
    data = {
        "num": len(messages),
        "messages": [
            {
                "uuid": msg[0],
                "timestamp": msg[1].isoformat(),  
                "message": msg[2],
                "category": msg[3],
                "num_lemm": msg[4],
                "num_char": msg[5]  
            }
            for msg in messages
        ]
    }

    with open(CONFIG['output_file'], "w") as json_file:
        json.dump(data, json_file, indent=4)

    logging.info(f"Saved {len(messages)} messages to {CONFIG['output_file']}.")

def main():
    if len(sys.argv) != 2:
        logging.error("Usage: python read.py <YYYY-MM-DD>")
        sys.exit(1)
    
    date_str = sys.argv[1]
    validate_date(date_str)
    messages = fetch_processed_messages(date_str)
    save_to_json(messages)

if __name__ == "__main__":
    main()
