#Be done by Sumedh
import spacy
import duckdb
import sys
import logging
from datetime import datetime

CONFIG = {
    'db_path': 'sup-san-reviews.db',
    'log_file': 'processing.log',
    'batch_size': 500
}

# FIX: Added logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(CONFIG['log_file']),
        logging.StreamHandler()
    ]
)

nlp = spacy.load("en_core_web_sm")

FOOD_LEMMAS = {"sandwich", "bread", "meat", "cheese", "ham", "omelette", "food", "meal",
               "lettuce", "tomato", "mayo", "mustard", "avocado", "bacon", "turkey", 
               "chicken", "toast", "baguette", "wrap", "salad", "fries", "vegetarian", 
               "vegan", "gluten", "flavor", "taste", "fresh", "spicy", "recipe", 
               "ingredient", "portion"}

SERVICE_LEMMAS = {"waiter", "service", "table", "staff", "server", "host", "manager",
                  "tip", "bill", "payment", "cashier", "order", "wait", "delay", 
                  "reservation", "cleanliness", "atmosphere", "ambiance", "complaint", 
                  "feedback", "experience"}

def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS proc_log (
            uuid VARCHAR PRIMARY KEY,
            proc_time TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS proc_messages (
            timestamp TIMESTAMP,
            uuid VARCHAR PRIMARY KEY,
            message VARCHAR,
            category VARCHAR,
            num_lemm INTEGER,
            num_char INTEGER
        )
    ''')
    conn.commit()
    cursor.close()

def calculate_category(doc):
    food_score = 0
    service_score = 0
    
    for token in doc:
        lemma = token.lemma_.lower()
        if lemma in FOOD_LEMMAS:
            food_score += 1
        elif lemma in SERVICE_LEMMAS:
            service_score += 1
    
    for ent in doc.ents:
        if ent.label_ == "MONEY":
            service_score += 1
    
    if service_score > food_score:
        return "SERVICE"
    elif food_score >= service_score and food_score > 0:
        return "FOOD"
    else:
        return "GENERAL"

def main():
    try:
        conn = duckdb.connect(CONFIG['db_path'])
        create_tables(conn)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO proc_log (uuid)
            SELECT uuid FROM raw_messages
            WHERE uuid NOT IN (SELECT uuid FROM proc_log)
            ON CONFLICT (uuid) DO NOTHING
        ''')
        new_uuids = cursor.rowcount
        logging.info(f"Found {new_uuids} new messages to process")

        cursor.execute('''
            SELECT r.timestamp, r.uuid, r.message 
            FROM raw_messages r
            JOIN proc_log p ON r.uuid = p.uuid
            WHERE p.proc_time IS NULL
        ''')
        unprocessed = cursor.fetchall()

        # FIX: Batch processing with spaCy's nlp.pipe
        texts = [msg for (_, _, msg) in unprocessed]
        docs = nlp.pipe(texts)

        batch = []
        processed_uuids = []
        for doc, (ts, uuid, _) in zip(docs, unprocessed):
            category = calculate_category(doc)
            num_lemm = len([t.lemma_ for t in doc])
            num_char = len(doc.text)  # More accurate than len(message)
            
            batch.append((ts, uuid, doc.text, category, num_lemm, num_char))
            processed_uuids.append(uuid)

            if len(batch) >= CONFIG['batch_size']:
                cursor.executemany('''
                    INSERT INTO proc_messages 
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', batch)
                conn.commit()
                batch = []

        if batch:
            cursor.executemany('''
                INSERT INTO proc_messages 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', batch)
            conn.commit()

        # FIX: Update ALL processed UUIDs in proc_log
        cursor.executemany('''
            UPDATE proc_log 
            SET proc_time = CURRENT_TIMESTAMP 
            WHERE uuid = ?
        ''', [(uuid,) for uuid in processed_uuids])
        conn.commit()

        total_processed = len(processed_uuids)
        duplicates = len(unprocessed) - total_processed
        logging.info(
            f"Processing complete:\n"
            f"- Total processed: {len(unprocessed)}\n"
            f"- Successfully inserted: {total_processed}\n"
            f"- Duplicates skipped: {duplicates}"
        )

    except Exception as e:
        logging.error(f"Processing failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()