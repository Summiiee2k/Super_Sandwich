#Be done by Sumedh
import spacy
import duckdb
import sys
import logging
from datetime import datetime

nlp = spacy.load("en_core_web_sm")

CONFIG = {
    'db_path': 'sup-san-reviews.db',  
    'log_file': 'processing.log',
    'batch_size': 500  
}

#creating appropriate lemmas for categories like FOOD,SERVICE and GENERAL comments, added extra lemmas for extra accruracy :D
FOOD_LEMMAS = {"sandwich", "bread", "meat", "cheese", "ham", "omelette", "food", "meal",
               "lettuce", "tomato", "mayo", "mustard",
               "avocado", "bacon", "turkey", "chicken",
               "toast", "baguette", "wrap",
               "salad", "fries",
               "vegetarian", "vegan", "gluten",
               "flavor", "taste", "fresh", "spicy",
               "recipe", "ingredient", "portion"
               }
SERVICE_LEMMAS = {"waiter", "service", "table",
                  "staff", "server", "host", "manager",
                  "tip", "bill", "payment", "cashier",
                  "order", "wait", "delay", "reservation",
                  "cleanliness", "atmosphere", "ambiance",
                  "complaint", "feedback", "experience"
                  }


#Now we create a function which will create the tables which we need, proc_log and proc_messages :)

def create_tables(conn):
    """Create required tables if they don't exist"""
    cursor = conn.cursor()
    
    # Control table to track processed UUIDs
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS proc_log (
            uuid VARCHAR PRIMARY KEY,
            proc_time TIMESTAMP
        )
    ''')
    
    # Processed messages table
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


#Function to determine whether we categorize the lemmas in FOOD, SERVICE or GENERAL category

def calculate_category(doc):
    food_score = 0
    service_score = 0
    
    # Count lemma matches
    for token in doc:
        lemma = token.lemma_.lower()
        if lemma in FOOD_LEMMAS:
            food_score += 1
        elif lemma in SERVICE_LEMMAS:
            service_score += 1
    
    for ent in doc.ents:
        if ent.label_ == "MONEY":
            service_score += 1
    
    # Decision logic
    if service_score > food_score:
        return "SERVICE"
    elif food_score >= service_score and food_score > 0:
        return "FOOD"
    else:
        return "GENERAL"
    
#Now we determine the main function 
def main():
    try:
        # Connect to DuckDB
        conn = duckdb.connect(CONFIG['db_path'])
        create_tables(conn)
        cursor = conn.cursor()

        #Insert new UUIDs into proc_log
        cursor.execute('''
            INSERT INTO proc_log (uuid)
            SELECT uuid FROM raw_messages
            WHERE uuid NOT IN (SELECT uuid FROM proc_log)
            ON CONFLICT (uuid) DO NOTHING
        ''')
        new_uuids = cursor.rowcount
        logging.info(f"Found {new_uuids} new messages to process")

        #Fetch unprocessed messages
        cursor.execute('''
            SELECT r.timestamp, r.uuid, r.message 
            FROM raw_messages r
            JOIN proc_log p ON r.uuid = p.uuid
            WHERE p.proc_time IS NULL
        ''')
        unprocessed = cursor.fetchall()

        #Process in batches
        total_processed = 0
        batch = []
        for ts, uuid, message in unprocessed:
            doc = nlp(message)
            
            # Calculate fields
            category = calculate_category(doc)
            num_lemm = len([t.lemma_ for t in doc])
            num_char = len(message)
            
            batch.append( (ts, uuid, message, category, num_lemm, num_char) )
            
            # Insert batch when full
            if len(batch) >= CONFIG['batch_size']:
                cursor.executemany('''
                    INSERT INTO proc_messages 
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (uuid) DO NOTHING
                ''', batch)
                total_processed += cursor.rowcount
                batch = []

        # Insert remaining records
        if batch:
            cursor.executemany('''
                INSERT INTO proc_messages 
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (uuid) DO NOTHING
            ''', batch)
            total_processed += cursor.rowcount

        # Step 4: Update proc_log timestamps
        cursor.execute('''
            UPDATE proc_log 
            SET proc_time = CURRENT_TIMESTAMP 
            WHERE uuid IN (
                SELECT uuid FROM proc_messages
            )
        ''')
        conn.commit()

        # Final report
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