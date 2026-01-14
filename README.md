# Super Sandwich Review Analysis Pipeline 🥪

A modular ETL (Extract, Transform, Load) pipeline designed to ingest customer reviews for the "Super Sandwich" brand, analyze them using Natural Language Processing (NLP), and export structured insights. This project uses **DuckDB** for high-performance embedded storage and **spaCy** for linguistic processing.

##  Project Overview

This tool automates the workflow of handling customer feedback through three distinct stages:

1.  **Ingestion:** Validates raw CSV data and loads it into a persistent database.
2.  **Processing:** Applies NLP to classify reviews (Food vs. Service) and calculate lemma metrics.
3.  **Extraction:** Exports processed data based on date filters into a structured JSON report.

##  Architecture

The pipeline moves data through the following flow:

`Raw CSV` ➔ **Ingestion Script** ➔ `DuckDB (Raw)` ➔ **Processing Script (NLP)** ➔ `DuckDB (Processed)` ➔ **Read Script** ➔ `JSON Report`

### Key Features
* **Idempotency:** The ingestion and processing steps utilize UUID checks (`ON CONFLICT DO NOTHING`) to prevent duplicate data entry.
* **NLP Categorization:** Automatically classifies reviews into **FOOD**, **SERVICE**, or **GENERAL** categories based on lemma analysis and entity recognition.
* **Batch Processing:** Efficiently handles large datasets using batch commits and `nlp.pipe` for faster tokenization.
* **Logging:** Detailed execution logs are generated for every stage (`ingestion.log`, `processing.log`, `read.log`).
