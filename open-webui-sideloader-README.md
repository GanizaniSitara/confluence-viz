# OpenWebUI Knowledge Collection Sideloader

This tool allows you to directly load documents into OpenWebUI's knowledge collections by bypassing the web interface and inserting directly into the PostgreSQL database.

## Overview

The sideloader:
- Connects directly to OpenWebUI's PostgreSQL database
- Lists existing knowledge collections
- Extracts text from documents using Apache Tika
- Generates embeddings using Ollama
- Properly inserts documents into OpenWebUI's schema

## Prerequisites

1. **OpenWebUI** must be installed and running
2. **PostgreSQL** access to OpenWebUI's database
3. **Apache Tika** server running (for document extraction)
4. **Ollama** with an embedding model installed

## Setup

1. Copy `settings.example.ini` to `settings.ini`
2. Configure the following sections:

```ini
[database]
# OpenWebUI's PostgreSQL connection
dsn = postgresql://webui:webui@localhost:5432/open-webui

[tika]
# Apache Tika server URL
url = http://localhost:9998

[ollama]
# Ollama embeddings API
url = http://localhost:11434/api/embeddings
model = nomic-embed-text

[sideloader]
# Documents directory
docs_dir = ./documents
# Optional: specify collection name
collection_name = confluence_docs
```

## Usage

1. **Create a knowledge collection in OpenWebUI first**
   - Log into OpenWebUI
   - Go to Knowledge section
   - Create a new collection (e.g., "confluence_docs")

2. **Prepare your documents**
   - Place documents in the configured `docs_dir`
   - Supported formats: Any format that Apache Tika can process

3. **Run the sideloader**
   ```bash
   python open-webui-sideloader.py
   ```

4. **Select collection**
   - If `collection_name` is not specified in settings, you'll be prompted
   - Choose from the list of available collections

## How It Works

The sideloader integrates with OpenWebUI's database schema:

1. **Files Table**: Stores file metadata
2. **File Data Table**: Stores extracted text and embeddings
3. **Knowledge Files Table**: Links files to collections

Each document is:
- Extracted to text using Tika
- Embedded using Ollama
- Stored with proper relationships in the database

## Differences from API Upload

Unlike the API uploader (`open-webui.py`), the sideloader:
- Bypasses the web interface entirely
- Works directly with the database
- Can handle larger volumes more efficiently
- Requires database access

## Troubleshooting

- **No collections found**: Create a collection in OpenWebUI first
- **Database connection failed**: Check PostgreSQL credentials and access
- **Tika errors**: Ensure Tika server is running
- **Embedding errors**: Check Ollama is running with the specified model

## Security Notes

- This tool requires direct database access
- Use appropriate security measures for database credentials
- Consider running on the same host as OpenWebUI for security