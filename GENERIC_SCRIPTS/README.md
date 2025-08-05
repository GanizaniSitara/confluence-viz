# Generic OpenWebUI Document Uploaders

This directory contains generic scripts for bulk uploading documents to OpenWebUI via Qdrant vector database. These scripts ensure documents are fully integrated and visible in the OpenWebUI interface.

## Scripts

### 1. `qdrant_markdown_uploader.py`
Uploads markdown (`.md`) files to OpenWebUI with full integration.

**Features:**
- Processes markdown documents recursively from input directory
- Chunks text for better search performance
- Generates embeddings using Ollama (GPU-accelerated)
- Registers files in PostgreSQL for UI visibility
- Uploads vectors to Qdrant for semantic search
- Updates knowledge.data to ensure immediate visibility
- Checkpoint support for resuming interrupted uploads

**Usage:**
```bash
python qdrant_markdown_uploader.py --input-dir ./my-docs
```

### 2. `qdrant_tika_uploader.py`
Uploads non-markdown documents using Apache Tika for text extraction.

**Supported formats:**
- Microsoft Office: `.doc`, `.docx`, `.ppt`, `.pptx`, `.xls`, `.xlsx`
- OpenDocument: `.odt`, `.ods`
- Text formats: `.rtf`, `.txt`, `.csv`
- Email: `.msg`, `.eml`

**Note:** PDFs are intentionally excluded - use a dedicated OCR solution for PDFs.

**Usage:**
```bash
# Start Tika server first
docker run -p 9998:9998 apache/tika:latest

# Run uploader
python qdrant_tika_uploader.py --input-dir ./my-docs
```

## Configuration

Edit `settings.ini` to configure both scripts:

### Required Settings

1. **OpenWebUI IDs:**
   - `knowledge_id`: Create a knowledge collection in OpenWebUI and copy its ID
   - `user_id`: Your user ID from OpenWebUI

2. **Database Connections:**
   - PostgreSQL: OpenWebUI's database
   - Qdrant: Vector database (can run in Docker/WSL)
   - Ollama: For generating embeddings

3. **Directories:**
   - `input_dir`: Where your documents are located

## Prerequisites

### 1. Python Dependencies
```bash
pip install psycopg2-binary qdrant-client ollama tqdm requests
```

### 2. Services Running
- **OpenWebUI** with PostgreSQL database
- **Qdrant** vector database
- **Ollama** with nomic-embed-text:v1.5 model
- **Apache Tika** (for non-markdown documents only)

### 3. Ollama Model
```bash
ollama pull nomic-embed-text:v1.5
```

## How It Works

Both scripts follow the same pattern:

1. **Read** documents from input directory
2. **Extract** text (directly for markdown, via Tika for others)
3. **Chunk** text into smaller segments
4. **Generate** embeddings using Ollama
5. **Register** file in PostgreSQL (OpenWebUI's database)
6. **Upload** vectors to Qdrant collections
7. **Update** knowledge.data for UI visibility

Files are stored in three places:
- PostgreSQL `file` table (metadata)
- Qdrant `open-webui_files` collection (file search)
- Qdrant `open-webui_knowledge` collection (knowledge search)
- PostgreSQL `knowledge.data` field (UI visibility)

## Environment Considerations

### WSL Users
If running from WSL accessing services on Windows:
- Set `db_host = 172.17.112.1` (or your Windows IP from WSL)
- Qdrant may need similar IP configuration

### Docker Users
Services running in Docker should be accessible at:
- `host.docker.internal` (from container)
- `localhost` (from host)

## Troubleshooting

### Files not visible in OpenWebUI?
- Check that knowledge.data was updated (scripts do this automatically)
- Verify knowledge_id and user_id are correct
- Refresh the OpenWebUI page

### Connection errors?
- Verify all services are running
- Check IP addresses for WSL/Docker environments
- Test connections individually

### Checkpoint/Resume
Both scripts support resuming if interrupted:
- Markdown: Uses `qdrant_markdown_checkpoint.json`
- Tika: Uses `qdrant_tika_checkpoint.json` and `.tika_processed.json`
- To start fresh: Delete checkpoint files or use `--clear-checkpoint`

## Performance Tips

1. **Batch Processing**: Scripts update knowledge.data every 100 files
2. **Chunk Size**: Default 500 chars with 50 char overlap works well
3. **Embedding Model**: nomic-embed-text:v1.5 provides 768-dim vectors
4. **Delays**: Built-in delays prevent overwhelming services

## Security Notes

- Store database passwords securely
- Don't commit settings.ini with real credentials
- Use environment variables for production deployments