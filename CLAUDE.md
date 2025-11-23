# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a SmugMug to S3 streaming transfer application that downloads images from SmugMug's REST API and uploads them directly to S3-compatible storage without local disk storage. The application preserves album/folder structure and transfers metadata.

## Architecture

### Core Components

- **SmugMugS3Streamer** (`main.py`): Main application class that orchestrates the entire transfer process
- **Authentication Module** (`common.py`): OAuth 1.0a authentication utilities for SmugMug API
- **Console Helper** (`console.py`): Interactive OAuth token generation utility

### Key Features

- **Streaming Transfer**: Images are streamed directly from SmugMug to S3 without local storage
- **Duplicate Prevention**: Uses SQLite database (`transfer_log.db`) to track completed transfers
- **Caching**: File list caching (`smugmug_files_cache.json`) to reduce API calls
- **Multiple Auth Methods**: Supports API key, username/password, and OAuth 1.0a authentication
- **Rate Limiting**: Configurable delays between API calls to respect service limits

## Development Commands

### Python Environment
This project uses **uv** for dependency management:

```bash
# Install dependencies
uv sync

# Run the application  
uv run python main.py

# Run with debugging
uv run python -c "import ipdb; ipdb.set_trace(); exec(open('main.py').read())"
```

### Authentication Setup

1. **OAuth Setup** (recommended): Run the console helper to get OAuth tokens:
   ```bash
   uv run python console.py
   ```

2. **Configuration**: Copy OAuth credentials to `.env` file with these variables:
   ```
   SMUGMUG_OAUTH_CONSUMER_KEY=your_key
   SMUGMUG_OAUTH_CONSUMER_SECRET=your_secret  
   SMUGMUG_OAUTH_TOKEN=your_token
   SMUGMUG_OAUTH_TOKEN_SECRET=your_token_secret
   S3_ACCESS_KEY=your_s3_key
   S3_SECRET_KEY=your_s3_secret
   S3_BUCKET=your_bucket
   S3_ENDPOINT_URL=your_s3_endpoint  # Optional for S3-compatible services
   ```

### Testing

The application includes authentication testing:
```bash
# Test authentication without full transfer
uv run python -c "from main import *; config = load_config(); s = SmugMugS3Streamer(config); s.test_authentication()"
```

## Key Files

- `main.py`: Core streaming logic and SmugMug API integration
- `common.py`: OAuth authentication utilities (duplicated code - needs cleanup)
- `console.py`: Interactive OAuth token generation
- `config.json`: OAuth consumer key/secret configuration
- `transfer_log.db`: SQLite database tracking completed transfers
- `.env`: Environment variables for credentials
- `pyproject.toml`: uv dependency configuration

## Important Notes

- The application creates and maintains a SQLite transfer log to prevent duplicate uploads
- File list caching reduces SmugMug API calls but can be cleared by deleting `smugmug_files_cache.json`
- Rate limiting is configurable via `RATE_LIMIT_DELAY` environment variable (default 0.1 seconds)
- S3 metadata is preserved from SmugMug image metadata
- The `common.py` file contains duplicated code that should be refactored