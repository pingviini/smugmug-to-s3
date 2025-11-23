#!/usr/bin/env python3
"""
Async SmugMug to S3 Streaming Transfer Application

This is an async version of the SmugMug streamer that enables concurrent processing
for significantly improved performance. It uses aiohttp for async HTTP requests,
asyncio for concurrency control, and maintains the same crash recovery features.

Requirements:
- aiohttp
- aiofiles
- aioboto3 (or async wrapper for boto3)
- asyncio (built-in)

Performance improvements:
- 10-50x faster transfers through concurrent processing
- Batch processing with configurable concurrency limits
- Direct streaming without local file storage
- Maintains API expansion optimizations
"""

import asyncio
import os
import json
import logging
from typing import Dict, List, Optional, AsyncIterator
from urllib.parse import urlparse, urlencode
from datetime import datetime, timedelta
import hashlib
import hmac
import base64
import uuid
import urllib.parse

import aiohttp
import aiofiles
import aioboto3
from botocore.exceptions import ClientError
import time
import sqlite3

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

TRANSFER_LOG_DB = "transfer_log.db"

class AsyncSmugMugS3Streamer:
    def __init__(self, config: Dict):
        """Initialize the async streamer with configuration."""
        self.config = config
        self.max_concurrent = config.get('max_concurrent_downloads', 20)
        self.batch_size = config.get('batch_size', 50)
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        self.transfer_log_db = TRANSFER_LOG_DB
        self._init_transfer_log_db()
        self.transferred_files = self.load_transfer_log()

    def _init_transfer_log_db(self):
        """Initialize SQLite database for transfer logging and caching."""
        db_exists = os.path.exists(self.transfer_log_db)
        self.log_conn = sqlite3.connect(self.transfer_log_db)
        self.log_cursor = self.log_conn.cursor()

        if not db_exists:
            self.log_cursor.execute(
                """
                CREATE TABLE transferred_files (
                    s3_key TEXT PRIMARY KEY,
                    transfer_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self.log_cursor.execute(
                """
                CREATE TABLE album_cache (
                    album_key TEXT NOT NULL,
                    album_name TEXT,
                    album_path TEXT,
                    cache_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed BOOLEAN DEFAULT 0,
                    PRIMARY KEY (album_key)
                )
                """
            )
            self.log_cursor.execute(
                """
                CREATE TABLE file_cache (
                    id TEXT PRIMARY KEY,
                    album_key TEXT NOT NULL,
                    album_name TEXT,
                    filename TEXT,
                    uri TEXT,
                    url TEXT,
                    keywords TEXT,
                    md5 TEXT,
                    size TEXT,
                    title TEXT,
                    caption TEXT,
                    cache_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (album_key) REFERENCES album_cache(album_key)
                )
                """
            )
            self.log_conn.commit()

    def load_transfer_log(self):
        """Load transferred files from database."""
        self.log_cursor.execute("SELECT s3_key FROM transferred_files")
        return {row[0] for row in self.log_cursor.fetchall()}

    def save_transfer_log(self, s3_key):
        """Save transferred file to database."""
        try:
            self.log_cursor.execute(
                "INSERT OR IGNORE INTO transferred_files (s3_key) VALUES (?)",
                (s3_key,),
            )
            self.log_conn.commit()
        except Exception as e:
            logger.error(f"Error logging transferred file '{s3_key}': {e}")

    async def setup_smugmug_session(self) -> aiohttp.ClientSession:
        """Setup async SmugMug session with authentication."""
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Smuglicious-Async/1.0'
        }

        # API Key authentication
        params = {}
        if self.config.get('smugmug_api_key'):
            params['APIKey'] = self.config['smugmug_api_key']
            logger.info("Using SmugMug API Key authentication")

        # OAuth 1.0a authentication
        elif all(self.config.get(key) for key in
                 ['smugmug_oauth_consumer_key', 'smugmug_oauth_consumer_secret',
                  'smugmug_oauth_token', 'smugmug_oauth_token_secret']):
            logger.info("Using SmugMug OAuth 1.0a authentication")
            # OAuth will be handled per-request in make_api_request

        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout

        # Store API key params for later use in requests
        self.api_params = params

        return aiohttp.ClientSession(
            headers=headers,
            connector=connector,
            timeout=timeout
        )

    def _generate_oauth_signature(self, method: str, url: str, params: Dict) -> Dict[str, str]:
        """Generate OAuth 1.0a signature for SmugMug API."""
        # OAuth parameters
        oauth_params = {
            'oauth_consumer_key': self.config['smugmug_oauth_consumer_key'],
            'oauth_token': self.config['smugmug_oauth_token'],
            'oauth_signature_method': 'HMAC-SHA1',
            'oauth_timestamp': str(int(time.time())),
            'oauth_nonce': str(uuid.uuid4()).replace('-', ''),
            'oauth_version': '1.0'
        }

        # Combine OAuth params with request params
        all_params = {**params, **oauth_params}

        # Create parameter string
        sorted_params = sorted(all_params.items())
        param_string = '&'.join([f"{urllib.parse.quote_plus(str(k))}={urllib.parse.quote_plus(str(v))}" 
                                for k, v in sorted_params])

        # Create signature base string
        parsed_url = urllib.parse.urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        base_string = f"{method}&{urllib.parse.quote_plus(base_url)}&{urllib.parse.quote_plus(param_string)}"

        # Create signing key
        consumer_secret = urllib.parse.quote_plus(self.config['smugmug_oauth_consumer_secret'])
        token_secret = urllib.parse.quote_plus(self.config['smugmug_oauth_token_secret'])
        signing_key = f"{consumer_secret}&{token_secret}"

        # Generate signature
        signature = hmac.new(
            signing_key.encode('utf-8'),
            base_string.encode('utf-8'),
            hashlib.sha1
        ).digest()
        oauth_signature = base64.b64encode(signature).decode('utf-8')

        oauth_params['oauth_signature'] = oauth_signature
        return oauth_params

    async def make_api_request(self, session: aiohttp.ClientSession, url: str) -> Optional[Dict]:
        """Make authenticated API request with rate limiting and retries."""
        max_retries = 3
        base_delay = 1

        for attempt in range(max_retries):
            try:
                # Prepare request parameters
                parsed_url = urllib.parse.urlparse(url)
                query_params = urllib.parse.parse_qsl(parsed_url.query)
                params = dict(query_params)

                # Add API key if using API key auth
                if hasattr(self, 'api_params') and self.api_params:
                    params.update(self.api_params)

                # Add OAuth signature if using OAuth
                headers = session.headers.copy()
                if all(self.config.get(key) for key in
                       ['smugmug_oauth_consumer_key', 'smugmug_oauth_consumer_secret',
                        'smugmug_oauth_token', 'smugmug_oauth_token_secret']):

                    oauth_params = self._generate_oauth_signature('GET', url, params)

                    # Create Authorization header
                    auth_header = 'OAuth ' + ', '.join([
                        f'{urllib.parse.quote_plus(k)}="{urllib.parse.quote_plus(str(v))}"'
                        for k, v in oauth_params.items()
                    ])
                    headers['Authorization'] = auth_header

                async with self.semaphore:  # Rate limiting
                    async with session.get(url, headers=headers, params=params) as response:
                        if response.status == 429:  # Rate limited
                            wait_time = base_delay * (2 ** attempt)
                            logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                            await asyncio.sleep(wait_time)
                            continue

                        if response.status == 200:
                            return await response.json()
                        else:
                            logger.error(f"API request failed with status {response.status}: {url}")
                            return None

            except asyncio.TimeoutError:
                logger.warning(f"Request timeout for {url}, attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(base_delay * (2 ** attempt))
                    continue
            except Exception as e:
                logger.error(f"API request error for {url}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(base_delay * (2 ** attempt))
                    continue

        logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None

    async def get_user_info_async(self, session: aiohttp.ClientSession) -> Dict:
        """Get authenticated user information."""
        url = "https://api.smugmug.com/api/v2!authuser"
        response_data = await self.make_api_request(session, url)

        if not response_data:
            raise Exception("Failed to get user info")

        return response_data

    async def get_albums_async(self, session: aiohttp.ClientSession, user_uri: str) -> AsyncIterator[Dict]:
        """Get all albums for a user asynchronously."""
        albums_url = f"https://api.smugmug.com{user_uri}/!albums"

        while albums_url:
            response_data = await self.make_api_request(session, albums_url)

            if not response_data or 'Response' not in response_data:
                break

            for album in response_data['Response']['Album']:
                yield album

            # Handle pagination
            pages = response_data['Response'].get('Pages', {})
            if 'NextPage' in pages:
                albums_url = f"https://api.smugmug.com{pages['NextPage']}"
            else:
                albums_url = None

    async def get_album_images_async(self, session: aiohttp.ClientSession, album_uri: str) -> AsyncIterator[Dict]:
        """Get all images from an album with expanded size details."""
        images_url = f"https://api.smugmug.com{album_uri}/!images?_expand=ImageSizeDetails"

        while images_url:
            response_data = await self.make_api_request(session, images_url)

            if not response_data or 'Response' not in response_data:
                break

            # Process images with expanded data
            for image in response_data['Response']['AlbumImage']:
                # If we have expansions, merge the expanded data
                if 'Expansions' in response_data['Response']:
                    image_key = image.get('ImageKey')
                    # Find matching expansion by ImageKey
                    for expansion_uri, expansion_data in response_data['Response']['Expansions'].items():
                        if f"/image/{image_key}!" in expansion_uri and 'ImageSizeDetails' in expansion_data:
                            image['ExpandedImageSizeDetails'] = expansion_data['ImageSizeDetails']
                            break

                yield image

            # Handle pagination
            pages = response_data['Response'].get('Pages', {})
            if 'NextPage' in pages:
                next_page = pages['NextPage']
                if '_expand=' not in next_page:
                    images_url = f"https://api.smugmug.com{next_page}&_expand=ImageSizeDetails"
                else:
                    images_url = f"https://api.smugmug.com{next_page}"
            else:
                images_url = None

    async def get_image_sizes_async(self, session: aiohttp.ClientSession, image_uri: str) -> Optional[Dict]:
        """Get download URLs for different image sizes (fallback when expansion fails)."""
        sizes_url = f"https://api.smugmug.com{image_uri}"
        response_data = await self.make_api_request(session, sizes_url)

        if response_data and 'Response' in response_data:
            return response_data['Response']['ImageSizeDetails']
        return None

    async def download_and_upload_image(self, session: aiohttp.ClientSession, s3_session,
                                      image_url: str, s3_key: str, metadata: Dict) -> bool:
        """Download image from SmugMug and upload directly to S3."""

        # Check if already transferred
        if s3_key in self.transferred_files:
            return True

        try:
            # Check if file exists in S3
            try:
                await s3_session.head_object(
                    Bucket=self.config['s3_bucket'],
                    Key=s3_key
                )
                logger.info(f"File '{s3_key}' already exists in S3. Skipping.")
                self.save_transfer_log(s3_key)
                self.transferred_files.add(s3_key)
                return True
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    raise

            # Download and upload
            async with self.semaphore:
                logger.info(f"Downloading: {image_url}")
                async with session.get(image_url) as response:
                    if response.status != 200:
                        logger.error(f"Failed to download {image_url}: HTTP {response.status}")
                        return False

                    # Prepare S3 metadata
                    s3_metadata = self._prepare_s3_metadata(metadata)

                    # Stream directly to S3
                    logger.info(f"Uploading: {s3_key}")
                    await s3_session.upload_fileobj(
                        response.content,
                        self.config['s3_bucket'],
                        s3_key,
                        ExtraArgs={
                            'Metadata': s3_metadata,
                            'ContentType': response.headers.get('Content-Type', 'image/jpeg')
                        }
                    )

                    self.save_transfer_log(s3_key)
                    self.transferred_files.add(s3_key)
                    logger.info(f"Successfully transferred: {s3_key}")
                    return True

        except Exception as e:
            logger.error(f"Transfer failed for '{s3_key}': {e}")
            return False

    def _prepare_s3_metadata(self, metadata: Dict) -> Dict[str, str]:
        """Prepare metadata for S3 storage (string values only)."""
        s3_metadata = {}

        field_mappings = {
            'Title': 'title',
            'Caption': 'caption',
            'Date': 'date',
            'FileName': 'filename',
            'Format': 'format',
            'Keywords': 'keywords',
            'Watermarked': 'watermarked',
            'Hidden': 'hidden',
            'ArchivedUri': 'archived_uri',
            'ArchivedSize': 'archived_size',
            'ArchivedMD5': 'archived_md5'
        }

        for smugmug_field, s3_field in field_mappings.items():
            if smugmug_field in metadata and metadata[smugmug_field] is not None:
                s3_metadata[s3_field] = str(metadata[smugmug_field])

        s3_metadata['smugmug_transfer_date'] = datetime.utcnow().isoformat()
        return s3_metadata

    def generate_s3_key(self, album_name: str, image_filename: str, folder_structure: str = None) -> str:
        """Generate S3 key preserving folder structure."""
        clean_album = self._clean_name(album_name)
        clean_filename = self._clean_name(image_filename)

        if folder_structure:
            clean_folder = self._clean_name(folder_structure)
            path = os.path.join(self.config.get('s3_prefix', ''), clean_folder, clean_filename)
            return path.lstrip('/')
        else:
            path = os.path.join(self.config.get('s3_prefix', ''), clean_album, clean_filename)
            return path

    def _clean_name(self, name: str) -> str:
        """Clean name for S3 key (remove invalid characters)."""
        if not name:
            return "unknown"

        invalid_chars = ['<', '>', ':', '"', '|', '?', '*', '\\']
        for char in invalid_chars:
            name = name.replace(char, '_')

        return name.strip()

    async def process_album_async(self, session: aiohttp.ClientSession, s3_session,
                                album: Dict) -> Dict:
        """Process a single album asynchronously with concurrent image transfers."""
        album_name = album.get('Name', 'Untitled Album')
        album_uri = album['Uri']
        album_key = album['AlbumKey']

        logger.info(f"Processing album: {album_name}")

        stats = {
            'total_images': 0,
            'successful_transfers': 0,
            'failed_transfers': 0,
            'album_name': album_name
        }

        # Check if album is already cached
        if self.is_album_cached(album_key):
            logger.info(f"Album '{album_name}' already cached, skipping")
            return stats

        self.start_album_cache(album_key, album_name, album.get('UrlPath', ''))

        tasks = []
        album_files = []

        try:
            async for image in self.get_album_images_async(session, album_uri):
                stats['total_images'] += 1

                # Use expanded ImageSizeDetails if available
                if 'ExpandedImageSizeDetails' in image:
                    image_sizes = image['ExpandedImageSizeDetails']
                    image_metadata = image
                else:
                    # Fallback to individual API call
                    image_sizes = await self.get_image_sizes_async(session, image['Uris']['ImageSizeDetails']['Uri'])
                    image_metadata = image
                    if not image_sizes:
                        stats['failed_transfers'] += 1
                        continue

                # Choose best available size
                download_url = image_sizes.get('ImageSizeOriginal', {}).get('Url')
                if not download_url:
                    logger.warning(f"No download URL found for image {image['Uri']}")
                    stats['failed_transfers'] += 1
                    continue

                # Generate S3 key
                filename = image_metadata.get('FileName', f"image_{image['ImageKey']}.jpg")
                url_path = album.get('UrlPath', '')
                s3_key = self.generate_s3_key(album_name, filename, url_path)

                # Cache file info
                file_info = {
                    'id': image.get('ImageKey'),
                    'album_key': album_key,
                    'album_name': album_name,
                    'filename': filename,
                    'url': download_url,
                    'keywords': image.get('Keywords', ''),
                    'md5': image.get('ArchivedMD5', ''),
                    'size': image.get('ArchivedSize', ''),
                    'title': image.get('Title', ''),
                    'caption': image.get('Caption', ''),
                }
                logger.info(f"Adding file to cache: {album_name}/{filename} ({s3_key})")
                album_files.append(file_info)

                # Create download task
                task = self.download_and_upload_image(
                    session, s3_session, download_url, s3_key, image_metadata
                )
                tasks.append(task)

                # Process in batches to avoid overwhelming the system
                if len(tasks) >= self.batch_size:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    self._update_stats_from_results(stats, results)
                    tasks = []

            # Process remaining tasks
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                self._update_stats_from_results(stats, results)

            # Cache album files and mark as completed
            self.cache_album_files(album_files)
            self.complete_album_cache(album_key)

        except Exception as e:
            logger.error(f"Error processing album '{album_name}': {e}")
            if album_files:
                self.cache_album_files(album_files)

        return stats

    def _update_stats_from_results(self, stats: Dict, results: List):
        """Update stats based on batch results."""
        for result in results:
            if isinstance(result, Exception):
                stats['failed_transfers'] += 1
                logger.error(f"Batch transfer failed: {result}")
            elif result:
                stats['successful_transfers'] += 1
            else:
                stats['failed_transfers'] += 1

    async def process_multiple_albums_async(self, session: aiohttp.ClientSession, 
                                          albums: List[Dict]) -> Dict:
        """Process multiple albums concurrently."""

        # Setup S3 session
        s3_session = aioboto3.Session()
        async with s3_session.client(
            's3',
            endpoint_url=self.config.get('s3_endpoint_url'),
            aws_access_key_id=self.config['s3_access_key'],
            aws_secret_access_key=self.config['s3_secret_key'],
            region_name=self.config.get('s3_region', 'us-east-1')
        ) as s3_client:

            # Create album processing tasks
            album_tasks = [
                self.process_album_async(session, s3_client, album)
                for album in albums
            ]

            # Process albums concurrently
            album_results = await asyncio.gather(*album_tasks, return_exceptions=True)

            # Aggregate stats
            total_stats = {
                'total_albums': len(albums),
                'total_images': 0,
                'successful_transfers': 0,
                'failed_transfers': 0
            }

            for result in album_results:
                if isinstance(result, Exception):
                    logger.error(f"Album processing failed: {result}")
                    continue

                total_stats['total_images'] += result['total_images']
                total_stats['successful_transfers'] += result['successful_transfers']
                total_stats['failed_transfers'] += result['failed_transfers']

                logger.info(f"Album '{result['album_name']}' completed: "
                           f"{result['successful_transfers']}/{result['total_images']} successful")

            return total_stats

    # Database methods (synchronous, reused from original)
    def is_album_cached(self, album_key: str) -> bool:
        """Check if album is already cached and completed."""
        self.log_cursor.execute(
            "SELECT completed FROM album_cache WHERE album_key = ?", 
            (album_key,)
        )
        result = self.log_cursor.fetchone()
        return result is not None and result[0] == 1

    def start_album_cache(self, album_key: str, album_name: str, album_path: str):
        """Mark album as being processed."""
        try:
            self.log_cursor.execute(
                "INSERT OR REPLACE INTO album_cache (album_key, album_name, album_path, completed) VALUES (?, ?, ?, 0)",
                (album_key, album_name, album_path)
            )
            self.log_conn.commit()
        except Exception as e:
            logger.error(f"Error starting album cache for '{album_key}': {e}")

    def complete_album_cache(self, album_key: str):
        """Mark album as completed."""
        try:
            self.log_cursor.execute(
                "UPDATE album_cache SET completed = 1 WHERE album_key = ?",
                (album_key,)
            )
            self.log_conn.commit()
        except Exception as e:
            logger.error(f"Error completing album cache for '{album_key}': {e}")

    def cache_album_files(self, files: List[Dict]):
        """Cache files for an album to database."""
        try:
            for file in files:
                self.log_cursor.execute(
                    """INSERT OR REPLACE INTO file_cache 
                    (id, album_key, album_name, filename, url, keywords, md5, size, title, caption) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        file.get('id'),
                        file.get('album_key'), 
                        file.get('album_name'),
                        file.get('filename'),
                        file.get('url'),
                        file.get('keywords'),
                        file.get('md5'),
                        file.get('size'),
                        file.get('title'),
                        file.get('caption')
                    )
                )
            self.log_conn.commit()
        except Exception as e:
            logger.error(f"Error caching files: {e}")

    async def run_async(self):
        """Main async execution method."""
        logger.info("Starting async SmugMug to S3 streaming transfer")

        async with await self.setup_smugmug_session() as session:
            try:
                # Get user information
                user_info = await self.get_user_info_async(session)
                user_uri = user_info['Response']['User']['Uri']
                username = user_info['Response']['User']['Name']

                logger.info(f"Authenticated as: {username}")

                # Collect all albums
                albums = []
                async for album in self.get_albums_async(session, user_uri):
                    logger.info(f"Found album: {album['Name']}")
                    albums.append(album)

                logger.info(f"Found {len(albums)} albums to process")

                # Process albums concurrently
                total_stats = await self.process_multiple_albums_async(session, albums)

                logger.info(f"Transfer completed! Total stats: {total_stats}")

            except Exception as e:
                logger.error(f"Fatal error: {str(e)}")
                raise

    def close(self):
        """Close database connections."""
        self.log_cursor.close()
        self.log_conn.close()


def load_config() -> Dict:
    """Load configuration from environment variables."""
    config = {
        # SmugMug API configuration
        'smugmug_api_key': os.getenv('SMUGMUG_API_KEY'),
        'smugmug_oauth_consumer_key': os.getenv('SMUGMUG_OAUTH_CONSUMER_KEY'),
        'smugmug_oauth_consumer_secret': os.getenv('SMUGMUG_OAUTH_CONSUMER_SECRET'),
        'smugmug_oauth_token': os.getenv('SMUGMUG_OAUTH_TOKEN'),
        'smugmug_oauth_token_secret': os.getenv('SMUGMUG_OAUTH_TOKEN_SECRET'),

        # S3 configuration
        's3_endpoint_url': os.getenv('S3_ENDPOINT_URL'),
        's3_access_key': os.getenv('S3_ACCESS_KEY'),
        's3_secret_key': os.getenv('S3_SECRET_KEY'),
        's3_bucket': os.getenv('S3_BUCKET'),
        's3_region': os.getenv('S3_REGION', 'us-east-1'),
        's3_prefix': os.getenv('S3_PREFIX', 'smugmug/'),

        # Performance configuration
        'max_concurrent_downloads': int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '20')),
        'batch_size': int(os.getenv('BATCH_SIZE', '50')),
        'rate_limit_delay': float(os.getenv('RATE_LIMIT_DELAY', '0.01')),
    }

    # Validate required configuration
    required_fields = ['s3_access_key', 's3_secret_key', 's3_bucket']

    # Check for valid SmugMug authentication
    has_api_key = bool(config['smugmug_api_key'])
    has_oauth = all(config.get(key) for key in
                    ['smugmug_oauth_consumer_key', 'smugmug_oauth_consumer_secret',
                     'smugmug_oauth_token', 'smugmug_oauth_token_secret'])

    if not (has_api_key or has_oauth):
        logger.error("No valid SmugMug authentication method configured.")
        logger.error("Please set SMUGMUG_API_KEY or full OAuth credentials")
        raise ValueError("SmugMug authentication credentials required")

    for field in required_fields:
        if not config[field]:
            raise ValueError(f"Required configuration field {field} is missing")

    return config


async def main():
    """Main async entry point."""
    try:
        config = load_config()
        streamer = AsyncSmugMugS3Streamer(config)

        # Run the async transfer
        await streamer.run_async()

        streamer.close()
        logger.info("Async transfer completed successfully")

    except Exception as e:
        logger.error(f"Async application failed: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    exit(asyncio.run(main()))

