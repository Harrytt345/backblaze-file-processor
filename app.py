# app.py - Backblaze B2 + Render Server (CORRECTED)
import sys
import os

# Try to import required packages with error handling
try:
    from flask import Flask, request, jsonify, send_file
    import requests
    import uuid
    import tempfile
    import threading
    import time
    from werkzeug.utils import secure_filename
    from concurrent.futures import ThreadPoolExecutor
    import json
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure all required packages are installed")
    sys.exit(1)

# Try to import b2sdk with error handling
try:
    import b2sdk.v1 as b2
except ImportError as e:
    print(f"B2 SDK import error: {e}")
    print("Trying to install setuptools...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "setuptools"])
    try:
        import b2sdk.v1 as b2
    except ImportError:
        print("Failed to import b2sdk even after installing setuptools")
        sys.exit(1)

app = Flask(__name__)

# Initialize B2 - CORRECTED: Use environment variable NAMES as strings
B2_KEY_ID = os.getenv('B2_KEY_ID')                    # Will read from environment variable named 'B2_KEY_ID'
B2_APPLICATION_KEY = os.getenv('B2_APPLICATION_KEY')  # Will read from environment variable named 'B2_APPLICATION_KEY'
B2_BUCKET_NAME = os.getenv('B2_BUCKET_NAME')          # Will read from environment variable named 'B2_BUCKET_NAME'
MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE', 524288000))  # 500MB
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 10485760))          # 10MB
MAX_CONCURRENT_CHUNKS = 5

# Validate environment variables
if not all([B2_KEY_ID, B2_APPLICATION_KEY, B2_BUCKET_NAME]):
    print("Error: Missing required environment variables")
    print("Please ensure B2_KEY_ID, B2_APPLICATION_KEY, and B2_BUCKET_NAME are set")
    sys.exit(1)

# Initialize B2 SDK with error handling
try:
    info = b2.InMemoryAccountInfo()
    b2_stor = b2.B2Stor(info)
    b2_stor.authorize_account("production", B2_KEY_ID, B2_APPLICATION_KEY)
    b2_bucket = b2_stor.get_bucket_by_name(B2_BUCKET_NAME)
    print("Successfully initialized B2 SDK")
except Exception as e:
    print(f"Error initializing B2 SDK: {e}")
    sys.exit(1)

# Active downloads tracking
active_downloads = {}

class DownloadJob:
    def __init__(self, job_id, url, webhook_url=None, metadata=None):
        self.job_id = job_id
        self.url = url
        self.webhook_url = webhook_url
        self.metadata = metadata or {}
        self.status = 'initiating'
        self.chunks = []
        self.merged_file = None
        self.error = None
        self.created_at = time.time()
        self.updated_at = time.time()
        
    def to_dict(self):
        return {
            'job_id': self.job_id,
            'url': self.url,
            'status': self.status,
            'chunks': [chunk.to_dict() for chunk in self.chunks],
            'merged_file': self.merged_file,
            'error': self.error,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

class Chunk:
    def __init__(self, index, start, end):
        self.index = index
        self.start = start
        self.end = end
        self.status = 'pending'
        self.size = 0
        self.error = None
        
    def to_dict(self):
        return {
            'index': self.index,
            'start': self.start,
            'end': self.end,
            'status': self.status,
            'size': self.size,
            'error': self.error
        }

@app.route('/download', methods=['POST'])
def start_download():
    """Start a chunked download"""
    try:
        data = request.json
        url = data.get('url')
        webhook_url = data.get('webhook_url')
        metadata = data.get('metadata', {})
        
        if not url:
            return jsonify({'error': 'URL is required'}), 400
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Create job
        job = DownloadJob(job_id, url, webhook_url, metadata)
        active_downloads[job_id] = job
        
        # Start download in background
        thread = threading.Thread(target=process_download, args=(job_id,))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'job_id': job_id,
            'status': 'initiated',
            'message': 'Download started'
        })
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500

def process_download(job_id):
    """Process download in background"""
    try:
        job = active_downloads.get(job_id)
        if not job:
            return
        
        # Get file size
        head_response = requests.head(job.url, timeout=30)
        file_size = int(head_response.headers.get('content-length', '0'))
        
        if file_size > MAX_FILE_SIZE:
            raise Exception(f"File too large: {file_size} bytes (max: {MAX_FILE_SIZE})")
        
        # Calculate chunks
        num_chunks = max(1, (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE)
        
        # Create chunk objects
        for i in range(num_chunks):
            start = i * CHUNK_SIZE
            end = min(start + CHUNK_SIZE - 1, file_size - 1)
            job.chunks.append(Chunk(i, start, end))
        
        # Update job status
        job.status = 'downloading'
        job.updated_at = time.time()
        
        # Download chunks in parallel
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CHUNKS) as executor:
            futures = [executor.submit(download_chunk, job_id, i) for i in range(num_chunks)]
            
            # Wait for all chunks to complete
            for future in futures:
                future.result()
        
        # Check if all chunks completed successfully
        if all(chunk.status == 'completed' for chunk in job.chunks):
            job.status = 'downloaded'
            job.updated_at = time.time()
            
            # Trigger merge
            merge_chunks(job_id)
            
            # Send webhook if provided
            if job.webhook_url:
                send_webhook(job.webhook_url, {
                    'event': 'download_completed',
                    'job_id': job_id,
                    'job': job.to_dict()
                })
        else:
            job.status = 'partially_failed'
            job.error = 'Some chunks failed to download'
            job.updated_at = time.time()
            
            # Send webhook if provided
            if job.webhook_url:
                send_webhook(job.webhook_url, {
                    'event': 'download_failed',
                    'job_id': job_id,
                    'error': job.error
                })
                
    except Exception as e:
        job = active_downloads.get(job_id)
        if job:
            job.status = 'failed'
            job.error = str(e)
            job.updated_at = time.time()
            
            # Send webhook if provided
            if job.webhook_url:
                send_webhook(job.webhook_url, {
                    'event': 'download_failed',
                    'job_id': job_id,
                    'error': str(e)
                })

def download_chunk(job_id, chunk_index):
    """Download a single chunk"""
    try:
        job = active_downloads.get(job_id)
        if not job:
            return
        
        chunk = job.chunks[chunk_index]
        if chunk.status != 'pending':
            return
        
        # Update chunk status
        chunk.status = 'downloading'
        job.updated_at = time.time()
        
        # Download chunk with range header
        response = requests.get(
            job.url,
            headers={'Range': f'bytes={chunk.start}-{chunk.end}'},
            timeout=30,
            stream=True
        )
        
        if response.status_code != 206:  # HTTP 206 Partial Content
            raise Exception(f"Failed to download chunk: HTTP {response.status_code}")
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            
            # Write chunk to temp file
            for chunk_data in response.iter_content(chunk_size=8192):
                if chunk_data:
                    temp_file.write(chunk_data)
            
            # Upload to B2
            chunk_name = f"{job_id}.chunk.{chunk_index:04d}"
            b2_bucket.upload_file(
                temp_path,
                chunk_name,
                {
                    'job_id': job_id,
                    'chunk_index': str(chunk_index),
                    'original_url': job.url
                }
            )
            
            # Clean up temp file
            os.unlink(temp_path)
            
            # Update chunk status
            chunk.status = 'completed'
            chunk.size = os.path.getsize(temp_path)
            job.updated_at = time.time()
            
    except Exception as e:
        job = active_downloads.get(job_id)
        if job and chunk_index < len(job.chunks):
            job.chunks[chunk_index].status = 'failed'
            job.chunks[chunk_index].error = str(e)
            job.updated_at = time.time()

def merge_chunks(job_id):
    """Merge chunks into a single file"""
    try:
        job = active_downloads.get(job_id)
        if not job:
            return
        
        # Update job status
        job.status = 'merging'
        job.updated_at = time.time()
        
        # Create temporary file for merged content
        with tempfile.NamedTemporaryFile(delete=False) as merged_file:
            merged_path = merged_file.name
            
            # Download and merge chunks
            for chunk in job.chunks:
                if chunk.status != 'completed':
                    raise Exception(f"Chunk {chunk.index} not completed")
                
                # Download chunk from B2
                chunk_name = f"{job_id}.chunk.{chunk.index:04d}"
                chunk_data = b2_bucket.download_file_by_name(chunk_name)
                
                # Write to merged file
                with open(merged_path, 'ab') as f:
                    f.write(chunk_data)
            
            # Upload merged file to B2
            merged_name = f"{job_id}.merged"
            b2_bucket.upload_file(
                merged_path,
                merged_name,
                {
                    'job_id': job_id,
                    'original_url': job.url,
                    'merged_at': time.time()
                }
            )
            
            # Clean up temp file
            os.unlink(merged_path)
            
            # Update job status
            job.status = 'completed'
            job.merged_file = merged_name
            job.updated_at = time.time()
            
            # Send webhook if provided
            if job.webhook_url:
                send_webhook(job.webhook_url, {
                    'event': 'merge_completed',
                    'job_id': job_id,
                    'job': job.to_dict()
                })
                
    except Exception as e:
        job = active_downloads.get(job_id)
        if job:
            job.status = 'merge_failed'
            job.error = str(e)
            job.updated_at = time.time()
            
            # Send webhook if provided
            if job.webhook_url:
                send_webhook(job.webhook_url, {
                    'event': 'merge_failed',
                    'job_id': job_id,
                    'error': str(e)
                })

@app.route('/merge', methods=['POST'])
def trigger_merge():
    """Manually trigger merge for a job"""
    try:
        data = request.json
        job_id = data.get('job_id')
        
        if not job_id:
            return jsonify({'error': 'Job ID is required'}), 400
        
        job = active_downloads.get(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        if job.status != 'downloaded':
            return jsonify({'error': 'Job not ready for merge'}), 409
        
        # Start merge in background
        thread = threading.Thread(target=merge_chunks, args=(job_id,))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'job_id': job_id,
            'status': 'merging',
            'message': 'Merge process started'
        })
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get job status"""
    try:
        job = active_downloads.get(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify(job.to_dict())
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/get/<job_id>', methods=['GET'])
def get_merged_file(job_id):
    """Get merged file"""
    try:
        job = active_downloads.get(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        if job.status != 'completed' or not job.merged_file:
            return jsonify({'error': 'File not ready'}), 409
        
        # Download merged file from B2
        merged_data = b2_bucket.download_file_by_name(job.merged_file)
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(merged_data)
            temp_path = temp_file.name
        
        # Send file
        return send_file(
            temp_path,
            as_attachment=True,
            download_name=f"{job_id}.merged"
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        # Clean up temp file
        if 'temp_path' in locals():
            try:
                os.unlink(temp_path)
            except:
                pass

@app.route('/cleanup/<job_id>', methods=['DELETE'])
def cleanup_job(job_id):
    """Clean up job files"""
    try:
        job = active_downloads.get(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        # Delete chunks
        for chunk in job.chunks:
            chunk_name = f"{job_id}.chunk.{chunk.index:04d}"
            try:
                b2_bucket.delete_file_by_name(chunk_name)
            except:
                pass
        
        # Delete merged file
        if job.merged_file:
            try:
                b2_bucket.delete_file_by_name(job.merged_file)
            except:
                pass
        
        # Remove from active downloads
        if job_id in active_downloads:
            del active_downloads[job_id]
        
        return jsonify({
            'job_id': job_id,
            'status': 'cleaned',
            'message': 'All files cleaned up'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def send_webhook(url, data):
    """Send webhook notification"""
    try:
        response = requests.post(
            url,
            json=data,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        return response.status_code == 200
    except:
        return False

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'active_downloads': len(active_downloads)
    })

if __name__ == '__main__':
    print("Starting Flask application...")
    app.run(host='0.0.0.0', port=5000)
