#!/usr/bin/env python3
import os
import time
import json
import socket
import logging
import argparse
import requests
import threading
from flask import Flask, request, jsonify, render_template, redirect, url_for, flash
from threading import Lock
import whoosh.index as index
from whoosh.fields import Schema, ID, TEXT, STORED
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.analysis import StemmingAnalyzer
from whoosh import scoring
import os.path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Indexer - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("indexer.log"),
        logging.StreamHandler()
    ]
)

class IndexerNode:
    def __init__(self, index_dir="index_data", crawler_api_url=None):
        """Initialize the indexer node"""
        self.hostname = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.hostname)
        self.crawler_api_url = crawler_api_url
        self.crawler_status = self.test_crawler_connection()
        
        # Create directory for pending URLs if it doesn't exist
        self.pending_urls_file = os.path.join(index_dir, "pending_urls.txt")

        # Create or open search index
        self.index_dir = index_dir
        self.index_lock = Lock()
        self.setup_index()

        # Statistics
        self.stats = {
            "pages_indexed": 0,
            "index_size_bytes": 0,
            "searches_performed": 0,
            "seed_urls_submitted": 0,
            "pending_urls": self._count_pending_urls(),
            "crawler_status": self.crawler_status
        }

        logging.info(f"Indexer node initialized at {self.ip_address}")

    def setup_index(self):
        """Set up the search index with enhanced Whoosh configuration"""
        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)

        # Define the index schema with stemming analyzer for better search
        analyzer = StemmingAnalyzer()
        self.schema = Schema(
            url=ID(stored=True, unique=True),
            title=TEXT(stored=True, analyzer=analyzer),
            content=TEXT(stored=True, analyzer=analyzer),
            domain=ID(stored=True),
            crawl_date=STORED
        )

        # Create or open the index
        if not index.exists_in(self.index_dir):
            logging.info(f"Creating new index in {self.index_dir}")
            self.ix = index.create_in(self.index_dir, self.schema)
        else:
            logging.info(f"Opening existing index in {self.index_dir}")
            self.ix = index.open_dir(self.index_dir)

        # Update statistics
        self.update_stats()

    def add_document(self, url, title, content):
        """Add a document to the index with enhanced metadata"""
        with self.index_lock:
            writer = self.ix.writer()
            try:
                # Extract domain from URL
                domain = url.split('//')[-1].split('/')[0]
                
                writer.update_document(
                    url=url,
                    title=title,
                    content=content,
                    domain=domain,
                    crawl_date=time.strftime("%Y-%m-%d %H:%M:%S")
                )
                writer.commit()
                self.stats["pages_indexed"] += 1
                self.update_stats()
                logging.info(f"Indexed document: {url}")
                return True
            except Exception as e:
                writer.cancel()
                logging.error(f"Error indexing {url}: {e}")
                return False

    def submit_seed_url(self, url):
        """Submit a seed URL to the crawler for processing"""
        # First, check the connection status
        self.crawler_status = self.test_crawler_connection()
        self.stats["crawler_status"] = self.crawler_status
        
        if not self.crawler_api_url or not self.crawler_status["connected"]:
            logging.error(f"Crawler API unavailable: {self.crawler_status['message']}")
            self._store_pending_url(url)
            return False, f"Crawler service unavailable: {self.crawler_status['message']}. URL has been saved and will be submitted later."
            
        try:
            # Ensure URL has a scheme
            if not url.startswith(('http://', 'https://')):
                url = 'http://' + url
                
            # Submit to crawler API
            crawler_endpoint = f"{self.crawler_api_url}/submit"
            try:
                response = requests.post(
                    crawler_endpoint, 
                    json={"url": url},
                    timeout=3  # Add timeout to avoid long waits
                )
                
                if response.status_code == 200:
                    self.stats["seed_urls_submitted"] += 1
                    logging.info(f"Seed URL submitted: {url}")
                    return True, "URL successfully submitted for crawling"
                else:
                    logging.error(f"Failed to submit seed URL: {url}, status: {response.status_code}")
                    self._store_pending_url(url)
                    return False, f"Failed to submit URL to crawler, but it has been saved for later submission."
            except (requests.ConnectionError, requests.Timeout):
                logging.error(f"Connection to crawler service failed: {self.crawler_api_url}")
                self._store_pending_url(url)
                return False, "Crawler service is currently unavailable. URL has been saved and will be submitted later."
                
        except Exception as e:
            logging.error(f"Error submitting seed URL {url}: {e}")
            self._store_pending_url(url)
            return False, f"Error: {str(e)}. URL has been saved for later submission."

    def search(self, query_str, max_results=10):
        """Enhanced search with support for field-specific queries and boolean operators"""
        self.stats["searches_performed"] += 1

        with self.index_lock:
            try:
                searcher = self.ix.searcher(weighting=scoring.BM25F)
                
                # Support for field-specific queries (title:term content:term)
                parser = MultifieldParser(["title", "content"], self.ix.schema)
                query = parser.parse(query_str)

                results = searcher.search(query, limit=max_results)
                search_results = []

                for result in results:
                    # Create a better snippet with highlighted terms
                    if hasattr(result, "highlights") and "content" in result:
                        snippet = result.highlights("content", text=result["content"], top=2) or result["content"][:200] + "..."
                    else:
                        snippet = result["content"][:200] + "..." if len(result["content"]) > 200 else result["content"]
                    
                    search_results.append({
                        "url": result["url"],
                        "title": result["title"],
                        "snippet": snippet,
                        "score": result.score,
                        "domain": result.get("domain", "unknown"),
                        "crawl_date": result.get("crawl_date", "unknown")
                    })

                searcher.close()
                return search_results
            except Exception as e:
                logging.error(f"Error searching for '{query_str}': {e}")
                return []

    def update_stats(self):
        """Update index statistics"""
        try:
            # Calculate index size
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(self.index_dir):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    total_size += os.path.getsize(fp)

            self.stats["index_size_bytes"] = total_size
        except Exception as e:
            logging.error(f"Error updating stats: {e}")

    def get_status(self):
        """Get the current status of the indexer"""
        self.update_stats()
        
        # Refresh crawler connection status
        self.crawler_status = self.test_crawler_connection()
        self.stats["crawler_status"] = self.crawler_status

        # Add index statistics
        with self.index_lock:
            searcher = self.ix.searcher()
            self.stats["document_count"] = searcher.doc_count()
            self.stats["index_size_mb"] = round(self.stats["index_size_bytes"] / (1024 * 1024), 2)
            searcher.close()

        return self.stats

    def retry_pending_urls(self):
        """Attempt to submit any pending URLs to the crawler"""
        if not os.path.exists(self.pending_urls_file) or not self.crawler_api_url:
            return {"success": False, "message": "No pending URLs or crawler API not configured", "submitted": 0, "failed": 0}
            
        try:
            # Read all pending URLs
            with open(self.pending_urls_file, 'r') as f:
                pending_urls = [line.strip() for line in f.readlines() if line.strip()]
                
            if not pending_urls:
                return {"success": True, "message": "No pending URLs to submit", "submitted": 0, "failed": 0}
                
            # Try to submit each URL
            submitted = []
            failed = []
            
            for url in pending_urls:
                try:
                    response = requests.post(
                        f"{self.crawler_api_url}/submit",
                        json={"url": url},
                        timeout=3
                    )
                    
                    if response.status_code == 200:
                        submitted.append(url)
                        self.stats["seed_urls_submitted"] += 1
                    else:
                        failed.append(url)
                except Exception as e:
                    logging.error(f"Error resubmitting URL {url}: {e}")
                    failed.append(url)
            
            # Write back the failed URLs
            with open(self.pending_urls_file, 'w') as f:
                for url in failed:
                    f.write(f"{url}\n")
                    
            # Update stats
            self.stats["pending_urls"] = len(failed)
            
            return {
                "success": True,
                "message": f"Resubmitted {len(submitted)} URLs, {len(failed)} remain pending",
                "submitted": len(submitted),
                "failed": len(failed)
            }
        except Exception as e:
            logging.error(f"Error retrying pending URLs: {e}")
            return {"success": False, "message": f"Error: {str(e)}", "submitted": 0, "failed": 0}

    def _count_pending_urls(self):
        """Count the number of pending URLs"""
        try:
            if os.path.exists(self.pending_urls_file):
                with open(self.pending_urls_file, 'r') as f:
                    return len(f.readlines())
            return 0
        except Exception as e:
            logging.error(f"Error counting pending URLs: {e}")
            return 0
            
    def _store_pending_url(self, url):
        """Store a URL that couldn't be submitted due to crawler unavailability"""
        try:
            with open(self.pending_urls_file, 'a') as f:
                f.write(f"{url}\n")
            self.stats["pending_urls"] = self._count_pending_urls()
            return True
        except Exception as e:
            logging.error(f"Error storing pending URL {url}: {e}")
            return False

    def test_crawler_connection(self):
        """Test connection to the crawler service"""
        if not self.crawler_api_url:
            return {"connected": False, "message": "No crawler URL configured", "last_check": time.strftime("%Y-%m-%d %H:%M:%S")}
            
        try:
            # Try to connect to the crawler service
            response = requests.get(
                f"{self.crawler_api_url}/status", 
                timeout=2
            )
            
            if response.status_code == 200:
                return {
                    "connected": True, 
                    "message": "Connected to crawler service", 
                    "last_check": time.strftime("%Y-%m-%d %H:%M:%S")
                }
            else:
                return {
                    "connected": False, 
                    "message": f"Crawler service responded with status code {response.status_code}", 
                    "last_check": time.strftime("%Y-%m-%d %H:%M:%S")
                }
        except requests.ConnectionError:
            return {
                "connected": False, 
                "message": "Could not connect to crawler service", 
                "last_check": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            return {
                "connected": False, 
                "message": f"Error checking crawler status: {str(e)}", 
                "last_check": time.strftime("%Y-%m-%d %H:%M:%S")
            }

def start_api_server(indexer, port=5002):
    """Start a simple HTTP server to expose indexer node API"""
    app = Flask(__name__)
    app.secret_key = os.urandom(24)  # For flash messages
    
    # Disable caching and force template reloading
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    
    @app.after_request
    def add_header(response):
        """Add headers to disable caching"""
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
        return response

    @app.route('/index', methods=['POST'])
    def index_document():
        data = request.json
        url = data.get('url')
        title = data.get('title', 'No Title')
        content = data.get('content', '')

        if not url or not content:
            return jsonify({"error": "URL and content are required"}), 400

        success = indexer.add_document(url, title, content)
        return jsonify({"success": success})

    @app.route('/search', methods=['GET'])
    def search():
        query = request.args.get('q', '')
        max_results = int(request.args.get('max', 10))

        if not query:
            return jsonify({"error": "Query parameter 'q' is required"}), 400

        results = indexer.search(query, max_results)
        return jsonify({"results": results})

    @app.route('/status', methods=['GET'])
    def status():
        return jsonify(indexer.get_status())
        
    @app.route('/refresh-connection', methods=['GET', 'POST'])
    def refresh_connection():
        """Refresh the crawler connection status"""
        indexer.crawler_status = indexer.test_crawler_connection()
        indexer.stats["crawler_status"] = indexer.crawler_status
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                "success": True,
                "crawler_status": indexer.crawler_status
            })
            
        flash(f"Connection status: {indexer.crawler_status['message']}", 
              "success" if indexer.crawler_status['connected'] else "error")
        return redirect(url_for('search_interface'))

    @app.route('/retry-pending', methods=['GET', 'POST'])
    def retry_pending():
        """Manually trigger resubmission of pending URLs"""
        result = indexer.retry_pending_urls()
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify(result)
            
        if result["success"]:
            flash(result["message"], "success")
        else:
            flash(result["message"], "error")
            
        return redirect(url_for('search_interface'))

    @app.route('/submit-url', methods=['POST'])
    def submit_url():
        """Handle seed URL submission from web interface"""
        url = request.form.get('seed_url', '').strip()
        
        if not url:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({"success": False, "message": "URL is required"}), 400
            flash("URL is required", "error")
            return redirect(url_for('search_interface'))
            
        success, message = indexer.submit_seed_url(url)
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": success, "message": message})
            
        if success:
            flash(message, "success")
        else:
            flash(message, "error")
            
        return redirect(url_for('search_interface'))

    # Add a simple web interface for search
    @app.route('/', methods=['GET'])
    def search_interface():
        query = request.args.get('q', '')
        results = []
        
        if query:
            results = indexer.search(query, max_results=20)
            
        return render_template('search.html', query=query, results=results, stats=indexer.get_status())

    # Create a templates directory and search.html file if they don't exist
    templates_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    if not os.path.exists(templates_dir):
        os.makedirs(templates_dir)
        
    search_template = os.path.join(templates_dir, 'search.html')
    if not os.path.exists(search_template):
        with open(search_template, 'w') as f:
            f.write('''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NexusCrawl | Distributed Search Engine</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
    <style>
        :root {
            --primary: #2c3e50;
            --secondary: #3498db;
            --accent: #e74c3c;
            --success: #2ecc71;
            --warning: #f39c12;
            --error: #e74c3c;
            --background: #f8f9fa;
            --text: #2c3e50;
            --light-text: #7f8c8d;
            --card: #ffffff;
            --border: #ecf0f1;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: var(--background);
            color: var(--text);
            line-height: 1.6;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 0 20px;
        }
        
        header {
            background: linear-gradient(135deg, #1a2a6c 0%, #b21f1f 50%, #fdbb2d 100%);
            color: white;
            padding: 2rem 0;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        
        .brand {
            display: flex;
            align-items: center;
            margin-bottom: 1.5rem;
        }
        
        .brand i {
            font-size: 2rem;
            margin-right: 10px;
            color: #fdbb2d;
        }
        
        .brand h1 {
            font-weight: 600;
            font-size: 2rem;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        
        .tagline {
            font-size: 1.1rem;
            margin-bottom: 1.5rem;
            text-align: center;
            color: rgba(255,255,255,0.9);
        }
        
        .search-container {
            max-width: 800px;
            margin: 0 auto;
        }
        
        .search-form {
            display: flex;
            border-radius: 50px;
            background: white;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        .search-box {
            flex: 1;
            padding: 15px 25px;
            border: none;
            font-size: 1.1rem;
            outline: none;
        }
        
        .search-button {
            background: var(--accent);
            color: white;
            border: none;
            padding: 0 30px;
            font-size: 1.1rem;
            cursor: pointer;
            transition: background 0.3s;
        }
        
        .search-button:hover {
            background: #c0392b;
        }
        
        .main-content {
            padding: 2rem 0;
        }
        
        .results-info {
            margin-bottom: 1.5rem;
            font-weight: 600;
            color: var(--primary);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .results-count {
            padding: 4px 12px;
            background: var(--secondary);
            color: white;
            border-radius: 20px;
            font-size: 0.9rem;
        }
        
        .results-container {
            display: grid;
            gap: 1.5rem;
        }
        
        .result {
            background: var(--card);
            border-radius: 8px;
            padding: 1.5rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
            transition: transform 0.2s, box-shadow 0.2s;
            border-left: 4px solid var(--secondary);
        }
        
        .result:hover {
            transform: translateY(-3px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        
        .result h3 {
            margin-bottom: 0.5rem;
            font-weight: 600;
        }
        
        .result h3 a {
            color: var(--secondary);
            text-decoration: none;
        }
        
        .result h3 a:hover {
            text-decoration: underline;
        }
        
        .url {
            color: #27ae60;
            font-size: 0.9rem;
            margin-bottom: 0.5rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        
        .snippet {
            font-size: 0.95rem;
            margin-bottom: 1rem;
            color: var(--text);
            line-height: 1.5;
        }
        
        .match {
            background-color: #fffacd;
            padding: 0 2px;
            font-weight: 600;
        }
        
        .meta {
            display: flex;
            gap: 1rem;
            color: var(--light-text);
            font-size: 0.8rem;
            border-top: 1px solid var(--border);
            padding-top: 0.8rem;
        }
        
        .meta-item {
            display: flex;
            align-items: center;
        }
        
        .meta-item i {
            margin-right: 4px;
        }
        
        .stats-card {
            background: var(--card);
            border-radius: 8px;
            padding: 1.5rem;
            margin-top: 2rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        }
        
        .stats-title {
            font-size: 1.2rem;
            margin-bottom: 1rem;
            color: var(--primary);
            font-weight: 600;
            display: flex;
            align-items: center;
        }
        
        .stats-title i {
            margin-right: 8px;
            color: var(--secondary);
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
        }
        
        .stat-item {
            background: #f1f5f9;
            border-radius: 8px;
            padding: 1rem;
            text-align: center;
        }
        
        .stat-value {
            font-size: 1.8rem;
            font-weight: 700;
            color: var(--secondary);
            margin-bottom: 0.3rem;
        }
        
        .stat-label {
            color: var(--light-text);
            font-size: 0.9rem;
        }
        
        footer {
            text-align: center;
            padding: 2rem 0;
            margin-top: 2rem;
            color: var(--light-text);
            font-size: 0.9rem;
            border-top: 1px solid var(--border);
        }
        
        @media (max-width: 768px) {
            .search-form {
                flex-direction: column;
                border-radius: 8px;
            }
            
            .search-button {
                padding: 12px;
            }
            
            .meta {
                flex-direction: column;
                gap: 0.5rem;
            }
        }
        
        .no-results {
            background: var(--card);
            border-radius: 8px;
            padding: 2rem;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        }
        
        .no-results i {
            font-size: 3rem;
            color: var(--light-text);
            margin-bottom: 1rem;
        }
        
        .no-results h3 {
            font-weight: 600;
            margin-bottom: 0.5rem;
        }
        
        .no-results p {
            color: var(--light-text);
        }
        
        /* Seed URL submission form */
        .seed-url-form {
            margin-top: 2rem;
            background: var(--card);
            border-radius: 8px;
            padding: 1.5rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        }
        
        .form-title {
            font-size: 1.2rem;
            margin-bottom: 1rem;
            color: var(--primary);
            font-weight: 600;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        
        .form-title i {
            margin-right: 8px;
            color: var(--secondary);
        }
        
        .crawler-status-indicator {
            font-size: 0.85rem;
            display: flex;
            align-items: center;
            padding: 4px 10px;
            border-radius: 50px;
            background: #f5f5f5;
            margin-left: auto;
        }
        
        .crawler-status-indicator.connected {
            background-color: rgba(46, 204, 113, 0.15);
            color: #27ae60;
        }
        
        .crawler-status-indicator.disconnected {
            background-color: rgba(231, 76, 60, 0.15);
            color: #c0392b;
        }
        
        .crawler-status-indicator i {
            margin-right: 5px;
            color: inherit;
        }
        
        .crawler-status-indicator span {
            margin-right: 5px;
        }
        
        .refresh-status {
            color: inherit;
            opacity: 0.7;
            transition: all 0.2s ease;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 20px;
            height: 20px;
        }
        
        .refresh-status:hover {
            opacity: 1;
            transform: rotate(180deg);
        }
        
        .form-group {
            display: flex;
            margin-bottom: 1rem;
        }
        
        .form-control {
            flex: 1;
            padding: 12px 15px;
            border: 1px solid var(--border);
            border-radius: 4px 0 0 4px;
            font-size: 1rem;
            outline: none;
        }
        
        .form-control:focus {
            border-color: var(--secondary);
        }
        
        .btn-submit {
            background: var(--secondary);
            color: white;
            border: none;
            padding: 0 20px;
            border-radius: 0 4px 4px 0;
            font-size: 1rem;
            cursor: pointer;
            transition: background 0.3s;
        }
        
        .btn-submit:hover {
            background: #2980b9;
        }
        
        .form-help {
            font-size: 0.9rem;
            color: var(--light-text);
        }
        
        /* Flash messages */
        .flash-messages {
            margin-bottom: 1.5rem;
        }
        
        .flash {
            padding: 12px 15px;
            border-radius: 4px;
            margin-bottom: 1rem;
            font-size: 0.95rem;
        }
        
        .flash-success {
            background-color: rgba(46, 204, 113, 0.15);
            border-left: 4px solid var(--success);
            color: #27ae60;
        }
        
        .flash-error {
            background-color: rgba(231, 76, 60, 0.15);
            border-left: 4px solid var(--error);
            color: #c0392b;
        }
        
        .flash-warning {
            background-color: rgba(243, 156, 18, 0.15);
            border-left: 4px solid var(--warning);
            color: #d35400;
        }
        
        /* Seed URL Examples Styling */
        .seed-example-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 1rem;
        }
        
        .seed-example {
            display: flex;
            align-items: center;
            background: #f8f9fa;
            padding: 1rem;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s ease;
        }
        
        .seed-example:hover {
            background: #e9f0f8;
            transform: translateY(-2px);
        }
        
        .seed-icon {
            font-size: 1.5rem;
            width: 40px;
            height: 40px;
            display: flex;
            align-items: center;
            justify-content: center;
            background: var(--secondary);
            color: white;
            border-radius: 8px;
            margin-right: 0.8rem;
        }
        
        .seed-info {
            flex: 1;
        }
        
        .seed-name {
            font-weight: 600;
            color: var(--primary);
        }
        
        .seed-url {
            font-size: 0.85rem;
            color: var(--light-text);
        }
        
        .copy-icon {
            color: var(--light-text);
            padding: 0.5rem;
            font-size: 1.1rem;
        }
        
        .seed-example:hover .copy-icon {
            color: var(--secondary);
        }
    </style>
</head>
<body>
    <header>
        <div class="container">
            <div class="brand">
                <i class="fas fa-spider"></i>
                <h1>NexusCrawl</h1>
            </div>
            <div class="tagline">Unveiling the web's hidden connections</div>
            <div class="search-container">
                <form action="/" method="get" class="search-form">
                    <input type="text" name="q" class="search-box" value="{{ query }}" placeholder="Search the distributed web...">
                    <button type="submit" class="search-button">
                        <i class="fas fa-search"></i> Search
                    </button>
                </form>
            </div>
        </div>
    </header>
    
    <main class="main-content">
        <div class="container">
            <!-- Flash Messages -->
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    <div class="flash-messages">
                        {% for category, message in messages %}
                            <div class="flash flash-{{ category }}">{{ message }}</div>
                        {% endfor %}
                    </div>
                {% endif %}
            {% endwith %}
            
            <!-- Add Seed URL Form -->
            <div class="seed-url-form">
                <div class="form-title">
                    <i class="fas fa-plus-circle"></i> Add New URL to Crawl
                    <div class="crawler-status-indicator {% if stats.crawler_status.connected %}connected{% else %}disconnected{% endif %}" 
                         title="{{ stats.crawler_status.message }} (last checked: {{ stats.crawler_status.last_check }})">
                        <i class="fas {% if stats.crawler_status.connected %}fa-check-circle{% else %}fa-exclamation-circle{% endif %}"></i>
                        <span>Crawler {{ 'Online' if stats.crawler_status.connected else 'Offline' }}</span>
                        <a href="/refresh-connection" class="refresh-status" title="Refresh connection status">
                            <i class="fas fa-sync-alt"></i>
                        </a>
                    </div>
                </div>
                <form action="/submit-url" method="post" id="seed-form">
                    <div class="form-group">
                        <input type="text" name="seed_url" class="form-control" placeholder="Enter a URL to crawl (e.g., https://example.com)" required>
                        <button type="submit" class="btn-submit">Submit URL</button>
                    </div>
                    <p class="form-help">Add a new website to be crawled and indexed by our distributed system. If the crawler service is unavailable, URLs will be stored locally and submitted later.</p>
                </form>
                {% if stats.pending_urls > 0 %}
                <div style="margin-top: 1rem; text-align: right;">
                    <form action="/retry-pending" method="post">
                        <button type="submit" class="btn-submit" style="border-radius: 4px;">
                            <i class="fas fa-sync-alt"></i> Retry {{ stats.pending_urls }} Pending URLs
                        </button>
                    </form>
                </div>
                {% endif %}
            </div>
            
            {% if query %}
                <div class="results-info">
                    <h2>Results for "{{ query }}"</h2>
                    {% if results %}
                        <span class="results-count">{{ results|length }} results</span>
                    {% endif %}
                </div>
                
                {% if results %}
                    <div class="results-container">
                        {% for result in results %}
                            <article class="result">
                                <h3><a href="{{ result.url }}" target="_blank">{{ result.title }}</a></h3>
                                <div class="url">{{ result.url }}</div>
                                <div class="snippet">{{ result.snippet|safe }}</div>
                                <div class="meta">
                                    <div class="meta-item">
                                        <i class="fas fa-globe"></i>
                                        {{ result.domain }}
                                    </div>
                                    <div class="meta-item">
                                        <i class="far fa-calendar"></i>
                                        {{ result.crawl_date }}
                                    </div>
                                    <div class="meta-item">
                                        <i class="fas fa-chart-line"></i>
                                        Score: {{ "%.2f"|format(result.score) }}
                                    </div>
                                </div>
                            </article>
                        {% endfor %}
                    </div>
                {% else %}
                    <div class="no-results">
                        <i class="fas fa-search"></i>
                        <h3>No results found</h3>
                        <p>Try different keywords or check your spelling</p>
                    </div>
                {% endif %}
            {% else %}
                <div class="stats-card">
                    <div class="stats-title">
                        <i class="fas fa-spider"></i> Welcome to NexusCrawl
                    </div>
                    <p style="margin-bottom: 1rem;">
                        A high-performance distributed web crawling system built with Python.
                        Enter a search query above to explore the indexed content.
                    </p>
                </div>
                
                <!-- Seed URL Examples -->
                <div class="stats-card" style="margin-top: 1.5rem;">
                    <div class="stats-title">
                        <i class="fas fa-lightbulb"></i> Try These Seed URLs
                    </div>
                    <p style="margin-bottom: 1rem;">
                        Not sure where to start? Try adding these popular websites to the crawler:
                    </p>
                    <div class="seed-examples">
                        <div class="seed-example-grid">
                            <div class="seed-example" onclick="copyToClipboard('https://en.wikipedia.org')">
                                <div class="seed-icon"><i class="fab fa-wikipedia-w"></i></div>
                                <div class="seed-info">
                                    <div class="seed-name">Wikipedia</div>
                                    <div class="seed-url">https://en.wikipedia.org</div>
                                </div>
                                <div class="copy-icon"><i class="far fa-copy"></i></div>
                            </div>
                            <div class="seed-example" onclick="copyToClipboard('https://news.ycombinator.com')">
                                <div class="seed-icon"><i class="fab fa-hacker-news"></i></div>
                                <div class="seed-info">
                                    <div class="seed-name">Hacker News</div>
                                    <div class="seed-url">https://news.ycombinator.com</div>
                                </div>
                                <div class="copy-icon"><i class="far fa-copy"></i></div>
                            </div>
                            <div class="seed-example" onclick="copyToClipboard('https://github.com')">
                                <div class="seed-icon"><i class="fab fa-github"></i></div>
                                <div class="seed-info">
                                    <div class="seed-name">GitHub</div>
                                    <div class="seed-url">https://github.com</div>
                                </div>
                                <div class="copy-icon"><i class="far fa-copy"></i></div>
                            </div>
                            <div class="seed-example" onclick="copyToClipboard('https://stackoverflow.com')">
                                <div class="seed-icon"><i class="fab fa-stack-overflow"></i></div>
                                <div class="seed-info">
                                    <div class="seed-name">Stack Overflow</div>
                                    <div class="seed-url">https://stackoverflow.com</div>
                                </div>
                                <div class="copy-icon"><i class="far fa-copy"></i></div>
                            </div>
                            <div class="seed-example" onclick="copyToClipboard('https://www.reddit.com')">
                                <div class="seed-icon"><i class="fab fa-reddit-alien"></i></div>
                                <div class="seed-info">
                                    <div class="seed-name">Reddit</div>
                                    <div class="seed-url">https://www.reddit.com</div>
                                </div>
                                <div class="copy-icon"><i class="far fa-copy"></i></div>
                            </div>
                            <div class="seed-example" onclick="copyToClipboard('https://developer.mozilla.org')">
                                <div class="seed-icon"><i class="fab fa-firefox-browser"></i></div>
                                <div class="seed-info">
                                    <div class="seed-name">MDN Web Docs</div>
                                    <div class="seed-url">https://developer.mozilla.org</div>
                                </div>
                                <div class="copy-icon"><i class="far fa-copy"></i></div>
                            </div>
                        </div>
                    </div>
                </div>
            {% endif %}
            
            <div class="stats-card">
                <div class="stats-title">
                    <i class="fas fa-chart-pie"></i> System Statistics
                </div>
                <div class="stats-grid">
                    <div class="stat-item">
                        <div class="stat-value">{{ stats.pages_indexed }}</div>
                        <div class="stat-label">Documents Indexed</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value">{{ stats.index_size_mb }}</div>
                        <div class="stat-label">Index Size (MB)</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value">{{ stats.searches_performed }}</div>
                        <div class="stat-label">Searches Performed</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value">{{ stats.seed_urls_submitted }}</div>
                        <div class="stat-label">URLs Submitted</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value">{{ stats.pending_urls }}</div>
                        <div class="stat-label">Pending URLs</div>
                    </div>
                </div>
            </div>
        </div>
    </main>
    
    <footer>
        <div class="container">
            <p>NexusCrawl - Distributed Web Crawling System &copy; 2025 | Advanced Cloud Computing Project</p>
        </div>
    </footer>
    
    <script>
        // Optional: Add JavaScript for AJAX form submission
        document.addEventListener('DOMContentLoaded', function() {
            const seedForm = document.getElementById('seed-form');
            
            // Uncomment this if you want AJAX submission instead of page reload
            /*
            seedForm.addEventListener('submit', function(e) {
                e.preventDefault();
                
                const url = seedForm.querySelector('input[name="seed_url"]').value;
                
                fetch('/submit-url', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-Requested-With': 'XMLHttpRequest'
                    },
                    body: `seed_url=${encodeURIComponent(url)}`
                })
                .then(response => response.json())
                .then(data => {
                    // Create flash message dynamically
                    const flashContainer = document.querySelector('.flash-messages');
                    if (!flashContainer) {
                        const container = document.querySelector('.container');
                        const newFlashContainer = document.createElement('div');
                        newFlashContainer.className = 'flash-messages';
                        container.prepend(newFlashContainer);
                    }
                    
                    const flashElement = document.createElement('div');
                    flashElement.className = `flash flash-${data.success ? 'success' : 'error'}`;
                    flashElement.textContent = data.message;
                    
                    document.querySelector('.flash-messages').appendChild(flashElement);
                    
                    // Clear form if successful
                    if (data.success) {
                        seedForm.reset();
                    }
                    
                    // Auto-remove flash after 5 seconds
                    setTimeout(() => {
                        flashElement.remove();
                    }, 5000);
                })
                .catch(error => {
                    console.error('Error:', error);
                });
            });
            */
        });
        
        // Function to copy seed URL to clipboard and to the form
        function copyToClipboard(text) {
            // Copy to clipboard
            navigator.clipboard.writeText(text).then(() => {
                // Also fill the form input
                const seedInput = document.querySelector('input[name="seed_url"]');
                if (seedInput) {
                    seedInput.value = text;
                    seedInput.focus();
                    
                    // Create a flash message
                    const flashContainer = document.querySelector('.flash-messages');
                    if (!flashContainer) {
                        const container = document.querySelector('.container');
                        const newFlashContainer = document.createElement('div');
                        newFlashContainer.className = 'flash-messages';
                        container.prepend(newFlashContainer);
                    }
                    
                    const flashElement = document.createElement('div');
                    flashElement.className = 'flash flash-success';
                    flashElement.textContent = `Copied "${text}" to form. Click Submit to add it!`;
                    
                    document.querySelector('.flash-messages').appendChild(flashElement);
                    
                    // Scroll to the form
                    document.querySelector('.seed-url-form').scrollIntoView({ behavior: 'smooth' });
                    
                    // Auto-remove flash after 5 seconds
                    setTimeout(() => {
                        flashElement.remove();
                    }, 5000);
                }
            }).catch(err => {
                console.error('Failed to copy: ', err);
            });
        }
    </script>
</body>
</html>
            ''')

    app.run(host='0.0.0.0', port=port)

def main():
    parser = argparse.ArgumentParser(description='Indexer Node for Distributed Web Crawler')
    parser.add_argument('--port', type=int, default=5002, help='Port for the API server')
    parser.add_argument('--index-dir', default='index_data', help='Directory to store the index')
    parser.add_argument('--crawler-api', default='http://172.31.29.194:5001', help='URL for the crawler API')
    args = parser.parse_args()

    indexer = IndexerNode(args.index_dir, args.crawler_api)
    
    # Try to resubmit any pending URLs on startup
    try:
        result = indexer.retry_pending_urls()
        if result["submitted"] > 0:
            logging.info(f"Startup: Resubmitted {result['submitted']} pending URLs, {result['failed']} remain pending")
    except Exception as e:
        logging.error(f"Error retrying pending URLs on startup: {e}")
        
    # Start background connection monitor thread
    def connection_monitor():
        while True:
            try:
                indexer.crawler_status = indexer.test_crawler_connection()
                indexer.stats["crawler_status"] = indexer.crawler_status
                if indexer.crawler_status["connected"]:
                    # If connection is restored, try to submit pending URLs
                    if indexer.stats["pending_urls"] > 0:
                        result = indexer.retry_pending_urls()
                        if result["submitted"] > 0:
                            logging.info(f"Auto-submitted {result['submitted']} pending URLs, {result['failed']} remain pending")
                # Check every 60 seconds
                time.sleep(60)
            except Exception as e:
                logging.error(f"Error in connection monitor: {e}")
                time.sleep(60)  # Keep trying even if there's an error
                
    # Start the monitor thread as a daemon so it exits when the main program exits
    monitor_thread = threading.Thread(target=connection_monitor, daemon=True)
    monitor_thread.start()
    
    start_api_server(indexer, args.port)

if __name__ == "__main__":
    main()