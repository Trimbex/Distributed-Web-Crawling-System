#!/usr/bin/env python3
import os
import time
import json
import socket
import logging
import argparse
from flask import Flask, request, jsonify, render_template
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
    def __init__(self, index_dir="index_data"):
        """Initialize the indexer node"""
        self.hostname = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.hostname)

        # Create or open search index
        self.index_dir = index_dir
        self.index_lock = Lock()
        self.setup_index()

        # Statistics
        self.stats = {
            "pages_indexed": 0,
            "index_size_bytes": 0,
            "searches_performed": 0
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

        # Add index statistics
        with self.index_lock:
            searcher = self.ix.searcher()
            self.stats["document_count"] = searcher.doc_count()
            self.stats["index_size_mb"] = round(self.stats["index_size_bytes"] / (1024 * 1024), 2)
            searcher.close()

        return self.stats

def start_api_server(indexer, port=5002):
    """Start a simple HTTP server to expose indexer node API"""
    app = Flask(__name__)

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
<html>
<head>
    <title>Distributed Web Crawler - Search</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; }
        .search-container { margin-bottom: 20px; }
        .search-box { padding: 10px; width: 70%; font-size: 16px; }
        .search-button { padding: 10px 20px; font-size: 16px; }
        .result { margin-bottom: 20px; padding: 10px; border-bottom: 1px solid #ddd; }
        .result h3 { margin-top: 0; }
        .result .url { color: green; font-size: 14px; }
        .result .snippet { font-size: 14px; }
        .result .meta { color: #777; font-size: 12px; }
        .stats { margin-top: 30px; font-size: 12px; color: #777; }
    </style>
</head>
<body>
    <h1>Distributed Web Crawler Search</h1>
    
    <div class="search-container">
        <form action="/" method="get">
            <input type="text" name="q" class="search-box" value="{{ query }}" placeholder="Enter search query...">
            <button type="submit" class="search-button">Search</button>
        </form>
    </div>
    
    {% if query %}
        <h2>Search Results for "{{ query }}"</h2>
        {% if results %}
            {% for result in results %}
                <div class="result">
                    <h3><a href="{{ result.url }}">{{ result.title }}</a></h3>
                    <div class="url">{{ result.url }}</div>
                    <div class="snippet">{{ result.snippet|safe }}</div>
                    <div class="meta">
                        Domain: {{ result.domain }} | 
                        Crawled: {{ result.crawl_date }} | 
                        Score: {{ result.score }}
                    </div>
                </div>
            {% endfor %}
        {% else %}
            <p>No results found.</p>
        {% endif %}
    {% endif %}
    
    <div class="stats">
        <h3>Indexer Statistics</h3>
        <p>Documents indexed: {{ stats.pages_indexed }}</p>
        <p>Index size: {{ stats.index_size_mb }} MB</p>
        <p>Searches performed: {{ stats.searches_performed }}</p>
    </div>
</body>
</html>
            ''')

    app.run(host='0.0.0.0', port=port)

def main():
    parser = argparse.ArgumentParser(description='Indexer Node for Distributed Web Crawler')
    parser.add_argument('--port', type=int, default=5002, help='Port for the API server')
    parser.add_argument('--index-dir', default='index_data', help='Directory to store the index')
    args = parser.parse_args()

    indexer = IndexerNode(args.index_dir)
    start_api_server(indexer, args.port)

if __name__ == "__main__":
    main()