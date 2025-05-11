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
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WebCrawler | Distributed Search Engine</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
    <style>
        :root {
            --primary: #2c3e50;
            --secondary: #3498db;
            --accent: #e74c3c;
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
            background: linear-gradient(135deg, var(--primary) 0%, var(--secondary) 100%);
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
        }
        
        .brand h1 {
            font-weight: 600;
            font-size: 2rem;
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
    </style>
</head>
<body>
    <header>
        <div class="container">
            <div class="brand">
                <i class="fas fa-spider"></i>
                <h1>WebCrawler</h1>
            </div>
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
                        <i class="fas fa-spider"></i> Welcome to WebCrawler
                    </div>
                    <p style="margin-bottom: 1rem;">
                        A high-performance distributed web crawling system built with Python.
                        Enter a search query above to explore the indexed content.
                    </p>
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
                </div>
            </div>
        </div>
    </main>
    
    <footer>
        <div class="container">
            <p>Distributed Web Crawling System &copy; 2025 | Advanced Cloud Computing Project</p>
        </div>
    </footer>
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