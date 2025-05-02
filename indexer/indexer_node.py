#!/usr/bin/env python3
import os
import time
import json
import socket
import logging
import argparse
from flask import Flask, request, jsonify
from threading import Lock
import whoosh.index as index
from whoosh.fields import Schema, ID, TEXT
from whoosh.qparser import QueryParser
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
        """Set up the search index"""
        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)

        # Define the index schema
        self.schema = Schema(
            url=ID(stored=True, unique=True),
            title=TEXT(stored=True),
            content=TEXT(stored=True)
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
        """Add a document to the index"""
        with self.index_lock:
            writer = self.ix.writer()
            try:
                writer.update_document(
                    url=url,
                    title=title,
                    content=content
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
        """Search the index for documents matching the query"""
        self.stats["searches_performed"] += 1

        with self.index_lock:
            try:
                searcher = self.ix.searcher(weighting=scoring.BM25F)
                parser = QueryParser("content", self.ix.schema)
                query = parser.parse(query_str)

                results = searcher.search(query, limit=max_results)
                search_results = []

                for result in results:
                    search_results.append({
                        "url": result["url"],
                        "title": result["title"],
                        "snippet": result["content"][:200] + "..." if len(result["content"]) > 200 else result["content"],
                        "score": result.score
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