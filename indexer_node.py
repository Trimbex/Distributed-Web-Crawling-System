from mpi4py import MPI
import time
import logging
import json
import os
import re
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

class SimpleInMemoryIndex:
    """A very basic in-memory inverted index."""
    
    def __init__(self):
        self.index = defaultdict(list)  # word -> list of document URLs
        self.documents = {}  # URL -> document content
        
    def tokenize(self, text):
        """Convert text to a list of tokens."""
        # Simple tokenization: lowercase and split on non-alphanumeric chars
        return re.findall(r'\w+', text.lower())
        
    def add_document(self, url, content):
        """Add a document to the index."""
        # Store the document content
        self.documents[url] = content
        
        # Tokenize
        tokens = self.tokenize(content)
        
        # Add to inverted index
        for token in set(tokens):  # Use set to avoid duplicates for the same document
            self.index[token].append(url)
            
    def search(self, query):
        """Search the index for documents matching the query."""
        tokens = self.tokenize(query)
        
        if not tokens:
            return []
            
        # Find documents containing all query tokens (AND logic)
        result_urls = set(self.index.get(tokens[0], []))
        
        for token in tokens[1:]:
            result_urls &= set(self.index.get(token, []))
            
        # Return list of matching URLs
        return list(result_urls)
        
    def save_to_file(self, filename="index.json"):
        """Save the index to a file."""
        data = {
            "index": {k: v for k, v in self.index.items()},
            "document_count": len(self.documents)
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f)
            
    def load_from_file(self, filename="index.json"):
        """Load the index from a file."""
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)
                
            # Convert loaded data to defaultdict
            self.index = defaultdict(list)
            for k, v in data["index"].items():
                self.index[k] = v
                
            logging.info(f"Loaded index with {len(self.index)} terms and {data['document_count']} documents")
            return True
        return False

def indexer_process():
    """
    Process for an indexer node.
    Receives web page content, indexes it, and handles search queries.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    logging.info(f"Indexer node started with rank {rank} of {size}")
    
    # Initialize index
    index = SimpleInMemoryIndex()
    
    # Try to load existing index
    if not index.load_from_file():
        logging.info("No existing index found. Starting with empty index.")
    
    save_interval = 60  # Save index every 60 seconds
    last_save_time = time.time()
    processed_docs = 0
    
    try:
        while True:
            status = MPI.Status()
            
            # Non-blocking probe for messages
            if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
                source = status.Get_source()
                tag = status.Get_tag()
                
                if tag == 2:  # Content from crawler nodes
                    data = comm.recv(source=source, tag=tag)
                    
                    if data is None:  # Shutdown signal
                        break
                        
                    url = data['url']
                    content = data['content']
                    
                    logging.info(f"Indexer {rank} received content from crawler {source} for URL: {url}")
                    
                    # Index the document
                    index.add_document(url, content)
                    processed_docs += 1
                    
                    # Report status to master
                    if processed_docs % 10 == 0:  # Report every 10 documents
                        comm.send(f"Indexer {rank} - Indexed {processed_docs} documents", dest=0, tag=99)
                        
                elif tag == 3:  # Search query
                    query = comm.recv(source=source, tag=tag)
                    logging.info(f"Indexer {rank} received search query: {query}")
                    
                    # Perform search
                    results = index.search(query)
                    
                    # Send results back
                    comm.send(results, dest=source, tag=4)  # Tag 4 for search results
            
            # Periodically save the index
            current_time = time.time()
            if current_time - last_save_time > save_interval:
                index.save_to_file()
                last_save_time = current_time
                logging.info(f"Saved index with {processed_docs} documents")
                
            time.sleep(0.1)  # Small delay to prevent CPU hogging
            
    except Exception as e:
        logging.error(f"Indexer {rank} error: {e}")
        
    finally:
        # Save the index before exiting
        index.save_to_file()
        logging.info(f"Indexer {rank} shutting down. Saved index with {processed_docs} documents.")

if __name__ == '__main__':
    indexer_process()