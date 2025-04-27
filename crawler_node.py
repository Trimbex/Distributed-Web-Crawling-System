from mpi4py import MPI
import requests
from bs4 import BeautifulSoup
import time
import logging
import re
import os
import urllib.robotparser
from urllib.parse import urlparse, urljoin

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

class Crawler:
    def __init__(self, rank):
        self.rank = rank
        self.robots_cache = {}  # Cache for robots.txt parsing
        self.crawl_delays = {}  # Store crawl delays by domain
        self.user_agent = "DistributedCrawlerBot/1.0"
        
    def fetch_page(self, url):
        """Fetch a web page and return its content."""
        try:
            # Check robots.txt first
            if not self.can_fetch(url):
                logging.warning(f"Crawler {self.rank}: robots.txt disallows fetching {url}")
                return None, []
                
            # Respect crawl delay
            domain = urlparse(url).netloc
            if domain in self.crawl_delays:
                time.sleep(self.crawl_delays[domain])
            else:
                time.sleep(1)  # Default politeness delay
                
            headers = {'User-Agent': self.user_agent}
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                return response.text, response.url
            else:
                logging.warning(f"Crawler {self.rank}: Failed to fetch {url}, status code: {response.status_code}")
                return None, []
        except Exception as e:
            logging.error(f"Crawler {self.rank}: Error fetching {url}: {e}")
            return None, []
    
    def can_fetch(self, url):
        """Check if robots.txt allows this URL to be fetched."""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        scheme = parsed_url.scheme
        
        if not domain:  # Invalid URL
            return False
            
        # Check if robots parser is already in cache
        if domain not in self.robots_cache:
            robots_parser = urllib.robotparser.RobotFileParser()
            robots_url = f"{scheme}://{domain}/robots.txt"
            
            try:
                robots_parser.set_url(robots_url)
                robots_parser.read()
                self.robots_cache[domain] = robots_parser
                
                # Get crawl delay
                crawl_delay = robots_parser.crawl_delay(self.user_agent)
                if crawl_delay:
                    self.crawl_delays[domain] = max(1, crawl_delay)  # Ensure at least 1 second
                
            except Exception as e:
                logging.warning(f"Crawler {self.rank}: Error fetching robots.txt for {domain}: {e}")
                # If we can't fetch robots.txt, we'll assume it's OK to crawl but be conservative
                self.crawl_delays[domain] = 3  # Conservative delay if robots.txt fails
                return True
                
        return self.robots_cache[domain].can_fetch(self.user_agent, url)
    
    def extract_urls(self, html, base_url):
        """Extract URLs from HTML content."""
        if not html:
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        extracted_urls = []
        
        # Find all link elements
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Create absolute URL
            absolute_url = urljoin(base_url, href)
            # Filter out non-HTTP URLs (like mailto: javascript:, etc.)
            if absolute_url.startswith(('http://', 'https://')):
                extracted_urls.append(absolute_url)
                
        return extracted_urls
        
    def extract_text(self, html):
        """Extract readable text content from HTML."""
        if not html:
            return ""
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script and style elements
        for script_or_style in soup(['script', 'style']):
            script_or_style.decompose()
            
        # Get text
        text = soup.get_text()
        
        # Break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        # Break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # Drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)
        
        return text

def crawler_process():
    """
    Process for a crawler node.
    Fetches web pages, extracts URLs, and sends results back to the master.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    crawler = Crawler(rank)
    logging.info(f"Crawler node started with rank {rank} of {size}")
    
    while True:
        status = MPI.Status()
        url_to_crawl = comm.recv(source=0, tag=0, status=status)  # Receive URL from master (tag 0)
        
        if not url_to_crawl:  # Could be a shutdown signal
            logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break
            
        logging.info(f"Crawler {rank} received URL: {url_to_crawl}")
        
        try:
            # Fetch web page
            html_content, final_url = crawler.fetch_page(url_to_crawl)
            
            if html_content:
                # Extract URLs from the page
                extracted_urls = crawler.extract_urls(html_content, final_url or url_to_crawl)
                
                # Extract text content for indexing
                extracted_text = crawler.extract_text(html_content)
                
                # Send extracted URLs back to master
                comm.send(extracted_urls, dest=0, tag=1)  # Tag 1 for sending extracted URLs
                
                # Send extracted content to indexer node
                # Assuming rank size-1 is the indexer (adjust as needed)
                indexer_rank = size - 1
                # Package data to include both the URL and content
                indexed_data = {
                    'url': final_url or url_to_crawl,
                    'content': extracted_text
                }
                comm.send(indexed_data, dest=indexer_rank, tag=2)  # Tag 2 for sending content to indexer
                
                logging.info(f"Crawler {rank} crawled {url_to_crawl}, extracted {len(extracted_urls)} URLs, sent content to indexer.")
                
                # Send status update to master
                comm.send(f"Crawler {rank} - Successfully crawled: {url_to_crawl}", dest=0, tag=99)
            else:
                # Send empty list if crawling failed
                comm.send([], dest=0, tag=1)
                comm.send(f"Crawler {rank} - Failed to crawl: {url_to_crawl}", dest=0, tag=99)
                
        except Exception as e:
            logging.error(f"Crawler {rank} error crawling {url_to_crawl}: {e}")
            # Send empty list on error
            comm.send([], dest=0, tag=1)
            # Report error to master
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)

if __name__ == '__main__':
    crawler_process()