#!/usr/bin/env python3
import os
import time
import json
import socket
import logging
import argparse
import requests
import threading
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Crawler - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)

class RobotsCache:
    """Cache for robots.txt files to avoid repeated fetches"""
    def __init__(self, cache_timeout=3600):
        self.cache = {}  # {domain: (parser, timestamp)}
        self.cache_timeout = cache_timeout

    def can_fetch(self, url, user_agent='*'):
        """Check if a URL can be fetched according to robots.txt rules"""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc

        # Check if we need to fetch/update robots.txt
        current_time = time.time()
        if domain not in self.cache or (current_time - self.cache[domain][1]) > self.cache_timeout:
            robots_url = f"{parsed_url.scheme}://{domain}/robots.txt"

            try:
                logging.info(f"Fetching robots.txt from {robots_url}")
                parser = RobotFileParser()
                parser.set_url(robots_url)
                parser.read()
                self.cache[domain] = (parser, current_time)
            except Exception as e:
                logging.warning(f"Error fetching robots.txt from {domain}: {e}")
                # If we can't fetch robots.txt, we'll assume it's allowed
                return True

        parser = self.cache[domain][0]
        return parser.can_fetch(user_agent, url)

class CrawlerNode:
    def __init__(self, master_url, user_agent="DistributedCrawler/1.0"):
        """Initialize the crawler node"""
        self.hostname = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.hostname)
        self.master_url = master_url
        self.user_agent = user_agent
        self.robots_cache = RobotsCache()

        # Initialize session for requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent
        })

        # Statistics
        self.stats = {
            "pages_crawled": 0,
            "pages_failed": 0,
            "urls_extracted": 0
        }

        logging.info(f"Crawler node initialized at {self.ip_address}")
        logging.info(f"Connected to master at {self.master_url}")

    def get_task(self):
        """Request a task from the master node"""
        try:
            response = requests.post(f"{self.master_url}/assign_task",
                json={"crawler_ip": self.ip_address},
                timeout=5)

            if response.status_code == 200:
                task = response.json()
                if "task_id" in task:
                    return task

            return None
        except Exception as e:
            logging.error(f"Error getting task from master: {e}")
            return None

    def submit_result(self, task_id, url, success, extracted_urls=None, error=None):
        """Submit crawl results back to the master node"""
        data = {
            "task_id": task_id,
            "url": url,
            "success": success
        }

        if extracted_urls:
            data["extracted_urls"] = extracted_urls

        if error:
            data["error"] = str(error)

        try:
            response = requests.post(f"{self.master_url}/submit_result",
                json=data,
                timeout=5)

            if response.status_code == 200:
                return True
            else:
                logging.error(f"Failed to submit result for task {task_id}: {response.text}")
                return False
        except Exception as e:
            logging.error(f"Error submitting result for task {task_id}: {e}")
            return False

    def crawl_url(self, url):
        """Crawl a URL and extract content and links"""
        # Check robots.txt rules
        if not self.robots_cache.can_fetch(url, self.user_agent):
            logging.info(f"Robots.txt disallows crawling {url}")
            return False, [], "Disallowed by robots.txt"

        try:
            logging.info(f"Crawling URL: {url}")

            # Add a small delay for politeness
            time.sleep(1)

            response = self.session.get(url, timeout=10)

            if response.status_code != 200:
                return False, [], f"HTTP Error: {response.status_code}"

            # Parse HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract all links
            extracted_urls = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Handle relative URLs
                absolute_url = urljoin(url, href)
                # Remove fragments
                absolute_url = absolute_url.split('#')[0]
                if absolute_url.startswith(('http://', 'https://')):
                    extracted_urls.append(absolute_url)

            # Extract text content for indexing (to be sent to indexer)
            title = soup.title.string if soup.title else "No Title"
            text = soup.get_text(separator=' ', strip=True)

            # In a real system, we would send the content to the indexer
            # For now, just log it
            logging.info(f"Extracted {len(extracted_urls)} URLs from {url}")

            # Send content to indexer
            self.send_to_indexer(url, title, text)

            return True, extracted_urls, None

        except RequestException as e:
            logging.error(f"Request error crawling {url}: {e}")
            return False, [], f"Request error: {str(e)}"
        except Exception as e:
            logging.error(f"Error crawling {url}: {e}")
            return False, [], f"Error: {str(e)}"

    def send_to_indexer(self, url, title, content):
        """Send crawled content to the indexer node"""
        # In a real implementation, you would send this to the indexer node
        # For now, we'll just log it
        content_summary = content[:100] + "..." if len(content) > 100 else content
        logging.info(f"Would send to indexer: URL={url}, Title={title}, Content={content_summary}")

        # TODO: Implement actual sending to indexer

    def run(self):
        """Main crawler loop"""
        while True:
            task = self.get_task()

            if not task:
                logging.info("No tasks available, waiting...")
                time.sleep(5)
                continue

            task_id = task["task_id"]
            url = task["url"]

            logging.info(f"Starting task {task_id}: {url}")

            success, extracted_urls, error = self.crawl_url(url)

            if success:
                self.stats["pages_crawled"] += 1
                self.stats["urls_extracted"] += len(extracted_urls)
            else:
                self.stats["pages_failed"] += 1

            self.submit_result(task_id, url, success, extracted_urls, error)

            # Brief pause between tasks
            time.sleep(1)

def main():
    parser = argparse.ArgumentParser(description='Crawler Node for Distributed Web Crawler')
    parser.add_argument('--master', default='http://172.31.24.121:5000', help='URL of the master node')
    parser.add_argument('--user-agent', default='DistributedCrawler/1.0', help='User agent string')
    args = parser.parse_args()

    crawler = CrawlerNode(args.master, args.user_agent)
    crawler.run()

if __name__ == "__main__":
    main()