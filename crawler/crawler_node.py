#!/usr/bin/env python3
import os
import time
import json
import socket
import logging
import argparse
import requests
import threading
import boto3
import hashlib
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
    def __init__(self, master_url, user_agent="DistributedCrawler/1.0", use_sqs=False, s3_bucket=None):
        """Initialize the crawler node"""
        self.hostname = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.hostname)
        self.master_url = master_url
        self.user_agent = user_agent
        self.robots_cache = RobotsCache()
        
        # SQS setup
        self.use_sqs = use_sqs
        if use_sqs:
            self.sqs = boto3.client('sqs')
            self.task_queue_url = None
            self.result_queue_url = None
            
        # S3 setup
        self.s3_bucket = s3_bucket
        if s3_bucket:
            self.s3 = boto3.client('s3')
            logging.info(f"Using S3 bucket {s3_bucket} for content storage")

        # Initialize session for requests
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent
        })

        # Statistics
        self.stats = {
            "pages_crawled": 0,
            "pages_failed": 0,
            "urls_extracted": 0,
            "content_stored": 0
        }
        
        # Heartbeat thread
        self.heartbeat_thread = None
        self.running = True

        logging.info(f"Crawler node initialized at {self.ip_address}")
        logging.info(f"Connected to master at {self.master_url}")

    def setup_sqs(self):
        """Set up SQS queues by asking the master node"""
        try:
            response = requests.post(f"{self.master_url}/assign_task",
                json={"crawler_ip": self.ip_address},
                timeout=5)

            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "use_sqs":
                    self.task_queue_url = data.get("task_queue_url")
                    self.result_queue_url = data.get("result_queue_url")
                    logging.info(f"Using SQS for task management")
                    logging.info(f"Task queue: {self.task_queue_url}")
                    logging.info(f"Result queue: {self.result_queue_url}")
                    return True
            
            logging.warning("Master did not provide SQS information, falling back to direct communication")
            return False
        except Exception as e:
            logging.error(f"Error setting up SQS: {e}")
            return False

    def send_heartbeat(self):
        """Send periodic heartbeats to the master node"""
        while self.running:
            try:
                response = requests.post(f"{self.master_url}/heartbeat",
                    json={"crawler_ip": self.ip_address},
                    timeout=5)
                
                if response.status_code == 200:
                    logging.debug("Heartbeat sent successfully")
                else:
                    logging.warning(f"Failed to send heartbeat: {response.text}")
            except Exception as e:
                logging.error(f"Error sending heartbeat: {e}")
                
            time.sleep(30)  # Send heartbeat every 30 seconds

    def get_task_from_sqs(self):
        """Get a task from the SQS queue"""
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.task_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            
            messages = response.get('Messages', [])
            if not messages:
                return None
                
            message = messages[0]
            receipt_handle = message['ReceiptHandle']
            body = json.loads(message['Body'])
            
            # Delete the message from the queue
            self.sqs.delete_message(
                QueueUrl=self.task_queue_url,
                ReceiptHandle=receipt_handle
            )
            
            return {"url": body.get('url'), "task_id": receipt_handle[:8]}
        except Exception as e:
            logging.error(f"Error getting task from SQS: {e}")
            return None

    def get_task(self):
        """Request a task from the master node"""
        if self.use_sqs and self.task_queue_url:
            return self.get_task_from_sqs()
            
        # Fall back to direct communication
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

    def submit_result_to_sqs(self, task_id, url, success, extracted_urls=None, error=None):
        """Submit crawl results to the SQS result queue"""
        data = {
            "task_id": task_id,
            "url": url,
            "success": success,
            "crawler_ip": self.ip_address
        }

        if extracted_urls:
            data["extracted_urls"] = extracted_urls

        if error:
            data["error"] = str(error)

        try:
            self.sqs.send_message(
                QueueUrl=self.result_queue_url,
                MessageBody=json.dumps(data)
            )
            return True
        except Exception as e:
            logging.error(f"Error submitting result to SQS: {e}")
            return False

    def submit_result(self, task_id, url, success, extracted_urls=None, error=None):
        """Submit crawl results back to the master node"""
        if self.use_sqs and self.result_queue_url:
            return self.submit_result_to_sqs(task_id, url, success, extracted_urls, error)
            
        # Fall back to direct communication
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

    def store_content_in_s3(self, url, html_content, text_content):
        """Store crawled content in S3"""
        if not self.s3_bucket:
            return False
            
        try:
            # Create a unique key for the content based on the URL
            url_hash = hashlib.md5(url.encode()).hexdigest()
            html_key = f"html/{url_hash}.html"
            text_key = f"text/{url_hash}.txt"
            metadata = {
                'url': url,
                'crawled_at': str(time.time())
            }
            
            # Store HTML content
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=html_key,
                Body=html_content,
                ContentType='text/html',
                Metadata=metadata
            )
            
            # Store text content
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=text_key,
                Body=text_content,
                ContentType='text/plain',
                Metadata=metadata
            )
            
            logging.info(f"Stored content for {url} in S3")
            self.stats["content_stored"] += 1
            return True
        except Exception as e:
            logging.error(f"Error storing content in S3: {e}")
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

            # Extract text content for indexing
            title = soup.title.string if soup.title else "No Title"
            text = soup.get_text(separator=' ', strip=True)

            logging.info(f"Extracted {len(extracted_urls)} URLs from {url}")

            # Store content in S3 if configured
            if self.s3_bucket:
                self.store_content_in_s3(url, response.text, text)

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
        try:
            # Get indexer URL from master node configuration
            indexer_url = "http://172.31.27.238:5002"  # This should be configurable
            
            data = {
                "url": url,
                "title": title,
                "content": content
            }
            
            response = requests.post(
                f"{indexer_url}/index",
                json=data,
                timeout=10
            )
            
            if response.status_code == 200:
                logging.info(f"Successfully sent {url} to indexer")
                return True
            else:
                logging.error(f"Failed to send {url} to indexer: {response.text}")
                return False
        except Exception as e:
            logging.error(f"Error sending to indexer: {e}")
            return False

    def run(self):
        """Main crawler loop"""
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        # Set up SQS if needed
        if self.use_sqs:
            self.setup_sqs()
        
        while True:
            task = self.get_task()

            if not task:
                logging.info("No tasks available, waiting...")
                time.sleep(5)
                continue

            task_id = task.get("task_id", "unknown")
            url = task.get("url")

            if not url:
                logging.warning(f"Received task without URL: {task}")
                time.sleep(1)
                continue

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
    parser.add_argument('--use-sqs', action='store_true', help='Use Amazon SQS for task queuing')
    parser.add_argument('--s3-bucket', default='crawler-content-storage', help='S3 bucket for content storage')
    args = parser.parse_args()

    crawler = CrawlerNode(args.master, args.user_agent, args.use_sqs, args.s3_bucket)
    try:
        crawler.run()
    except KeyboardInterrupt:
        logging.info("Crawler shutting down...")
        crawler.running = False
        if crawler.heartbeat_thread:
            crawler.heartbeat_thread.join(timeout=1)

if __name__ == "__main__":
    main()