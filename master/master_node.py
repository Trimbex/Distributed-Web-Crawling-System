#!/usr/bin/env python3
import os
import time
import json
import socket
import logging
import argparse
import requests
import boto3
from threading import Thread
from datetime import datetime
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Master - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("master.log"),
        logging.StreamHandler()
    ]
)

class MasterNode:
    def __init__(self, crawler_nodes, indexer_nodes, seed_urls=None, use_sqs=True):
        """Initialize the master node with crawler and indexer information"""
        self.hostname = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.hostname)

        # Store node information
        self.crawler_nodes = crawler_nodes
        self.indexer_nodes = indexer_nodes
        
        # SQS setup
        self.use_sqs = use_sqs
        if use_sqs:
            self.sqs = boto3.client('sqs')
            self.task_queue_url = self.get_queue_url('crawler-tasks')
            self.result_queue_url = self.get_queue_url('crawler-results')
            logging.info(f"Using SQS queues for task management")

        # Initialize visited URLs set
        self.visited_urls = set()
        self.seen_urls = set()

        # Add seed URLs to queue
        if seed_urls:
            for url in seed_urls:
                if url not in self.seen_urls:
                    self.add_url_to_queue(url)
                    self.seen_urls.add(url)

        # Task management
        self.task_id_counter = 0
        self.active_tasks = {}  # {task_id: (url, crawler_ip, timestamp)}
        
        # Crawler heartbeats
        self.crawler_heartbeats = {}  # {crawler_ip: last_heartbeat_time}

        # Statistics
        self.stats = {
            "urls_queued": 0,
            "urls_crawled": 0,
            "urls_failed": 0,
            "start_time": datetime.now().isoformat(),
            "active_crawlers": 0
        }

        logging.info(f"Master node initialized at {self.ip_address}")
        logging.info(f"Connected to {len(crawler_nodes)} crawler nodes and {len(indexer_nodes)} indexer nodes")

    def get_queue_url(self, queue_name):
        """Get or create an SQS queue URL"""
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
            return response['QueueUrl']
        except self.sqs.exceptions.QueueDoesNotExist:
            response = self.sqs.create_queue(
                QueueName=queue_name,
                Attributes={'VisibilityTimeout': '300'}
            )
            return response['QueueUrl']

    def add_url_to_queue(self, url):
        """Add a URL to the task queue"""
        if self.use_sqs:
            self.sqs.send_message(
                QueueUrl=self.task_queue_url,
                MessageBody=json.dumps({'url': url})
            )
        self.stats["urls_queued"] += 1
        logging.info(f"Added URL to queue: {url}")

    def assign_task(self, crawler_ip):
        """Assign a URL to crawl to a specific crawler node"""
        # Update crawler heartbeat
        self.crawler_heartbeats[crawler_ip] = time.time()
        
        if self.use_sqs:
            # Let SQS handle the task assignment
            return {"status": "use_sqs", "task_queue_url": self.task_queue_url, "result_queue_url": self.result_queue_url}
        else:
            # Legacy direct assignment logic
            # This would be removed in a full SQS implementation
            logging.warning("Using legacy direct task assignment")
            return None

    def process_crawl_result(self, task_id, url, success, extracted_urls=None, error=None):
        """Process the results from a crawler node"""
        if task_id in self.active_tasks:
            del self.active_tasks[task_id]

            if success:
                self.visited_urls.add(url)
                self.stats["urls_crawled"] += 1
                logging.info(f"Task {task_id} completed successfully for {url}")

                # Add new URLs to the queue
                if extracted_urls:
                    new_urls_count = 0
                    for new_url in extracted_urls:
                        if new_url not in self.seen_urls:
                            self.add_url_to_queue(new_url)
                            self.seen_urls.add(new_url)
                            new_urls_count += 1

                    logging.info(f"Added {new_urls_count} new URLs to the queue from task {task_id}")
            else:
                self.stats["urls_failed"] += 1
                logging.error(f"Task {task_id} failed for {url}: {error}")
        else:
            logging.warning(f"Received result for unknown task ID: {task_id}")

    def process_sqs_results(self):
        """Process crawler results from SQS queue"""
        while True:
            try:
                response = self.sqs.receive_message(
                    QueueUrl=self.result_queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20
                )
                
                messages = response.get('Messages', [])
                if not messages:
                    time.sleep(5)  # No messages, wait a bit
                    continue
                    
                for message in messages:
                    receipt_handle = message['ReceiptHandle']
                    body = json.loads(message['Body'])
                    
                    url = body.get('url')
                    success = body.get('success', False)
                    extracted_urls = body.get('extracted_urls', [])
                    error = body.get('error')
                    crawler_ip = body.get('crawler_ip')
                    
                    # Update crawler heartbeat
                    if crawler_ip:
                        self.crawler_heartbeats[crawler_ip] = time.time()
                    
                    if success:
                        self.visited_urls.add(url)
                        self.stats["urls_crawled"] += 1
                        
                        # Add new URLs to the queue
                        new_urls_count = 0
                        for new_url in extracted_urls:
                            if new_url not in self.seen_urls:
                                self.add_url_to_queue(new_url)
                                self.seen_urls.add(new_url)
                                new_urls_count += 1
                                
                        logging.info(f"Processed result for {url}: added {new_urls_count} new URLs")
                    else:
                        self.stats["urls_failed"] += 1
                        logging.error(f"Crawl failed for {url}: {error}")
                    
                    # Delete the message
                    self.sqs.delete_message(
                        QueueUrl=self.result_queue_url,
                        ReceiptHandle=receipt_handle
                    )
            except Exception as e:
                logging.error(f"Error processing SQS results: {e}")
                time.sleep(10)  # Wait before retrying

    def check_crawler_health(self, timeout=60):
        """Check for stalled or failed crawler nodes"""
        current_time = time.time()
        active_crawlers = 0
        
        for crawler_ip, last_heartbeat in list(self.crawler_heartbeats.items()):
            if current_time - last_heartbeat > timeout:
                logging.warning(f"Crawler {crawler_ip} appears to be down (no heartbeat)")
            else:
                active_crawlers += 1
                
        self.stats["active_crawlers"] = active_crawlers
        return active_crawlers

    def get_status(self):
        """Get the current status of the crawler system"""
        self.stats["queue_size"] = "N/A (Using SQS)" if self.use_sqs else "N/A"
        self.stats["visited_count"] = len(self.visited_urls)
        self.stats["active_tasks"] = len(self.active_tasks)
        self.stats["running_time"] = str(datetime.now() - datetime.fromisoformat(self.stats["start_time"]))
        self.stats["active_crawlers"] = self.check_crawler_health()
        return self.stats

    def add_urls(self, urls):
        """Add new URLs to the crawl queue"""
        added = 0
        for url in urls:
            if url not in self.seen_urls:
                self.add_url_to_queue(url)
                self.seen_urls.add(url)
                added += 1
        return added

def start_api_server(master, port=5000):
    """Start a simple HTTP server to expose master node API"""
    from flask import Flask, request, jsonify

    app = Flask(__name__)

    @app.route('/assign_task', methods=['POST'])
    def assign_task():
        data = request.json
        crawler_ip = data.get('crawler_ip')
        task = master.assign_task(crawler_ip)
        return jsonify(task if task else {"status": "no_task"})

    @app.route('/submit_result', methods=['POST'])
    def submit_result():
        data = request.json
        task_id = data.get('task_id')
        url = data.get('url')
        success = data.get('success', False)
        extracted_urls = data.get('extracted_urls', [])
        error = data.get('error')

        master.process_crawl_result(task_id, url, success, extracted_urls, error)
        return jsonify({"status": "result_processed"})

    @app.route('/heartbeat', methods=['POST'])
    def heartbeat():
        data = request.json
        crawler_ip = data.get('crawler_ip')
        if crawler_ip:
            master.crawler_heartbeats[crawler_ip] = time.time()
            return jsonify({"status": "heartbeat_received"})
        return jsonify({"error": "No crawler_ip provided"}), 400

    @app.route('/status', methods=['GET'])
    def status():
        return jsonify(master.get_status())

    @app.route('/add_urls', methods=['POST'])
    def add_urls():
        data = request.json
        urls = data.get('urls', [])
        added = master.add_urls(urls)
        return jsonify({"added": added})

    app.run(host='0.0.0.0', port=port)

def monitor_tasks(master, check_interval=30):
    """Background thread to monitor tasks and handle stalled ones"""
    while True:
        master.check_crawler_health()
        time.sleep(check_interval)

def main():
    parser = argparse.ArgumentParser(description='Master Node for Distributed Web Crawler')
    parser.add_argument('--port', type=int, default=5000, help='Port for the API server')
    parser.add_argument('--seed-urls', nargs='+', default=['https://example.com'], help='Seed URLs to start crawling')
    parser.add_argument('--use-sqs', action='store_true', help='Use Amazon SQS for task queuing')
    args = parser.parse_args()

    # In a real system, these would come from configuration or service discovery
    # For now, we'll hardcode crawler and indexer information
    crawler_nodes = [
        {"ip": "172.31.29.194", "port": 5001}
    ]

    indexer_nodes = [
        {"ip": "172.31.27.238", "port": 5002}
    ]

    # Initialize master node
    master = MasterNode(crawler_nodes, indexer_nodes, args.seed_urls, args.use_sqs)

    # Start task monitoring thread
    monitor_thread = Thread(target=monitor_tasks, args=(master,))
    monitor_thread.daemon = True
    monitor_thread.start()
    
    # Start SQS result processing thread if using SQS
    if args.use_sqs:
        sqs_thread = Thread(target=master.process_sqs_results)
        sqs_thread.daemon = True
        sqs_thread.start()

    # Start API server
    start_api_server(master, args.port)

if __name__ == "__main__":
    main()