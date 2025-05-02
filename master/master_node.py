#!/usr/bin/env python3
import os
import time
import json
import socket
import logging
import argparse
import requests
from queue import Queue
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
    def __init__(self, crawler_nodes, indexer_nodes, seed_urls=None):
        """Initialize the master node with crawler and indexer information"""
        self.hostname = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.hostname)

        # Store node information
        self.crawler_nodes = crawler_nodes
        self.indexer_nodes = indexer_nodes

        # Initialize queues
        self.url_queue = Queue()
        self.visited_urls = set()
        self.seen_urls = set()

        # Add seed URLs to queue
        if seed_urls:
            for url in seed_urls:
                if url not in self.seen_urls:
                    self.url_queue.put(url)
                    self.seen_urls.add(url)

        # Task management
        self.task_id_counter = 0
        self.active_tasks = {}  # {task_id: (url, crawler_ip, timestamp)}

        # Statistics
        self.stats = {
            "urls_queued": 0,
            "urls_crawled": 0,
            "urls_failed": 0,
            "start_time": datetime.now().isoformat()
        }

        logging.info(f"Master node initialized at {self.ip_address}")
        logging.info(f"Connected to {len(crawler_nodes)} crawler nodes and {len(indexer_nodes)} indexer nodes")

    def assign_task(self, crawler_ip):
        """Assign a URL to crawl to a specific crawler node"""
        if self.url_queue.empty():
            logging.info(f"No tasks available for crawler {crawler_ip}")
            return None

        url = self.url_queue.get()
        task_id = self.task_id_counter
        self.task_id_counter += 1

        # Record task assignment
        self.active_tasks[task_id] = (url, crawler_ip, time.time())
        self.stats["urls_queued"] += 1

        logging.info(f"Assigned task {task_id}: {url} to crawler {crawler_ip}")
        return {"task_id": task_id, "url": url}

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
                            self.url_queue.put(new_url)
                            self.seen_urls.add(new_url)
                            new_urls_count += 1

                    logging.info(f"Added {new_urls_count} new URLs to the queue from task {task_id}")
            else:
                self.stats["urls_failed"] += 1
                logging.error(f"Task {task_id} failed for {url}: {error}")
        else:
            logging.warning(f"Received result for unknown task ID: {task_id}")

    def check_for_stalled_tasks(self, timeout=60):
        """Check for stalled tasks and reassign if necessary"""
        current_time = time.time()
        stalled_tasks = []

        for task_id, (url, crawler_ip, timestamp) in list(self.active_tasks.items()):
            if current_time - timestamp > timeout:
                logging.warning(f"Task {task_id} for {url} on crawler {crawler_ip} appears stalled")
                stalled_tasks.append(task_id)
                del self.active_tasks[task_id]

                # Re-queue the URL
                self.url_queue.put(url)
                logging.info(f"Re-queued {url} from stalled task {task_id}")

        return stalled_tasks

    def get_status(self):
        """Get the current status of the crawler system"""
        self.stats["queue_size"] = self.url_queue.qsize()
        self.stats["visited_count"] = len(self.visited_urls)
        self.stats["active_tasks"] = len(self.active_tasks)
        self.stats["running_time"] = str(datetime.now() - datetime.fromisoformat(self.stats["start_time"]))
        return self.stats

    def add_urls(self, urls):
        """Add new URLs to the crawl queue"""
        added = 0
        for url in urls:
            if url not in self.seen_urls:
                self.url_queue.put(url)
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
        stalled_tasks = master.check_for_stalled_tasks()
        if stalled_tasks:
            logging.info(f"Detected {len(stalled_tasks)} stalled tasks and reassigned them")
        time.sleep(check_interval)

def main():
    parser = argparse.ArgumentParser(description='Master Node for Distributed Web Crawler')
    parser.add_argument('--port', type=int, default=5000, help='Port for the API server')
    parser.add_argument('--seed-urls', nargs='+', default=['https://example.com'], help='Seed URLs to start crawling')
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
    master = MasterNode(crawler_nodes, indexer_nodes, args.seed_urls)

    # Start task monitoring thread
    monitor_thread = Thread(target=monitor_tasks, args=(master,))
    monitor_thread.daemon = True
    monitor_thread.start()

    # Start API server
    start_api_server(master, args.port)

if __name__ == "__main__":
    main()