#!/usr/bin/env python3
import os
import time
import argparse
import requests
import json
import boto3
import webbrowser
from tabulate import tabulate
from datetime import datetime

def get_master_status(master_url):
    """Get status information from the master node"""
    try:
        response = requests.get(f"{master_url}/status", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"HTTP {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": str(e)}

def get_indexer_status(indexer_url):
    """Get status information from the indexer node"""
    try:
        response = requests.get(f"{indexer_url}/status", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"HTTP {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": str(e)}

def add_urls_to_master(master_url, urls):
    """Add new URLs to the master's crawl queue"""
    try:
        response = requests.post(
            f"{master_url}/add_urls",
            json={"urls": urls},
            timeout=5
        )
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"HTTP {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": str(e)}

def search_index(indexer_url, query):
    """Search the index for documents matching the query"""
    try:
        response = requests.get(
            f"{indexer_url}/search",
            params={"q": query, "max": 10},
            timeout=5
        )
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"HTTP {response.status_code}: {response.text}"}
    except Exception as e:
        return {"error": str(e)}

def display_status(master_status, indexer_status=None):
    """Display master and indexer status in a formatted table"""
    if "error" in master_status:
        print(f"Error getting master status: {master_status['error']}")
    else:
        # Format the data for display
        data = []
        for key, value in master_status.items():
            data.append(["Master: " + key, value])

        print("\nMaster Node Status:")
        print(tabulate(data, headers=["Metric", "Value"], tablefmt="grid"))
    
    if indexer_status:
        if "error" in indexer_status:
            print(f"Error getting indexer status: {indexer_status['error']}")
        else:
            # Format the data for display
            data = []
            for key, value in indexer_status.items():
                data.append(["Indexer: " + key, value])

            print("\nIndexer Node Status:")
            print(tabulate(data, headers=["Metric", "Value"], tablefmt="grid"))

def display_search_results(results):
    """Display search results in a formatted table"""
    if "error" in results:
        print(f"Error searching: {results['error']}")
        return

    if not results.get("results"):
        print("No matching documents found.")
        return

    # Format the results for display
    data = []
    for result in results["results"]:
        data.append([
            result["title"],
            result["url"],
            result["snippet"],
            result["score"]
        ])

    print("\nSearch Results:")
    print(tabulate(data, headers=["Title", "URL", "Snippet", "Score"], tablefmt="grid"))

def monitor_mode(master_url, indexer_url, interval=10, log_file=None):
    """Continuously monitor the crawler status"""
    try:
        if log_file:
            with open(log_file, 'a') as f:
                f.write(f"=== Monitoring started at {datetime.now().isoformat()} ===\n")
        
        while True:
            master_status = get_master_status(master_url)
            indexer_status = get_indexer_status(indexer_url)
            
            display_status(master_status, indexer_status)
            
            if log_file:
                with open(log_file, 'a') as f:
                    timestamp = datetime.now().isoformat()
                    f.write(f"{timestamp} - Master: {json.dumps(master_status)}\n")
                    f.write(f"{timestamp} - Indexer: {json.dumps(indexer_status)}\n")
            
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

def check_sqs_queues(region_name='us-east-1'):
    """Check SQS queues status"""
    try:
        sqs = boto3.client('sqs', region_name=region_name)
        queues = sqs.list_queues(QueueNamePrefix='crawler')
        
        if not queues.get('QueueUrls'):
            print("No crawler queues found in SQS.")
            return
            
        print("\nSQS Queue Status:")
        for queue_url in queues['QueueUrls']:
            queue_name = queue_url.split('/')[-1]
            
            # Get queue attributes
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            
            messages = int(attrs['Attributes']['ApproximateNumberOfMessages'])
            in_flight = int(attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
            
            print(f"Queue: {queue_name}")
            print(f"  Messages available: {messages}")
            print(f"  Messages in flight: {in_flight}")
            print()
    except Exception as e:
        print(f"Error checking SQS queues: {e}")

def check_s3_storage(bucket_name, region_name='us-east-1'):
    """Check S3 storage status"""
    try:
        s3 = boto3.client('s3', region_name=region_name)
        
        # Check if bucket exists
        try:
            s3.head_bucket(Bucket=bucket_name)
        except:
            print(f"S3 bucket '{bucket_name}' not found or not accessible.")
            return
            
        # Get object count and size
        paginator = s3.get_paginator('list_objects_v2')
        total_objects = 0
        total_size = 0
        
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                total_objects += len(page['Contents'])
                total_size += sum(obj['Size'] for obj in page['Contents'])
        
        print("\nS3 Storage Status:")
        print(f"Bucket: {bucket_name}")
        print(f"Total objects: {total_objects}")
        print(f"Total size: {total_size / (1024 * 1024):.2f} MB")
        
        # Count by prefix
        prefixes = ['html/', 'text/']
        for prefix in prefixes:
            count = 0
            size = 0
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    count += len(page['Contents'])
                    size += sum(obj['Size'] for obj in page['Contents'])
            
            print(f"{prefix} objects: {count} ({size / (1024 * 1024):.2f} MB)")
    except Exception as e:
        print(f"Error checking S3 storage: {e}")

def open_search_interface(indexer_url):
    """Open the search interface in a web browser"""
    try:
        webbrowser.open(indexer_url)
        print(f"Opening search interface at {indexer_url}")
    except Exception as e:
        print(f"Error opening search interface: {e}")

def main():
    parser = argparse.ArgumentParser(description='Monitor and Control Distributed Web Crawler')
    parser.add_argument('--master', default='http://172.31.24.121:5000', help='URL of the master node')
    parser.add_argument('--indexer', default='http://172.31.27.238:5002', help='URL of the indexer node')
    parser.add_argument('--region', default='us-east-1', help='AWS region name')
    parser.add_argument('--s3-bucket', default='crawler-content-storage', help='S3 bucket name')

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Monitor command
    monitor_parser = subparsers.add_parser('monitor', help='Monitor crawler status')
    monitor_parser.add_argument('--interval', type=int, default=10, help='Update interval in seconds')
    monitor_parser.add_argument('--log', help='Log file to record monitoring data')

    # Add URLs command
    add_parser = subparsers.add_parser('add', help='Add URLs to crawl')
    add_parser.add_argument('urls', nargs='+', help='URLs to add to the crawl queue')

    # Search command
    search_parser = subparsers.add_parser('search', help='Search the index')
    search_parser.add_argument('query', help='Search query')

    # Status command
    subparsers.add_parser('status', help='Show current status')

    # AWS commands
    subparsers.add_parser('sqs', help='Check SQS queues status')
    subparsers.add_parser('s3', help='Check S3 storage status')
    
    # Web interface command
    subparsers.add_parser('web', help='Open search web interface')

    args = parser.parse_args()

    # Handle commands
    if args.command == 'monitor':
        monitor_mode(args.master, args.indexer, args.interval, args.log)
    elif args.command == 'add':
        result = add_urls_to_master(args.master, args.urls)
        if "error" in result:
            print(f"Error adding URLs: {result['error']}")
        else:
            print(f"Added {result.get('added', 0)} URLs to the crawl queue")
    elif args.command == 'search':
        results = search_index(args.indexer, args.query)
        display_search_results(results)
    elif args.command == 'status' or not args.command:
        master_status = get_master_status(args.master)
        indexer_status = get_indexer_status(args.indexer)
        display_status(master_status, indexer_status)
    elif args.command == 'sqs':
        check_sqs_queues(args.region)
    elif args.command == 's3':
        check_s3_storage(args.s3_bucket, args.region)
    elif args.command == 'web':
        open_search_interface(args.indexer)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()