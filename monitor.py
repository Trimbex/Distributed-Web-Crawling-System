#!/usr/bin/env python3
import os
import time
import argparse
import requests
import json
from tabulate import tabulate

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

def display_status(master_status):
    """Display master status in a formatted table"""
    if "error" in master_status:
        print(f"Error getting master status: {master_status['error']}")
        return

    # Format the data for display
    data = []
    for key, value in master_status.items():
        data.append([key, value])

    print("\nMaster Node Status:")
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

def monitor_mode(master_url, interval=10):
    """Continuously monitor the crawler status"""
    try:
        while True:
            status = get_master_status(master_url)
            display_status(status)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

def main():
    parser = argparse.ArgumentParser(description='Monitor and Control Distributed Web Crawler')
    parser.add_argument('--master', default='http://172.31.24.121:5000', help='URL of the master node')
    parser.add_argument('--indexer', default='http://172.31.27.238:5002', help='URL of the indexer node')

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Monitor command
    monitor_parser = subparsers.add_parser('monitor', help='Monitor crawler status')
    monitor_parser.add_argument('--interval', type=int, default=10, help='Update interval in seconds')

    # Add URLs command
    add_parser = subparsers.add_parser('add', help='Add URLs to crawl')
    add_parser.add_argument('urls', nargs='+', help='URLs to add to the crawl queue')

    # Search command
    search_parser = subparsers.add_parser('search', help='Search the index')
    search_parser.add_argument('query', help='Search query')

    # Status command
    subparsers.add_parser('status', help='Show current status')

    args = parser.parse_args()

    # Handle commands
    if args.command == 'monitor':
        monitor_mode(args.master, args.interval)
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
        status = get_master_status(args.master)
        display_status(status)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()