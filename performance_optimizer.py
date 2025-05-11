#!/usr/bin/env python3
import os
import re
import json
import time
import boto3
import logging
import argparse
import requests
import statistics
from tabulate import tabulate
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - PerformanceOptimizer - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("performance_optimizer.log"),
        logging.StreamHandler()
    ]
)

class PerformanceOptimizer:
    def __init__(self, master_url, indexer_url, s3_bucket=None, region_name='us-east-1'):
        """Initialize the performance optimizer"""
        self.master_url = master_url
        self.indexer_url = indexer_url
        self.s3_bucket = s3_bucket
        self.region_name = region_name
        
        # AWS clients
        if s3_bucket:
            self.s3 = boto3.client('s3', region_name=region_name)
        
        # Performance metrics
        self.metrics = {
            "crawl_rate": [],  # URLs per minute
            "index_rate": [],  # Documents per minute
            "response_times": [],  # API response times (ms)
            "memory_usage": [],  # Memory usage (MB)
            "network_latency": []  # Network latency (ms)
        }
        
        # Optimization recommendations
        self.recommendations = []
        
        logging.info(f"Performance optimizer initialized with master at {master_url} and indexer at {indexer_url}")

    def measure_api_response_times(self, num_requests=10):
        """Measure API response times"""
        logging.info(f"Measuring API response times ({num_requests} requests)...")
        
        endpoints = [
            (self.master_url + "/status", "GET", None),
            (self.indexer_url + "/status", "GET", None),
            (self.master_url + "/add_urls", "POST", {"urls": ["https://example.com"]}),
            (self.indexer_url + "/search", "GET", {"q": "web crawler", "max": 5})
        ]
        
        response_times = {}
        
        for endpoint, method, data in endpoints:
            times = []
            for _ in range(num_requests):
                try:
                    start_time = time.time()
                    
                    if method == "GET":
                        if data:
                            response = requests.get(endpoint, params=data, timeout=5)
                        else:
                            response = requests.get(endpoint, timeout=5)
                    elif method == "POST":
                        response = requests.post(endpoint, json=data, timeout=5)
                    
                    elapsed_ms = (time.time() - start_time) * 1000
                    
                    if response.status_code == 200:
                        times.append(elapsed_ms)
                    else:
                        logging.warning(f"Request to {endpoint} failed: {response.status_code}")
                    
                    # Small delay to avoid overwhelming the server
                    time.sleep(0.2)
                except Exception as e:
                    logging.error(f"Error measuring response time for {endpoint}: {e}")
            
            if times:
                avg_time = statistics.mean(times)
                response_times[endpoint] = {
                    "avg_ms": avg_time,
                    "min_ms": min(times),
                    "max_ms": max(times)
                }
                self.metrics["response_times"].append(avg_time)
                
                # Assess if response time is problematic
                if avg_time > 500:  # More than 500ms is slow
                    self.recommendations.append({
                        "type": "API Performance",
                        "severity": "HIGH" if avg_time > 1000 else "MEDIUM",
                        "component": endpoint,
                        "description": f"Slow API response time ({avg_time:.2f}ms) for {endpoint}",
                        "suggestion": "Consider optimizing database queries, implementing caching, or reducing computational complexity"
                    })
        
        # Display results
        print("\nAPI Response Times:")
        data = []
        for endpoint, metrics in response_times.items():
            data.append([
                endpoint.split('/')[-1],  # Just show endpoint name
                f"{metrics['avg_ms']:.2f}ms",
                f"{metrics['min_ms']:.2f}ms",
                f"{metrics['max_ms']:.2f}ms"
            ])
        
        print(tabulate(data, headers=["Endpoint", "Average", "Min", "Max"], tablefmt="grid"))
        return response_times

    def analyze_crawl_rate(self, duration_minutes=5):
        """Analyze the crawl rate"""
        logging.info(f"Analyzing crawl rate over {duration_minutes} minutes...")
        
        try:
            # Get initial count
            response = requests.get(f"{self.master_url}/status", timeout=5)
            if response.status_code != 200:
                logging.error(f"Failed to get master status: {response.status_code}")
                return None
                
            initial_state = response.json()
            initial_crawled = initial_state.get("urls_crawled", 0)
            start_time = time.time()
            
            # Add some test URLs to crawl
            test_urls = [
                f"https://en.wikipedia.org/wiki/Special:Random?{i}" 
                for i in range(20)  # 20 random Wikipedia pages
            ]
            
            requests.post(
                f"{self.master_url}/add_urls",
                json={"urls": test_urls},
                timeout=5
            )
            
            print(f"Added {len(test_urls)} test URLs to crawl")
            print(f"Monitoring crawl rate for {duration_minutes} minutes...")
            
            # Wait for the specified duration
            time.sleep(duration_minutes * 60)
            
            # Get final count
            response = requests.get(f"{self.master_url}/status", timeout=5)
            if response.status_code != 200:
                logging.error(f"Failed to get master status: {response.status_code}")
                return None
                
            final_state = response.json()
            final_crawled = final_state.get("urls_crawled", 0)
            end_time = time.time()
            
            # Calculate crawl rate
            urls_crawled = final_crawled - initial_crawled
            elapsed_time = (end_time - start_time) / 60  # in minutes
            crawl_rate = urls_crawled / elapsed_time if elapsed_time > 0 else 0
            
            self.metrics["crawl_rate"] = crawl_rate
            
            # Assess if crawl rate is acceptable
            if crawl_rate < 1:  # Less than 1 URL per minute is very slow
                self.recommendations.append({
                    "type": "Crawler Performance",
                    "severity": "HIGH",
                    "component": "Crawler Nodes",
                    "description": f"Very low crawl rate: {crawl_rate:.2f} URLs per minute",
                    "suggestion": "Increase the number of crawler nodes, optimize request handling, or check for network limitations"
                })
            elif crawl_rate < 5:  # Less than 5 URLs per minute is somewhat slow
                self.recommendations.append({
                    "type": "Crawler Performance",
                    "severity": "MEDIUM",
                    "component": "Crawler Nodes",
                    "description": f"Low crawl rate: {crawl_rate:.2f} URLs per minute",
                    "suggestion": "Consider adding more crawler nodes or optimizing crawler efficiency"
                })
            
            print(f"\nCrawl Rate Analysis:")
            print(f"URLs crawled: {urls_crawled}")
            print(f"Time elapsed: {elapsed_time:.2f} minutes")
            print(f"Crawl rate: {crawl_rate:.2f} URLs per minute")
            
            return crawl_rate
        
        except Exception as e:
            logging.error(f"Error analyzing crawl rate: {e}")
            return None

    def analyze_index_performance(self):
        """Analyze indexer performance"""
        logging.info("Analyzing indexer performance...")
        
        try:
            # Get indexer stats
            response = requests.get(f"{self.indexer_url}/status", timeout=5)
            if response.status_code != 200:
                logging.error(f"Failed to get indexer status: {response.status_code}")
                return None
                
            stats = response.json()
            docs_indexed = stats.get("pages_indexed", 0)
            index_size_mb = stats.get("index_size_mb", 0)
            
            # Test search performance
            search_terms = ["web", "crawler", "distributed", "computing", "python"]
            search_times = []
            
            for term in search_terms:
                try:
                    start_time = time.time()
                    response = requests.get(
                        f"{self.indexer_url}/search",
                        params={"q": term, "max": 10},
                        timeout=5
                    )
                    elapsed_ms = (time.time() - start_time) * 1000
                    
                    if response.status_code == 200:
                        results = response.json().get("results", [])
                        search_times.append({
                            "term": term,
                            "results": len(results),
                            "time_ms": elapsed_ms
                        })
                except Exception as e:
                    logging.error(f"Error testing search for '{term}': {e}")
            
            # Calculate average search time
            if search_times:
                avg_search_time = statistics.mean([s["time_ms"] for s in search_times])
                
                # Assess search performance
                if avg_search_time > 500:  # More than 500ms is slow
                    self.recommendations.append({
                        "type": "Indexer Performance",
                        "severity": "HIGH" if avg_search_time > 1000 else "MEDIUM",
                        "component": "Search Engine",
                        "description": f"Slow search response time: {avg_search_time:.2f}ms average",
                        "suggestion": "Optimize index configuration, add appropriate field caching, or consider index sharding"
                    })
                
                # Assess index size
                if docs_indexed > 0 and index_size_mb / docs_indexed > 0.5:  # More than 0.5MB per document is large
                    self.recommendations.append({
                        "type": "Indexer Performance",
                        "severity": "MEDIUM",
                        "component": "Index Storage",
                        "description": f"Large index size: {index_size_mb:.2f}MB for {docs_indexed} documents ({index_size_mb/docs_indexed:.2f}MB per document)",
                        "suggestion": "Optimize stored fields, consider compression, or review what content is being indexed"
                    })
                
                # Display results
                print("\nIndexer Performance:")
                print(f"Documents indexed: {docs_indexed}")
                print(f"Index size: {index_size_mb:.2f}MB")
                print(f"Avg size per document: {index_size_mb/docs_indexed:.4f}MB" if docs_indexed > 0 else "N/A")
                
                print("\nSearch Performance:")
                data = []
                for search in search_times:
                    data.append([
                        search["term"],
                        search["results"],
                        f"{search['time_ms']:.2f}ms"
                    ])
                print(tabulate(data, headers=["Query", "Results", "Time"], tablefmt="grid"))
                
                return {
                    "docs_indexed": docs_indexed,
                    "index_size_mb": index_size_mb,
                    "avg_search_time_ms": avg_search_time,
                    "search_times": search_times
                }
        
        except Exception as e:
            logging.error(f"Error analyzing indexer performance: {e}")
            return None

    def analyze_s3_performance(self):
        """Analyze S3 storage performance"""
        if not self.s3_bucket:
            logging.warning("No S3 bucket specified, skipping S3 performance analysis")
            return None
            
        logging.info(f"Analyzing S3 performance for bucket {self.s3_bucket}...")
        
        try:
            # Check object count and size
            paginator = self.s3.get_paginator('list_objects_v2')
            
            # Check HTML files
            html_objects = []
            html_total_size = 0
            
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix='html/'):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        html_objects.append(obj)
                        html_total_size += obj['Size']
            
            # Check item/text files
            item_objects = []
            item_total_size = 0
            
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix='text/'):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        item_objects.append(obj)
                        item_total_size += obj['Size']
            
            # Measure download time for a sample of objects
            download_times = []
            sample_size = min(10, len(html_objects))
            
            if sample_size > 0:
                sample_objects = html_objects[:sample_size]
                
                for obj in sample_objects:
                    try:
                        start_time = time.time()
                        self.s3.get_object(Bucket=self.s3_bucket, Key=obj['Key'])
                        elapsed_ms = (time.time() - start_time) * 1000
                        download_times.append(elapsed_ms)
                    except Exception as e:
                        logging.error(f"Error downloading {obj['Key']}: {e}")
            
            # Calculate statistics
            stats = {
                "html_count": len(html_objects),
                "html_size_mb": html_total_size / (1024 * 1024),
                "item_count": len(item_objects),
                "item_size_mb": item_total_size / (1024 * 1024),
                "total_objects": len(html_objects) + len(item_objects),
                "total_size_mb": (html_total_size + item_total_size) / (1024 * 1024)
            }
            
            if download_times:
                stats["avg_download_ms"] = statistics.mean(download_times)
                
                # Assess download performance
                if stats["avg_download_ms"] > 500:  # More than 500ms is slow
                    self.recommendations.append({
                        "type": "S3 Performance",
                        "severity": "MEDIUM",
                        "component": "Storage",
                        "description": f"Slow S3 download time: {stats['avg_download_ms']:.2f}ms average",
                        "suggestion": "Consider using CloudFront for caching or check network connectivity to S3"
                    })
            
            # Display results
            print("\nS3 Storage Analysis:")
            print(f"Total objects: {stats['total_objects']}")
            print(f"Total size: {stats['total_size_mb']:.2f}MB")
            print(f"HTML files: {stats['html_count']} ({stats['html_size_mb']:.2f}MB)")
            print(f"Text/item files: {stats['item_count']} ({stats['item_size_mb']:.2f}MB)")
            
            if download_times:
                print(f"Average download time: {stats['avg_download_ms']:.2f}ms")
            
            # Check for storage efficiency
            if stats['total_objects'] > 0:
                avg_size = stats['total_size_mb'] / stats['total_objects']
                
                if avg_size > 1.0:  # More than 1MB per object is large
                    self.recommendations.append({
                        "type": "Storage Efficiency",
                        "severity": "MEDIUM",
                        "component": "S3 Storage",
                        "description": f"Large average file size: {avg_size:.2f}MB per object",
                        "suggestion": "Consider text extraction optimizations or content filtering to reduce storage needs"
                    })
                elif html_objects and stats['html_count'] > 0 and stats['html_size_mb'] / stats['html_count'] > 0.5:
                    self.recommendations.append({
                        "type": "Storage Efficiency",
                        "severity": "LOW",
                        "component": "HTML Storage",
                        "description": f"Large HTML files: {stats['html_size_mb'] / stats['html_count']:.2f}MB per file",
                        "suggestion": "Consider selective HTML storage or implementing size limits"
                    })
            
            return stats
        
        except Exception as e:
            logging.error(f"Error analyzing S3 performance: {e}")
            return None

    def analyze_sqs_performance(self):
        """Analyze SQS queue performance"""
        logging.info("Analyzing SQS queue performance...")
        
        try:
            sqs = boto3.client('sqs', region_name=self.region_name)
            queue_stats = {}
            
            # Get queue URLs
            queues = sqs.list_queues(QueueNamePrefix='crawler')
            
            if not queues.get('QueueUrls'):
                logging.warning("No crawler queues found")
                return None
            
            for queue_url in queues['QueueUrls']:
                queue_name = queue_url.split('/')[-1]
                
                # Get queue attributes
                attrs = sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=[
                        'ApproximateNumberOfMessages', 
                        'ApproximateNumberOfMessagesNotVisible', 
                        'ApproximateNumberOfMessagesDelayed'
                    ]
                )
                
                messages = int(attrs['Attributes']['ApproximateNumberOfMessages'])
                in_flight = int(attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
                delayed = int(attrs['Attributes']['ApproximateNumberOfMessagesDelayed'])
                
                queue_stats[queue_name] = {
                    "messages": messages,
                    "in_flight": in_flight,
                    "delayed": delayed,
                    "total": messages + in_flight + delayed
                }
                
                # Assess queue health
                if in_flight > 10 * messages and in_flight > 100:
                    self.recommendations.append({
                        "type": "Queue Performance",
                        "severity": "HIGH",
                        "component": queue_name,
                        "description": f"High in-flight to available message ratio: {in_flight} in-flight vs {messages} available",
                        "suggestion": "Check for stalled message processing or increase crawler node count to handle load"
                    })
                elif messages > 1000 and in_flight < messages * 0.1:
                    self.recommendations.append({
                        "type": "Queue Performance",
                        "severity": "MEDIUM",
                        "component": queue_name,
                        "description": f"Queue backlog: {messages} messages with low processing rate",
                        "suggestion": "Add more crawler nodes to process the backlog or check for processing bottlenecks"
                    })
            
            # Display results
            print("\nSQS Queue Analysis:")
            data = []
            for queue, stats in queue_stats.items():
                data.append([
                    queue,
                    stats["messages"],
                    stats["in_flight"],
                    stats["delayed"],
                    stats["total"]
                ])
            
            print(tabulate(data, headers=["Queue", "Available", "In Flight", "Delayed", "Total"], tablefmt="grid"))
            
            return queue_stats
        except Exception as e:
            logging.error(f"Error analyzing SQS performance: {e}")
            return None

    def analyze_code_for_bottlenecks(self, files_to_analyze=None):
        """Analyze code for potential performance bottlenecks"""
        logging.info("Analyzing code for potential bottlenecks...")
        
        if not files_to_analyze:
            # Default files to analyze
            files_to_analyze = [
                'master/master_node.py',
                'crawler/crawler_node.py',
                'indexer/indexer_node.py'
            ]
        
        patterns = [
            (r'for\s+\w+\s+in\s+.+\s*:.*for\s+\w+\s+in', 'Nested loops'),
            (r'\.readlines\(\)', 'Reading entire file into memory'),
            (r'time\.sleep\((\d+)\)', 'Long sleep periods'),
            (r'requests\.\w+\([^)]*timeout\s*=\s*(\d+)', 'Long request timeouts'),
            (r'BeautifulSoup\(.*?html\.parser', 'Using slower html.parser instead of lxml'),
            (r'\.get_object\(', 'Downloading entire S3 object'),
            (r'\.load\(', 'Loading large JSON data')
        ]
        
        bottlenecks = []
        
        for file_path in files_to_analyze:
            try:
                if not os.path.exists(file_path):
                    logging.warning(f"File {file_path} not found")
                    continue
                    
                with open(file_path, 'r') as f:
                    content = f.read()
                
                for pattern, description in patterns:
                    matches = re.finditer(pattern, content)
                    for match in matches:
                        line_no = content[:match.start()].count('\n') + 1
                        bottlenecks.append({
                            "file": file_path,
                            "line": line_no,
                            "description": description,
                            "code": match.group(0)
                        })
                        
                        # Add recommendation based on the bottleneck
                        if 'Nested loops' in description:
                            self.recommendations.append({
                                "type": "Code Optimization",
                                "severity": "MEDIUM",
                                "component": f"{file_path}:{line_no}",
                                "description": f"Potential performance bottleneck: {description}",
                                "suggestion": "Consider restructuring to avoid nested loops or use more efficient data structures"
                            })
                        elif 'Long sleep' in description:
                            sleep_time = match.group(1)
                            if int(sleep_time) > 5:
                                self.recommendations.append({
                                    "type": "Code Optimization",
                                    "severity": "LOW",
                                    "component": f"{file_path}:{line_no}",
                                    "description": f"Long sleep period: {sleep_time} seconds",
                                    "suggestion": "Consider reducing sleep time for more responsive processing"
                                })
                        elif 'Long request timeouts' in description:
                            timeout = match.group(1)
                            if int(timeout) > 10:
                                self.recommendations.append({
                                    "type": "Code Optimization",
                                    "severity": "MEDIUM",
                                    "component": f"{file_path}:{line_no}",
                                    "description": f"Long request timeout: {timeout} seconds",
                                    "suggestion": "Consider reducing timeout to fail faster and avoid hanging connections"
                                })
            except Exception as e:
                logging.error(f"Error analyzing {file_path}: {e}")
        
        # Display results
        if bottlenecks:
            print("\nCode Bottleneck Analysis:")
            data = []
            for b in bottlenecks:
                data.append([
                    f"{b['file']}:{b['line']}",
                    b['description'],
                    b['code']
                ])
            
            print(tabulate(data, headers=["Location", "Issue", "Code"], tablefmt="grid"))
        else:
            print("\nNo obvious code bottlenecks found.")
        
        return bottlenecks

    def generate_optimization_recommendations(self):
        """Generate optimization recommendations based on all analyses"""
        logging.info("Generating optimization recommendations...")
        
        # At this point, self.recommendations should already contain findings from individual analyses
        # We'll add some general recommendations based on best practices
        
        general_recommendations = [
            {
                "type": "General Optimization",
                "severity": "MEDIUM",
                "component": "System",
                "description": "Consider implementing connection pooling",
                "suggestion": "Use connection pooling for databases and HTTP requests to reduce connection overhead"
            },
            {
                "type": "General Optimization",
                "severity": "MEDIUM",
                "component": "Crawler",
                "description": "Implement domain-aware crawling",
                "suggestion": "Group URLs by domain and assign domains to specific crawler nodes to improve robots.txt caching and reduce DNS lookups"
            },
            {
                "type": "General Optimization",
                "severity": "LOW",
                "component": "Indexer",
                "description": "Batch indexing operations",
                "suggestion": "Process documents in batches rather than one at a time to reduce index commit overhead"
            },
            {
                "type": "General Optimization",
                "severity": "MEDIUM",
                "component": "Master Node",
                "description": "Implement adaptive crawl rate limiting",
                "suggestion": "Dynamically adjust crawl rates based on domain response times and server load"
            }
        ]
        
        self.recommendations.extend(general_recommendations)
        
        # Sort recommendations by severity
        self.recommendations.sort(key=lambda x: {
            "HIGH": 0,
            "MEDIUM": 1,
            "LOW": 2
        }.get(x["severity"], 3))
        
        # Display recommendations
        print("\n===== PERFORMANCE OPTIMIZATION RECOMMENDATIONS =====")
        
        for severity in ["HIGH", "MEDIUM", "LOW"]:
            recs = [r for r in self.recommendations if r["severity"] == severity]
            if recs:
                print(f"\n----- {severity} PRIORITY RECOMMENDATIONS -----")
                for rec in recs:
                    print(f"\n[{rec['type']}] {rec['description']}")
                    print(f"Component: {rec['component']}")
                    print(f"Suggestion: {rec['suggestion']}")
        
        # Save recommendations to file
        try:
            with open('performance_recommendations.json', 'w') as f:
                json.dump({
                    "timestamp": datetime.now().isoformat(),
                    "metrics": self.metrics,
                    "recommendations": self.recommendations
                }, f, indent=2)
                
            logging.info("Recommendations saved to performance_recommendations.json")
        except Exception as e:
            logging.error(f"Error saving recommendations: {e}")
        
        return self.recommendations

    def run_analysis(self, skip_s3=False, skip_sqs=False, duration_minutes=2):
        """Run all performance analyses"""
        logging.info("Starting comprehensive performance analysis...")
        
        # Analyze API response times
        self.measure_api_response_times()
        
        # Analyze crawl rate
        self.analyze_crawl_rate(duration_minutes=duration_minutes)
        
        # Analyze index performance
        self.analyze_index_performance()
        
        # Analyze S3 if not skipped
        if not skip_s3 and self.s3_bucket:
            self.analyze_s3_performance()
        
        # Analyze SQS if not skipped
        if not skip_sqs:
            self.analyze_sqs_performance()
        
        # Analyze code for bottlenecks
        self.analyze_code_for_bottlenecks()
        
        # Generate recommendations
        self.generate_optimization_recommendations()
        
        print("\nPerformance analysis complete. See performance_recommendations.json for full details.")
        return True

def main():
    parser = argparse.ArgumentParser(description='Performance Optimizer for Distributed Web Crawler')
    parser.add_argument('--master', default='http://localhost:5000', help='URL of the master node')
    parser.add_argument('--indexer', default='http://localhost:5002', help='URL of the indexer node')
    parser.add_argument('--s3-bucket', help='S3 bucket name for content storage')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--skip-s3', action='store_true', help='Skip S3 performance analysis')
    parser.add_argument('--skip-sqs', action='store_true', help='Skip SQS performance analysis')
    parser.add_argument('--duration', type=int, default=2, help='Duration in minutes for crawl rate test')
    
    args = parser.parse_args()
    
    optimizer = PerformanceOptimizer(
        master_url=args.master,
        indexer_url=args.indexer,
        s3_bucket=args.s3_bucket,
        region_name=args.region
    )
    
    optimizer.run_analysis(
        skip_s3=args.skip_s3,
        skip_sqs=args.skip_sqs,
        duration_minutes=args.duration
    )

if __name__ == "__main__":
    main() 