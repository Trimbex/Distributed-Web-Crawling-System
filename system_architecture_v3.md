# Distributed Web Crawling System - Architecture Document (V3)

## 1. System Overview

The Distributed Web Crawling System is a scalable, fault-tolerant infrastructure for crawling, processing, and indexing web content. It uses a distributed architecture with multiple specialized nodes working together to efficiently crawl the web at scale.

### 1.1 System Components

The system consists of the following core components:

1. **Master Node**: Coordinates crawling tasks, monitors system health, and handles fault recovery
2. **Crawler Nodes**: Perform web page fetching, content extraction, and URL discovery
3. **Indexer Node**: Processes, indexes, and makes content searchable
4. **Storage Layer**: Stores crawled content, extracted data, and system state
5. **Message Queues**: Enable asynchronous communication between components
6. **Monitoring System**: Tracks system health, performance metrics, and provides dashboards

### 1.2 Key Features

- **Distributed Processing**: Multiple crawler nodes working in parallel
- **Fault Tolerance**: Heartbeat monitoring, task timeout detection, and task requeuing
- **Enhanced Indexing**: Robust search capabilities using Whoosh with advanced text processing
- **Data Persistence**: Crawled content stored in cloud storage (AWS S3)
- **Monitoring**: Comprehensive system monitoring with logging and performance tracking
- **Web Interface**: Simple web UI for search and system status

## 2. Architectural Patterns

### 2.1 Master-Worker Pattern

The system follows a master-worker pattern where:
- The Master Node manages the work queue and assigns tasks
- Crawler Nodes (workers) execute crawling tasks independently
- The Indexer Node processes and indexes the crawled content

### 2.2 Message Queue Pattern

The system uses message queues to:
- Decouple components for improved scalability
- Enable asynchronous processing
- Provide a buffer during traffic spikes
- Support task persistence and reprocessing

### 2.3 Fault Tolerance Patterns

- **Heartbeat Monitoring**: Crawler nodes send periodic heartbeats to the master
- **Task Timeout**: Master monitors task execution time and reacts to stalled tasks
- **Task Requeuing**: Failed or stalled tasks are automatically requeued for processing
- **Data Persistence**: Critical data is persisted to allow recovery from failures

## 3. Component Details

### 3.1 Master Node

**Purpose**: Coordinates the crawling process and system health

**Responsibilities**:
- Maintain the URL frontier (queue of URLs to crawl)
- Distribute crawling tasks to crawler nodes
- Track crawler node health through heartbeat monitoring
- Detect stalled or failed tasks and requeue them
- Maintain crawl state and statistics
- Expose API for system control and monitoring

**Implementation**:
- Python Flask application with REST API
- Uses SQS for task distribution and result collection
- Tracks crawler node health with a heartbeat mechanism
- Implements task timeout detection and requeuing
- Stores state in memory with periodic persistence

### 3.2 Crawler Nodes

**Purpose**: Fetch and process web pages

**Responsibilities**:
- Retrieve URLs from the task queue
- Fetch web pages while respecting robots.txt and politeness policies
- Extract content and discover new URLs
- Store crawled content in cloud storage
- Send extracted URLs back to the master node
- Send heartbeats to indicate health status

**Implementation**:
- Python application with HTTP client for web fetching
- BeautifulSoup for HTML parsing and content extraction
- Robots.txt parsing and caching for politeness
- AWS S3 SDK for content storage
- Configurable crawl rate limiting and politeness measures
- Heartbeat mechanism for health monitoring

### 3.3 Indexer Node

**Purpose**: Process and index crawled content

**Responsibilities**:
- Create and maintain a searchable index of crawled content
- Process and analyze text content for better search quality
- Provide search functionality via a REST API
- Expose a web interface for search
- Track indexing statistics

**Implementation**:
- Python Flask application with REST API
- Whoosh for text indexing and search
- Stemming analyzer for improved text matching
- Field-specific and boolean operator search support
- Web interface for search using HTML/CSS/JS

### 3.4 Storage Layer

**Purpose**: Persist crawled data and system state

**Components**:
- **Content Storage**: AWS S3 for raw HTML and processed text
  - HTML documents stored in `html/` prefix
  - Processed text stored in `text/` prefix
  - Content addressable by URL hash
  
- **Task Queues**: AWS SQS for task distribution
  - `crawler-tasks` queue for pending URLs
  - `crawler-results` queue for processing results

- **State Storage**: Local file system for system state
  - Periodic snapshots of crawler state
  - Index data persistence

### 3.5 Monitoring System

**Purpose**: Track system health and performance

**Features**:
- Comprehensive logging across all components
- Real-time system status dashboard
- Crawling and indexing rate monitoring
- Crawler node health tracking
- Error rate monitoring
- S3 storage utilization tracking
- SQS queue depth monitoring

**Implementation**:
- Python logging framework with structured logging
- Command-line monitoring tool (`monitor.py`)
- API endpoints for health and status reporting
- JSON-based metrics export

## 4. Communication Flows

### 4.1 Task Distribution Flow

1. Master Node adds URLs to the `crawler-tasks` SQS queue
2. Crawler Nodes poll the queue for new URLs to crawl
3. Crawler Nodes process URLs and store content in S3
4. Crawler Nodes send extraction results to the `crawler-results` SQS queue
5. Master Node processes results and adds new URLs to the queue

### 4.2 Fault Detection Flow

1. Crawler Nodes send heartbeats to the Master Node every 30 seconds
2. Master Node tracks the last heartbeat time for each Crawler Node
3. If no heartbeat is received within a timeout period (e.g., 60 seconds):
   - Master Node marks the Crawler Node as failed
   - Any in-progress tasks from the failed node are requeued
4. If a task exceeds its maximum execution time:
   - Master Node considers the task stalled
   - The task is requeued for processing by another node

### 4.3 Content Indexing Flow

1. Crawler Nodes store raw HTML content in S3 (`html/` prefix)
2. Crawler Nodes extract and process text content
3. Crawler Nodes send content to the Indexer Node via HTTP
4. Indexer Node analyzes and indexes the content using Whoosh
5. Indexed content becomes searchable via the search API

## 5. Fault Tolerance

### 5.1 Failure Detection

- **Crawler Node Failures**:
  - Detected through missing heartbeats
  - Configurable heartbeat timeout (default: 60 seconds)
  - Node status tracked in Master Node memory

- **Task Stalling**:
  - Tasks have a maximum execution time
  - Master Node monitors task progress
  - Stalled tasks are automatically requeued

- **Master Node Failures**:
  - State periodically persisted to disk
  - SQS queues maintain task state outside the Master Node
  - System can recover by reloading state and resuming operations

### 5.2 Recovery Mechanisms

- **Task Requeuing**:
  - Failed or stalled tasks are returned to the queue
  - SQS visibility timeout automatically returns unacknowledged messages
  - Master Node can explicitly requeue known failed tasks

- **Node Recovery**:
  - Crawler Nodes can be restarted and automatically rejoin the system
  - New nodes can be added to scale out or replace failed nodes
  - Nodes automatically reconnect to the Master and request work

- **Data Durability**:
  - All crawled content stored in S3 for durability
  - Critical system state periodically persisted
  - Search index saved to disk at regular intervals

## 6. Scalability

### 6.1 Horizontal Scaling

- **Crawler Node Scaling**:
  - Additional crawler nodes can be added at any time
  - No configuration changes needed in the Master Node
  - New nodes automatically register and receive work
  - Linear scaling of crawl rate with node count (with diminishing returns)

- **Master Node Scaling**:
  - Single Master Node with task distribution via SQS
  - SQS provides a buffer during traffic spikes
  - Master Node focuses on coordination, not content processing

- **Indexer Scaling**:
  - Currently single-node implementation
  - Could be extended to a distributed index (future enhancement)

### 6.2 Performance Characteristics

- **Crawl Rate**:
  - Primarily limited by crawler node count and politeness policies
  - Typical rate: 5-20 URLs per minute per crawler node
  - Scales approximately linearly with crawler node count
  - Becomes network and I/O bound at higher scales

- **Indexing Rate**:
  - Limited by Whoosh single-node performance
  - Typical rate: 10-30 documents per second
  - Can be improved with batch processing

- **Search Performance**:
  - Sub-second query response for most queries
  - Performance degrades with index size and query complexity
  - Optimized for small to medium index sizes (up to ~1M documents)

## 7. Security Considerations

### 7.1 Data Security

- **Content Storage**:
  - S3 bucket with appropriate access controls
  - Content hashed for storage to prevent path traversal
  - No storage of sensitive user data

- **Communication Security**:
  - Internal component communication over HTTP
  - Could be enhanced with HTTPS for production
  - AWS service communication uses SDK with secure credentials

### 7.2 Access Control

- **System Access**:
  - API endpoints lack authentication in current version
  - Instance-level security via AWS security groups
  - Future enhancement: Add API key authentication

- **AWS Resource Access**:
  - IAM roles with least privilege principle
  - S3 bucket policies limiting access to system components
  - SQS queue policies restricting access

## 8. Monitoring and Operations

### 8.1 System Monitoring

- **Component Health**:
  - Heartbeat monitoring for crawler nodes
  - API endpoints exposing health status
  - Node status in monitoring dashboard

- **Performance Metrics**:
  - Crawl rate tracking (URLs/minute)
  - Index size and document count
  - Search query response time
  - Queue depths and processing rates

- **Resource Utilization**:
  - S3 storage usage
  - SQS queue statistics
  - Instance resource monitoring (future enhancement)

### 8.2 Troubleshooting Tools

- **Logging**:
  - Detailed logging in all components
  - Log levels configurable at runtime
  - Structured logs for easier parsing

- **Monitoring Scripts**:
  - `monitor.py` for system status and statistics
  - Queue inspection tools
  - S3 content browser

- **Health Checks**:
  - API endpoints for component self-checks
  - System-wide health check functionality

## 9. Future Enhancements

- **Distributed Indexing**: Implement a distributed search index for improved scalability
- **Content Classification**: Add ML-based classification of crawled content
- **Advanced Scheduling**: Prioritized URL queue with importance-based crawling
- **Enhanced Security**: Add authentication and encryption for all communications
- **Containerization**: Package components as Docker containers for easier deployment
- **Kubernetes Deployment**: Orchestrate components with Kubernetes for improved operations
- **Content Deduplication**: Detect and handle duplicate content
- **Language Detection**: Automatically identify content language for better search
- **Domain-Specific Crawling**: Optimize crawling for specific website patterns
- **Link Graph Analysis**: Build and analyze the web graph from crawled data

## 10. AWS Architecture Diagram

```
+------------------+       +-------------------+       +-------------------+
|                  |       |                   |       |                   |
|  AWS EC2         |       |  AWS EC2          |       |  AWS EC2          |
|  Master Node     |<----->|  Crawler Node(s)  |<----->|  Indexer Node     |
|  [master_node.py]|       |  [crawler_node.py]|       |  [indexer_node.py]|
|                  |       |                   |       |                   |
+--------+---------+       +--------+----------+       +---------+---------+
         ^                          ^                            ^
         |                          |                            |
         v                          v                            v
+--------+--------------------------+----------------------------+---------+
|                                                                          |
|  AWS SQS                                                                 |
|  +---------------------+    +------------------------+                   |
|  |                     |    |                        |                   |
|  |  crawler-tasks      |    |  crawler-results       |                   |
|  |  Queue              |    |  Queue                 |                   |
|  |                     |    |                        |                   |
|  +---------------------+    +------------------------+                   |
|                                                                          |
+--------+--------------------------+----------------------------+---------+
         ^                          ^                            ^
         |                          |                            |
         v                          v                            v
+--------+--------------------------+----------------------------+---------+
|                                                                          |
|  AWS S3 Bucket                                                           |
|  +---------------------+    +------------------------+                   |
|  |                     |    |                        |                   |
|  |  html/              |    |  text/                 |                   |
|  |  Raw HTML Storage   |    |  Processed Text        |                   |
|  |                     |    |                        |                   |
|  +---------------------+    +------------------------+                   |
|                                                                          |
+--------------------------------------------------------------------------+
```

## 11. Installation and Deployment

### 11.1 Prerequisites

- Python 3.7 or higher
- AWS account with permissions for EC2, S3, and SQS
- pip for dependency installation

### 11.2 Installation Steps

1. Clone the repository on each node
2. Install dependencies: `pip install -r requirements.txt`
3. Configure AWS credentials using environment variables or AWS CLI
4. Create S3 bucket and SQS queues if not existing

### 11.3 Deployment Configuration

- **Master Node**:
  ```
  python master/master_node.py --port 5000 --region us-east-1
  ```

- **Crawler Node**:
  ```
  python crawler/crawler_node.py --master-url http://master-ip:5000 --s3-bucket crawler-bucket --region us-east-1
  ```

- **Indexer Node**:
  ```
  python indexer/indexer_node.py --port 5002 --index-dir /path/to/index
  ```

- **Monitor**:
  ```
  python monitor.py --master http://master-ip:5000 --indexer http://indexer-ip:5002
  ```

## 12. Conclusion

This distributed web crawling system provides a scalable, fault-tolerant architecture for web crawling and indexing. With its master-worker pattern, message queue-based communication, and AWS cloud infrastructure, it can efficiently crawl and index web content while handling node failures and maintaining system stability. The enhanced monitoring, fault tolerance mechanisms, and improved search capabilities make it suitable for medium-scale web crawling projects.

Version 3 improves upon the previous architecture with more robust fault tolerance, enhanced monitoring, and better storage handling. Future enhancements will focus on distributed indexing, content classification, and advanced scheduling to further improve the system's capabilities. 