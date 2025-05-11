# WebCrawl - Distributed Web Crawling System

A high-performance, distributed web crawling and search system built with Python. This project implements a complete search engine solution with distributed crawling, content indexing, and a modern search interface.

<div align="center">
![video alt](https://github.com/Trimbex/Distributed-Web-Crawling-System/blob/00b96ccfda85df60515f7db08ae806e5c713bc05/images/2025-05-11%2022-27-57.mp4)
  <p><i>WebCrawl System Demo</i></p>
  <br>
  <img src="images/WhatsApp Image 2025-05-11 at 22.04.17_063f254b.jpg" alt="WebCrawl Search Interface" width="800"/>
  <br><br>
  <img src="images/WhatsApp Image 2025-05-11 at 22.04.03_89cc0683.jpg" alt="WebCrawl Results" width="800"/>
</div>

## Features

### Distributed Architecture
- **Master Node**: Coordinates crawling tasks and manages the URL frontier
- **Multiple Crawler Nodes**: Independently fetch and process web pages in parallel
- **Indexer Node**: Processes crawled content for efficient search
- **AWS Integration**: Optional SQS queues for task distribution and S3 for content storage
- **Horizontal Scaling**: Add more crawler nodes dynamically to increase throughput

### Crawler Features
- **Robots.txt Compliance**: Respects website crawling policies
- **Politeness Mechanisms**: Rate limiting and crawl delays to avoid overloading websites
- **Recursive Crawling**: Discovers new URLs and adds them to the crawl frontier
- **Content Extraction**: Parses HTML to extract valuable text and links
- **Error Handling**: Robust error recovery and retry mechanisms

### Indexing & Search
- **Full-Text Indexing**: Uses Whoosh for efficient content indexing
- **Relevance Ranking**: BM25F algorithm for high-quality search results
- **Field-Specific Search**: Supports querying by title, content, or domain
- **Query Highlighting**: Shows matching terms in context
- **Rich Search Results**: Displays titles, snippets, domains, and metadata

### Web Interface
- **Clean Modern UI**: Responsive search interface with clear result display
- **Real-Time Statistics**: Shows system performance metrics
- **Minimalist Design**: Focused on search functionality
- **Cross-Browser Compatible**: Works across modern browsers

### System Features
- **Fault Tolerance**: Automatic recovery from node failures
- **Heartbeat Mechanism**: Detects and handles crawler node failures
- **Task Requeuing**: Failed tasks are automatically retried
- **Comprehensive Logging**: Detailed logs for monitoring and debugging
- **Scalability Testing**: Built-in tools to measure performance with different configurations

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│                 │     │                  │     │                  │
│  Master Node    │◄────┤  Crawler Node 1  │     │  Crawler Node 2  │
│  (Coordinator)  │     │                  │     │                  │
│                 │────►│                  │     │                  │
└────────┬────────┘     └──────────┬───────┘     └──────────┬───────┘
         │                         │                        │
         │                         │                        │
         ▼                         ▼                        ▼
┌─────────────────┐      ┌──────────────────┐     ┌──────────────────┐
│                 │      │                  │     │                  │
│  Amazon SQS     │      │  Amazon S3       │     │  Indexer Node    │
│  (Task Queues)  │      │  (Content Store) │     │  (Search Engine) │
│                 │      │                  │     │                  │
└─────────────────┘      └──────────────────┘     └──────────────────┘
```

## Setup

### Prerequisites
- Python 3.7+
- AWS account (optional, for S3 and SQS features)
- boto3 Python library (for AWS integration)
- Flask, Whoosh, Beautiful Soup, Requests

### Installation

1. Clone the repository
```bash
git clone https://github.com/yourusername/webCrawl.git
cd webCrawl
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials (optional)
```bash
aws configure
# Follow prompts to enter your AWS Access Key, Secret Key, and region
```

### Starting the System

1. **Start the Master Node**
```bash
python master/master_node.py --port 5000
```

2. **Start the Indexer Node**
```bash
python indexer/indexer_node.py --port 5002 --crawler-api http://<MASTER_IP>:5000
```

3. **Start Crawler Nodes** (can be on separate machines)
```bash
python crawler/crawler_node.py --master http://<MASTER_IP>:5000 --s3-bucket your-crawler-bucket --use-sqs
```

To run multiple crawler processes on the same machine:
```bash
# In separate terminal windows:
python crawler/crawler_node.py --master http://<MASTER_IP>:5000 --s3-bucket your-crawler-bucket --use-sqs
python crawler/crawler_node.py --master http://<MASTER_IP>:5000 --s3-bucket your-crawler-bucket --use-sqs
python crawler/crawler_node.py --master http://<MASTER_IP>:5000 --s3-bucket your-crawler-bucket --use-sqs
```

## Usage

### Adding Seed URLs
```bash
curl -X POST http://<MASTER_IP>:5000/add_urls -H "Content-Type: application/json" -d '{"urls":["https://en.wikipedia.org", "https://news.ycombinator.com"]}'
```

### Using the Search Interface
1. Open your browser and navigate to `http://<INDEXER_IP>:5002`
2. Enter your search query in the search box
3. Review the results with titles, snippets, and metadata

### Checking System Status
```bash
# Master node status
curl http://<MASTER_IP>:5000/status

# Indexer node status
curl http://<INDEXER_IP>:5002/status

# Run scalability tests
python test_system.py scalability --master http://<MASTER_IP>:5000 --indexer http://<INDEXER_IP>:5002 --crawlers <CRAWLER_IP_1> <CRAWLER_IP_2> <CRAWLER_IP_3>
```

## Performance Tuning

### Crawler Performance
- Adjust the number of crawler nodes based on desired throughput
- Modify `crawler_node.py` politeness settings to balance speed with website etiquette
- Ensure sufficient bandwidth and CPU resources on crawler machines

### Indexer Performance
- Increase instance size for better search performance
- Configure Whoosh indexing parameters in `indexer_node.py` for optimal search

### AWS Configuration
- Use appropriate SQS settings for visibility timeout
- Choose S3 storage class based on access patterns
- Consider using AWS Elastic Beanstalk or ECS for easier scaling

## Testing

The system includes comprehensive testing tools:

```bash
# Run all tests
python test_system.py all --master http://<MASTER_IP>:5000 --indexer http://<INDEXER_IP>:5002

# Run just functional tests
python test_system.py functional --master http://<MASTER_IP>:5000 --indexer http://<INDEXER_IP>:5002

# Run fault tolerance tests
python test_system.py fault --master http://<MASTER_IP>:5000 --indexer http://<INDEXER_IP>:5002

# Run scalability tests with specific crawler counts
python test_system.py scalability --master http://<MASTER_IP>:5000 --indexer http://<INDEXER_IP>:5002 --counts 1 2 4 8
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
