# Distributed Web Crawling System - Quick Start Guide

## Setup Instructions

### Master Node

```bash
git clone https://github.com/yourusername/Distributed-Web-Crawling-System.git
cd Distributed-Web-Crawling-System
pip install -r requirements.txt
cd master
python master_node.py --use-sqs --region eu-north-1 --seed-urls https://example.com
```

### Crawler Node

```bash
git clone https://github.com/yourusername/Distributed-Web-Crawling-System.git
cd Distributed-Web-Crawling-System
pip install -r requirements.txt
cd crawler
python crawler_node.py --master-url http://172.31.29.194:5000 --use-sqs --region eu-north-1
```

### Indexer Node

```bash
git clone https://github.com/yourusername/Distributed-Web-Crawling-System.git
cd Distributed-Web-Crawling-System
pip install -r requirements.txt
mkdir -p indexer/templates
echo '<html><head><title>Search</title></head><body><h1>Search</h1><form action="/" method="get"><input type="text" name="q" value="{{query}}"><input type="submit" value="Search"></form>{% if results %}<h2>Results</h2><ul>{% for result in results %}<li><a href="{{result.url}}">{{result.title}}</a><p>{{result.snippet|safe}}</p></li>{% endfor %}</ul>{% endif %}</body></html>' > indexer/templates/search.html
cd indexer
python indexer_node.py --master-url http://172.31.29.194:5000 --region eu-north-1
```

## Client Commands

### Add URLs to Crawl

```bash
python monitor.py --master http://172.31.29.194:5000 add https://example.com https://wikipedia.org
```

### Check System Status

```bash
python monitor.py --master http://172.31.29.194:5000 status
```

### Search Crawled Content

```bash
python monitor.py --indexer http://172.31.27.238:5002 search "your search query"
```

### Open Web Interface

```bash
python monitor.py --indexer http://172.31.27.238:5002 web
```

> **NOTE:** If the web interface doesn't work using the private IP, use the public IP of the indexer instance instead.

### Monitor SQS Queues

```bash
python monitor.py --region eu-north-1 sqs
```

### Check S3 Storage

```bash
python monitor.py --region eu-north-1 s3
```

## Security Group Settings

- Open port `5000` on Master Node (`172.31.29.194`)
- Open port `5001` on Crawler Node (`172.31.29.194`)
- Open port `5002` on Indexer Node (`172.31.27.238`)
- Use `0.0.0.0/0` as source for testing

## Requirements

- Python 3.7+
- AWS account with configured credentials
- Required Python packages:
  - flask
  - requests
  - boto3
  - beautifulsoup4
  - whoosh
  - tabulate

## Troubleshooting

- If web interface doesn't load, check security group settings
- Ensure all services are running on their respective instances
- Verify AWS credentials are properly configured
- Check log files in each component's directory
