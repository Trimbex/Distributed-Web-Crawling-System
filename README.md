# Distributed Web Crawling System

A scalable distributed web crawling system designed to efficiently crawl and process web pages across multiple nodes.

## Overview

This system implements a distributed architecture for web crawling, allowing for parallel processing of web pages across multiple worker nodes coordinated by a central manager.

## Prerequisites

- Python 3.7+
- Required Python packages (install via pip):

# Instructions

pip install mpi4py requests beautifulsoup4

Download https://www.microsoft.com/en-us/download/details.aspx?id=57467 (both .exe and .msi)

## Run with 2 crawler nodes (resulting in 4 total MPI processes: 1 master, 2 crawlers, 1 indexer)

python run_crawler.py 2

python run_crawler.py search
