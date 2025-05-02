from mpi4py import MPI
import logging
import time
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Client - %(levelname)s - %(message)s')

def client_process():
    """Simple client process for testing the search functionality."""
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Assuming the last rank is the indexer
    indexer_rank = size - 1
    
    logging.info(f"Client started with rank {rank}")
    
    while True:
        try:
            print("\n=== Web Crawler Search Client ===")
            print("Enter a search query (or 'exit' to quit): ")
            query = input().strip()
            
            if query.lower() == 'exit':
                break
                
            if not query:
                continue
                
            # Send search query to indexer
            logging.info(f"Sending query '{query}' to indexer...")
            comm.send(query, dest=indexer_rank, tag=3)  # Tag 3 for search query
            
            # Add timeout for receiving results
            start_time = time.time()
            timeout = 5  # 5 seconds timeout
            
            # Non-blocking probe for response
            while time.time() - start_time < timeout:
                if comm.Iprobe(source=indexer_rank, tag=4):
                    # Receive results
                    results = comm.recv(source=indexer_rank, tag=4)  # Tag 4 for search results
                    
                    # Display results
                    print(f"\nFound {len(results)} results for '{query}':")
                    for i, url in enumerate(results, 1):
                        print(f"{i}. {url}")
                    break
                time.sleep(0.1)  # Small delay to prevent CPU hogging
            
            # If we timed out
            if time.time() - start_time >= timeout:
                print("\nNo response from indexer. Make sure the crawler system is running.")
                print("Try running 'python run_crawler.py 2' in another terminal window.")
                
        except Exception as e:
            logging.error(f"Search client error: {e}")
            
    print("Search client exiting.")

if __name__ == "__main__":
    client_process()