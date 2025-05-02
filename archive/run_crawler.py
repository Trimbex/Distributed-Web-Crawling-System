import subprocess
import sys
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Runner - %(levelname)s - %(message)s')

def run_crawler_system(num_crawler_nodes=2):
    """
    Run the distributed web crawler system with MPI.
    
    Args:
        num_crawler_nodes (int): Number of crawler nodes to run
    """
    # Total nodes = 1 master + num_crawler_nodes + 1 indexer
    total_nodes = 1 + num_crawler_nodes + 1
    
    try:
        # Start the MPI processes
        logging.info(f"Starting crawler system with {num_crawler_nodes} crawler nodes...")
        cmd = ["mpiexec",
    "--oversubscribe", "-n", str(total_nodes), "python", "master_node.py"]
        
        proc = subprocess.Popen(cmd)
        
        # Let the process run
        logging.info("Crawler system running. Press Ctrl+C to stop.")
        proc.wait()
        
    except KeyboardInterrupt:
        logging.info("Stopping crawler system...")
        proc.terminate()
        
    except Exception as e:
        logging.error(f"Error running crawler system: {e}")
        if 'proc' in locals():
            proc.terminate()

def run_search_client():
    """Run the search client separately."""
    try:
        # Start the client
        logging.info("Starting search client...")
        subprocess.run(["python", "client.py"])
        
    except Exception as e:
        logging.error(f"Error running search client: {e}")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == 'search':
            run_search_client()
        else:
            try:
                num_nodes = int(sys.argv[1])
                run_crawler_system(num_nodes)
            except ValueError:
                print(f"Invalid argument: {sys.argv[1]}")
                print("Usage: python run_crawler.py [num_crawler_nodes|search]")
    else:
        # Default: run with 2 crawler nodes
        run_crawler_system(2)
