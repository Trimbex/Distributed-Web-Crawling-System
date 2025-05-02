from mpi4py import MPI
import time
import logging
import threading
import queue
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

class MasterNode:
    def __init__(self, comm, size):
        self.comm = comm
        self.size = size
        self.task_queue = queue.Queue()  # Local queue for this example
        self.visited_urls = set()  # Track visited URLs to avoid duplicates
        self.crawler_nodes = size - 2  # Assuming master and at least one indexer node
        self.indexer_nodes = 1  # At least one indexer node
        self.active_crawler_nodes = list(range(1, 1 + self.crawler_nodes))
        self.active_indexer_nodes = list(range(1 + self.crawler_nodes, size))
        self.crawler_status = {rank: "idle" for rank in self.active_crawler_nodes}
        self.tasks_assigned = 0
        self.tasks_completed = 0
        
    def load_seed_urls(self, seed_file=None):
        """Load seed URLs from a file or use defaults."""
        if seed_file and os.path.exists(seed_file):
            with open(seed_file, 'r') as f:
                seed_urls = [line.strip() for line in f if line.strip()]
        else:
            # Default seed URLs
            seed_urls = [
                "https://www.python.org/",
                "https://example.org",
                "https://www.wikipedia.org",
            ]
            
        for url in seed_urls:
            if url not in self.visited_urls:
                self.task_queue.put(url)
                self.visited_urls.add(url)
                
        logging.info(f"Loaded {len(seed_urls)} seed URLs")
        
    def assign_tasks(self):
        """Assign crawling tasks to available crawler nodes."""
        for crawler_rank in self.active_crawler_nodes:
            if self.crawler_status[crawler_rank] == "idle" and not self.task_queue.empty():
                url_to_crawl = self.task_queue.get()
                self.comm.send(url_to_crawl, dest=crawler_rank, tag=0)  # Tag 0 for task assignment
                self.crawler_status[crawler_rank] = "busy"
                self.tasks_assigned += 1
                logging.info(f"Assigned task (crawl {url_to_crawl}) to Crawler {crawler_rank}")
                
    def process_crawler_messages(self):
        """Process messages from crawler nodes."""
        status = MPI.Status()
        
        # Non-blocking check for incoming messages
        if self.comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            source = status.Get_source()
            tag = status.Get_tag()
            
            # Only process messages from crawler nodes
            if source in self.active_crawler_nodes:
                message = self.comm.recv(source=source, tag=tag)
                
                if tag == 1:  # Crawler completed task and sent back extracted URLs
                    self.tasks_completed += 1
                    self.crawler_status[source] = "idle"
                    
                    # Process new URLs
                    new_urls = message
                    for url in new_urls:
                        if url not in self.visited_urls:
                            self.task_queue.put(url)
                            self.visited_urls.add(url)
                            
                    logging.info(f"Received {len(new_urls)} URLs from Crawler {source}, " 
                                f"queue size: {self.task_queue.qsize()}, "
                                f"completed: {self.tasks_completed}/{self.tasks_assigned}")
                    
                elif tag == 99:  # Status update
                    logging.info(f"Status from Crawler {source}: {message}")
                    
                elif tag == 999:  # Error report
                    logging.error(f"Error from Crawler {source}: {message}")
                    self.crawler_status[source] = "idle"  # Mark crawler as available again
                    
    def save_state(self, filename="crawler_state.json"):
        """Save the current state of the crawler to a file."""
        state = {
            "visited_urls": list(self.visited_urls),
            "queue_size": self.task_queue.qsize(),
            "tasks_assigned": self.tasks_assigned,
            "tasks_completed": self.tasks_completed
        }
        
        with open(filename, 'w') as f:
            json.dump(state, f)
            
    def run(self):
        """Main loop for the master node."""
        self.load_seed_urls()
        
        save_interval = 30  # Save state every 30 seconds
        last_save_time = time.time()
        
        try:
            while not self.task_queue.empty() or any(status == "busy" for status in self.crawler_status.values()):
                self.assign_tasks()
                self.process_crawler_messages()
                
                # Periodically save state
                current_time = time.time()
                if current_time - last_save_time > save_interval:
                    self.save_state()
                    last_save_time = current_time
                    
                time.sleep(0.1)  # Small delay to prevent CPU hogging
                
            # When we're done, send shutdown signal to all crawler nodes
            for crawler_rank in self.active_crawler_nodes:
                self.comm.send(None, dest=crawler_rank, tag=0)
                
            logging.info("Master node finished URL distribution. All tasks completed.")
            
        except Exception as e:
            logging.error(f"Master node error: {e}")
            
        finally:
            # Final state save
            self.save_state()

def master_process():
    """Main process for the master node."""
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Only the process with rank 0 should be the master
    if rank != 0:
        # If not rank 0, determine if this is a crawler or indexer
        if rank < size - 1:
            # This is a crawler node
            from crawler_node import crawler_process
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')
            crawler_process()
        else:
            # This is the indexer node (last rank)
            from indexer_node import indexer_process
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')
            indexer_process()
        return
    
    logging.info(f"Master node started with rank {rank} of {size}")
    
    # Check if we have enough nodes
    if size < 3:  # Need at least one master, one crawler, one indexer
        logging.error("Not enough nodes to run crawler and indexer. Need at least 3 nodes.")
        for i in range(1, size):
            comm.send(None, dest=i, tag=0)  # Send shutdown signal to all nodes
        return
        
    master = MasterNode(comm, size)
    master.run()
    
    print("Master Node Finished.")

if __name__ == "__main__":
    master_process()