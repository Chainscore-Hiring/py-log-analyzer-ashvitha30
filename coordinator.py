import asyncio
import json
from aiohttp import web
from typing import Dict
from analyzer import Analyzer

class Coordinator:
    """Manages workers and aggregates results."""
    
    def __init__(self, port: int):
        self.port = port
        self.workers = {}  # Dictionary to store worker IDs and their ports
        self.results = {}  # To store the results of processed chunks
        self.failed_workers = set()  # Set to keep track of failed workers
    
    async def worker_result(self, request):
        """Receive results from workers."""
        data = await request.json()
        worker_id = data["worker_id"]
        metrics = data["metrics"]

        # Store worker results
        self.results[worker_id] = metrics
        print(f"Results received from worker {worker_id}: {metrics}")
        
        return web.json_response({"status": "success"})

   

    
    
    async def finalize_and_analyze(self):
        """After processing all chunks, call the Analyzer to generate a report."""
        # Assume results are aggregated in self.results
        analyzer = Analyzer(self.coordinator_url)
        await analyzer.run_analysis()   
    
    async def get_results(self, request):
        """Return the results of all processed chunks."""
        return web.json_response(self.results)

    
     
    async def process_chunk(self, request):
        """Handle processing of log chunks sent by workers"""
        data = await request.json()
        worker_id = data.get("worker_id")
        chunk_data = data.get("chunk_data")

        # Simulate chunk processing
        self.results[worker_id] = chunk_data

        # After processing, aggregate the results
        await self.aggregate_results({"worker_id": worker_id, "metrics": chunk_data})

        return web.Response(text="Chunk processed successfully")


    async def register_worker(self, request: web.Request):
        """Register a worker with the coordinator."""
        data = await request.json()
        worker_id = data.get("worker_id")
        worker_port = data.get("port")
        
        if worker_id and worker_port:
            self.workers[worker_id] = worker_port
            print(f"Worker {worker_id} registered with port {worker_port}")
            return web.json_response({"status": "success"})
        return web.json_response({"status": "failure", "message": "Invalid worker data"})

    async def worker_health(self, request):
        """Handle health check requests from workers."""
        data = await request.json()  # Get the worker ID from the request body
        worker_id = data.get("worker_id")
        
        if worker_id:
            print(f"Health check received from worker {worker_id}")
            return web.json_response({"status": "success", "message": "Worker is alive"})
        else:
            return web.json_response({"status": "failure", "message": "Worker ID missing"})

    async def distribute_work(self, filepath: str):
        """Split file and assign chunks to workers."""
        chunk_size = 1024 * 1024  # 1MB per chunk

        with open(filepath, "rb") as f:
            f.seek(0, 2)
            file_size = f.tell()
            f.seek(0)

            chunk_count = file_size // chunk_size + (1 if file_size % chunk_size != 0 else 0)

            print(f"Distributing {chunk_count} chunks of work to workers.")

            # Assign work to workers
            chunk_index = 0
            for worker_id, worker_port in self.workers.items():
                if chunk_index >= chunk_count:
                    break

                start_byte = chunk_index * chunk_size
                size = chunk_size if chunk_index < chunk_count - 1 else file_size - start_byte

                # Sending chunk to worker
                await self.send_chunk_to_worker(worker_id, worker_port, filepath, start_byte, size)
                chunk_index += 1             
   
    async def get_results(self, request):
        """Fetch the results stored by the coordinator."""
        return web.json_response(self.results)


    async def send_chunk_to_worker(self, worker_id: str, worker_port: int, filepath: str, start_byte: int, size: int):
        """Send chunk details to worker."""
        worker_url = f"http://localhost:{worker_port}/process_chunk"
        data = {
            "filepath": filepath,
            "start_byte": start_byte,
            "size": size
        }
        
        async with web.ClientSession() as session:
            async with session.post(worker_url, json=data) as response:
                if response.status == 200:
                    print(f"Chunk {start_byte}-{start_byte+size} sent to worker {worker_id}")
                else:
                    print(f"Failed to send chunk {start_byte}-{start_byte+size} to worker {worker_id}")
                    self.failed_workers.add(worker_id)

    async def handle_worker_failure(self, worker_id: str):
        """Reassign work from failed worker."""
        print(f"Worker {worker_id} has failed. Reassigning tasks.")
        
        # Here you would want to track which chunks were assigned to the failed worker
        # Reassign those chunks to remaining workers
        # For simplicity, we just print the failure for now.

    async def aggregate_results(self, result: Dict):
        """Aggregate results from workers."""
        worker_id = result["worker_id"]
        self.results[worker_id] = result["metrics"]
        print(f"Aggregated results from worker {worker_id}: {result['metrics']}")



    async def start_server(self):
        """Start the HTTP server for processing requests."""
        app = web.Application()

        # Register routes
        app.router.add_post("/process_chunk", self.process_chunk)
        app.router.add_post("/register_worker", self.register_worker)
        app.router.add_post("/worker_health", self.worker_health)  # Add the health check route
        app.router.add_get("/get_results", self.get_results)
        app.router.add_post("/worker_result", self.worker_result)
        # Start the server
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", self.port)
        print(f"Coordinator running at http://localhost:{self.port}")
        await site.start()

        # Keep the server running
        while True:
            await asyncio.sleep(3600)  # Keep the server alive for 1 hour

# Run the Coordinator
if __name__ == "__main__":
    port = 8000  # Default port
    coordinator = Coordinator(port)

    # Run the server
    asyncio.run(coordinator.start_server())
