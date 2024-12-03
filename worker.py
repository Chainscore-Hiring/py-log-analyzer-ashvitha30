import argparse
import asyncio
import json
from aiohttp import web, ClientSession
from typing import Dict

class Worker:
    """Processes log chunks and reports results"""
    
    def __init__(self, worker_id: str, coordinator_url: str, port: int):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url
        self.port = port

    async def register_with_coordinator(self):
        """Register the worker with the coordinator."""
        url = f"{self.coordinator_url}/register_worker"
        data = {"worker_id": self.worker_id, "port": self.port}
        async with ClientSession() as session:
            try:
                async with session.post(url, json=data) as response:
                    result = await response.json()
                    if response.status == 200:
                        print(f"Worker {self.worker_id} registered successfully.")
                    else:
                        print(f"Failed to register worker {self.worker_id}: {result.get('message', 'Unknown error')}")
            except Exception as e:
                print(f"Error registering worker {self.worker_id}: {str(e)}")

    async def process_chunk(self, request: web.Request):
        """Process a chunk of the log file."""
        data = await request.json()
        filepath = data["filepath"]
        start_byte = data["start_byte"]
        size = data["size"]

        # Simulate processing delay
        await asyncio.sleep(1)

        lines_processed = size // 100  # Mock metric
        metrics = {"lines_processed": lines_processed}

        print(f"Worker {self.worker_id} processed chunk {start_byte}-{start_byte + size}.")

        # Send the result back to the coordinator
        await self.send_result_to_coordinator(metrics)
        return web.json_response({"status": "success", "metrics": metrics})


    async def send_result_to_coordinator(self, metrics: Dict):
        """Send processing results back to the coordinator."""
        url = f"{self.coordinator_url}/worker_result"
        data = {
            "worker_id": self.worker_id,
            "metrics": metrics
        }
        async with ClientSession() as session:
            try:
                async with session.post(url, json=data) as response:
                    result = await response.json()
                    if response.status == 200:
                        print(f"Worker {self.worker_id} result sent to coordinator.")
                    else:
                        print(f"Failed to send result from worker {self.worker_id}: {result.get('message', 'Unknown error')}")
            except Exception as e:
                print(f"Error sending result from worker {self.worker_id}: {str(e)}")


    async def report_health(self):
        """Send heartbeat to coordinator."""
        url = f"{self.coordinator_url}/worker_health"
        data = {"worker_id": self.worker_id}
        async with ClientSession() as session:
            try:
                async with session.post(url, json=data) as response:
                    result = await response.json()
                    if response.status == 200:
                        print(f"Worker {self.worker_id} is alive.")
                    else:
                        print(f"Failed to report health for worker {self.worker_id}: {result.get('message', 'Unknown error')}")
            except Exception as e:
                print(f"Error reporting health for worker {self.worker_id}: {str(e)}")
                
    async def worker_health(self, request):
        """Handle health check requests from workers"""
        data = await request.json()  # Get the worker ID from the request body
        worker_id = data.get("worker_id")

        # Just a simple check to confirm the worker is alive
        print(f"Received health check from worker {worker_id}")
        return web.json_response({"status": "success", "message": "Worker is alive"})

    async def start_server(self):
        """Start the HTTP server and register routes"""
        app = web.Application()

        # Register routes
        app.router.add_post("/process_chunk", self.process_chunk)
        app.router.add_post("/worker_health", self.worker_health)  # Register worker health check route

        # Start the server
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", self.port)
        print(f"Worker {self.worker_id} running at http://localhost:{self.port}")
        await site.start()

        # Register with the coordinator
        await self.register_with_coordinator()

        # Periodically send health reports
        while True:
            await self.report_health()
            await asyncio.sleep(30)  # Send health report every 30 seconds

# Run the Worker
if __name__ == "__main__":
    # Parse arguments to get worker_id and port
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker_id', type=str, required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--coordinator', type=str, required=True)
    args = parser.parse_args()

    worker_id = args.worker_id
    port = args.port
    coordinator_url = args.coordinator  # The coordinator URL

    worker = Worker(worker_id, coordinator_url, port)
    asyncio.run(worker.start_server())
