import asyncio
import json
from aiohttp import ClientSession

class Analyzer:
    def __init__(self, coordinator_url):
        self.coordinator_url = coordinator_url
        self.results = {}  # This will hold the results fetched from the coordinator

    async def fetch_results_from_coordinator(self):
        """Fetch results from the coordinator."""
        url = f"{self.coordinator_url}/get_results"  # Use the correct endpoint
        async with ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        self.results = await response.json()  # Store the results
                        print(f"Results fetched from coordinator: {self.results}")
                    else:
                        print(f"Failed to fetch results: {response.status}")
            except Exception as e:
                print(f"Error fetching results: {str(e)}")

    async def run_analysis(self):
        """Run the log analysis after fetching results."""
        await self.fetch_results_from_coordinator()  # Fetch the results
        print("Analyzing results...")
        if not self.results:
            print("No results fetched.")
        else:
            print(f"Results: {self.results}")  # Now you should have results to analyze

