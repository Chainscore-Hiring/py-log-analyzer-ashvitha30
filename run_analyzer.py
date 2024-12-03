import argparse

from aiohttp import ClientSession
from analyzer import Analyzer
import asyncio

def main():
    parser = argparse.ArgumentParser(description="Run the log analyzer.")
    parser.add_argument('--coordinator', type=str, required=True, help="URL of the coordinator")
    args = parser.parse_args()
    # Initialize and run the analyzer
    analyzer = Analyzer(args.coordinator)
    asyncio.run(analyzer.run_analysis())
    
async def fetch_results_from_coordinator(self):
    """Fetch results from the coordinator."""
    url = f"{self.coordinator_url}/get_results"
    async with ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    results = await response.json()
                    print(f"Results fetched from coordinator: {results}")
                else:
                    print(f"Failed to fetch results: {response.status}")
        except Exception as e:
            print(f"Error fetching results: {str(e)}")

if __name__ == "__main__":
    main()
