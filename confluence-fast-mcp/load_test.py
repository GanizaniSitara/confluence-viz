#!/usr/bin/env python3
"""Load test script for Confluence Fast MCP server - simulates 30+ concurrent users."""

import asyncio
import aiohttp
import time
import random
import statistics
from typing import List, Dict, Any
import argparse


class MCPLoadTester:
    """Load tester for MCP server."""

    def __init__(self, base_url: str = "http://localhost:8070"):
        self.base_url = base_url
        self.results: List[Dict[str, Any]] = []

    async def call_mcp_tool(self, session: aiohttp.ClientSession, tool: str,
                           params: Dict[str, Any], user_id: int) -> Dict[str, Any]:
        """Call an MCP tool and measure response time."""
        start_time = time.time()

        try:
            async with session.post(
                f"{self.base_url}/mcp/call",
                json={
                    "method": tool,
                    "params": params
                },
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                result = await response.json()
                elapsed = time.time() - start_time

                return {
                    "user_id": user_id,
                    "tool": tool,
                    "success": response.status == 200,
                    "status": response.status,
                    "elapsed": elapsed,
                    "error": None
                }
        except Exception as e:
            elapsed = time.time() - start_time
            return {
                "user_id": user_id,
                "tool": tool,
                "success": False,
                "status": 0,
                "elapsed": elapsed,
                "error": str(e)
            }

    async def simulate_user(self, user_id: int, num_requests: int = 10):
        """Simulate a single user making requests."""
        async with aiohttp.ClientSession() as session:
            for i in range(num_requests):
                # Simulate realistic usage patterns
                action = random.choice([
                    'list_spaces',
                    'list_spaces',  # More common
                    'search',
                    'search',  # More common
                    'get_page',
                ])

                if action == 'list_spaces':
                    result = await self.call_mcp_tool(
                        session,
                        'getConfluenceSpaces',
                        {'cloudId': 'local', 'maxResults': 50},
                        user_id
                    )
                    self.results.append(result)

                elif action == 'search':
                    # Random search terms
                    terms = ['api', 'test', 'documentation', 'guide', 'setup',
                            'kubernetes', 'docker', 'deployment', 'config']
                    query = random.choice(terms)
                    result = await self.call_mcp_tool(
                        session,
                        'search',
                        {'query': query, 'limit': 20},
                        user_id
                    )
                    self.results.append(result)

                elif action == 'get_page':
                    # Would need real page IDs, for now just test the endpoint
                    result = await self.call_mcp_tool(
                        session,
                        'getConfluenceSpaces',  # Fallback to safe call
                        {'cloudId': 'local', 'maxResults': 10},
                        user_id
                    )
                    self.results.append(result)

                # Small delay between requests from same user
                await asyncio.sleep(random.uniform(0.1, 0.5))

    async def run_load_test(self, num_users: int = 30, requests_per_user: int = 10):
        """Run load test with multiple concurrent users."""
        print(f"Starting load test: {num_users} users, {requests_per_user} requests each")
        print(f"Target: {self.base_url}")
        print("-" * 60)

        start_time = time.time()

        # Create tasks for all users
        tasks = [
            self.simulate_user(user_id, requests_per_user)
            for user_id in range(num_users)
        ]

        # Run all users concurrently
        await asyncio.gather(*tasks)

        total_time = time.time() - start_time

        # Analyze results
        self.print_results(total_time, num_users, requests_per_user)

    def print_results(self, total_time: float, num_users: int, requests_per_user: int):
        """Print load test results."""
        total_requests = len(self.results)
        successful = [r for r in self.results if r['success']]
        failed = [r for r in self.results if not r['success']]

        print("\n" + "=" * 60)
        print("LOAD TEST RESULTS")
        print("=" * 60)
        print(f"Total time: {total_time:.2f}s")
        print(f"Concurrent users: {num_users}")
        print(f"Requests per user: {requests_per_user}")
        print(f"Total requests: {total_requests}")
        print(f"Successful: {len(successful)} ({100*len(successful)/total_requests:.1f}%)")
        print(f"Failed: {len(failed)} ({100*len(failed)/total_requests:.1f}%)")

        if successful:
            times = [r['elapsed'] for r in successful]
            print("\nResponse Times (successful requests):")
            print(f"  Min: {min(times):.3f}s")
            print(f"  Max: {max(times):.3f}s")
            print(f"  Mean: {statistics.mean(times):.3f}s")
            print(f"  Median: {statistics.median(times):.3f}s")
            print(f"  95th percentile: {statistics.quantiles(times, n=20)[18]:.3f}s")

        print(f"\nThroughput: {total_requests/total_time:.2f} requests/second")

        # Group by tool
        by_tool = {}
        for r in successful:
            tool = r['tool']
            if tool not in by_tool:
                by_tool[tool] = []
            by_tool[tool].append(r['elapsed'])

        if by_tool:
            print("\nPerformance by Tool:")
            for tool, times in sorted(by_tool.items()):
                print(f"  {tool}:")
                print(f"    Count: {len(times)}")
                print(f"    Mean: {statistics.mean(times):.3f}s")
                print(f"    Median: {statistics.median(times):.3f}s")

        if failed:
            print(f"\nErrors ({len(failed)}):")
            error_counts = {}
            for r in failed:
                error = r.get('error', 'Unknown')
                error_counts[error] = error_counts.get(error, 0) + 1
            for error, count in sorted(error_counts.items(), key=lambda x: -x[1]):
                print(f"  {count}x {error}")

        print("\n" + "=" * 60)

        # Pass/Fail criteria
        success_rate = 100 * len(successful) / total_requests
        avg_response = statistics.mean([r['elapsed'] for r in successful]) if successful else 999

        if success_rate >= 95 and avg_response < 2.0:
            print("RESULT: PASS")
            print(f"  Success rate: {success_rate:.1f}% (>= 95%)")
            print(f"  Avg response: {avg_response:.3f}s (< 2.0s)")
        else:
            print("RESULT: FAIL")
            if success_rate < 95:
                print(f"  Success rate: {success_rate:.1f}% (expected >= 95%)")
            if avg_response >= 2.0:
                print(f"  Avg response: {avg_response:.3f}s (expected < 2.0s)")
        print("=" * 60)


async def main():
    parser = argparse.ArgumentParser(description='Load test Confluence Fast MCP server')
    parser.add_argument('--url', default='http://localhost:8070',
                       help='MCP server URL (default: http://localhost:8070)')
    parser.add_argument('--users', type=int, default=30,
                       help='Number of concurrent users (default: 30)')
    parser.add_argument('--requests', type=int, default=10,
                       help='Requests per user (default: 10)')

    args = parser.parse_args()

    tester = MCPLoadTester(args.url)
    await tester.run_load_test(args.users, args.requests)


if __name__ == '__main__':
    asyncio.run(main())
