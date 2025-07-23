#!/usr/bin/env python3
"""
Benchmark script to compare upload performance
"""
import time
import subprocess
import sys
import os
from pathlib import Path

def run_benchmark(script: str, workers: int = None, spaces_limit: int = 2):
    """Run upload script and measure performance"""
    print(f"\n{'='*60}")
    print(f"Running: {script}" + (f" with {workers} workers" if workers else ""))
    print(f"{'='*60}")
    
    # Clear checkpoint to ensure fair comparison
    if os.path.exists('openwebui_checkpoint.txt'):
        os.remove('openwebui_checkpoint.txt')
    
    cmd = [sys.executable, script, '--test-mode']  # Test mode automatically uses txt format
    if workers:
        cmd.extend(['--workers', str(workers)])
    
    print(f"Running command: {' '.join(cmd)}")
    start_time = time.time()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)  # 5 min timeout
        elapsed = time.time() - start_time
        
        print(f"Exit code: {result.returncode}")
        print(f"Total time: {elapsed:.2f} seconds")
        
        # Show stderr if there are errors
        if result.stderr:
            print(f"STDERR: {result.stderr[:500]}...")
        
        # Show last part of stdout for debugging
        if not result.stdout:
            print("No stdout output received!")
        else:
            print(f"Output preview (last 500 chars): ...{result.stdout[-500:]}")
        
        # Extract statistics from output
        output_lines = result.stdout.split('\n')
        for line in output_lines[-20:]:  # Check last 20 lines for stats
            if 'pages/second' in line.lower() or 'total pages uploaded' in line.lower():
                print(line.strip())
        
        return elapsed, result.returncode == 0
        
    except Exception as e:
        print(f"Error: {e}")
        return None, False

def main():
    """Run benchmarks"""
    print("Upload Performance Benchmark")
    print("=" * 60)
    
    # Check if scripts exist
    if not Path('open-webui.py').exists():
        print("Error: open-webui.py not found")
        return 1
    
    if not Path('open-webui-parallel.py').exists():
        print("Error: open-webui-parallel.py not found")
        return 1
    
    # Make parallel script executable
    os.chmod('open-webui-parallel.py', 0o755)
    
    print("\nThis benchmark will use test mode - creates temporary collections")
    print("Each test run creates its own collection and cleans up afterward")
    print("Make sure your Open-WebUI instance is running and configured in settings.ini")
    
    input("\nPress Enter to start benchmark...")
    
    results = []
    
    # Test original sequential version
    elapsed, success = run_benchmark('open-webui.py')
    if success:
        results.append(('Sequential (original)', elapsed))
    
    # Test parallel versions with different worker counts
    for workers in [2, 4, 8]:
        elapsed, success = run_benchmark('open-webui-parallel.py', workers)
        if success:
            results.append((f'Parallel ({workers} workers)', elapsed))
    
    # Summary
    print(f"\n{'='*60}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*60}")
    print(f"{'Method':<25} {'Time (seconds)':<15} {'Speedup':<10}")
    print("-" * 50)
    
    if results:
        baseline = results[0][1]  # Sequential time
        for method, elapsed in results:
            speedup = baseline / elapsed if elapsed > 0 else 0
            print(f"{method:<25} {elapsed:<15.2f} {speedup:<10.2f}x")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())