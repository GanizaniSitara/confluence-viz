#!/usr/bin/env python3
"""
CUDA diagnostic script to check PyTorch CUDA availability and configuration.
"""

import sys
import os
import subprocess

print("=== CUDA Diagnostic Report ===\n")

# Check Python and system info
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")
print(f"Platform: {sys.platform}")
print()

# Check environment variables
print("CUDA-related environment variables:")
cuda_vars = ['CUDA_HOME', 'CUDA_PATH', 'CUDA_VISIBLE_DEVICES', 'LD_LIBRARY_PATH', 'PATH']
for var in cuda_vars:
    value = os.environ.get(var, 'Not set')
    print(f"  {var}: {value}")
print()

# Try to check nvidia-smi
print("NVIDIA-SMI output:")
try:
    result = subprocess.run(['nvidia-smi'], capture_output=True, text=True)
    if result.returncode == 0:
        print(result.stdout)
    else:
        print(f"  Error running nvidia-smi: {result.stderr}")
except FileNotFoundError:
    print("  nvidia-smi not found in PATH")
except Exception as e:
    print(f"  Error: {e}")
print()

# Check PyTorch
try:
    import torch
    print("PyTorch information:")
    print(f"  PyTorch version: {torch.__version__}")
    print(f"  CUDA available: {torch.cuda.is_available()}")
    print(f"  CUDA version: {torch.version.cuda if hasattr(torch.version, 'cuda') else 'N/A'}")
    print(f"  cuDNN version: {torch.backends.cudnn.version() if torch.cuda.is_available() else 'N/A'}")
    print(f"  Number of GPUs: {torch.cuda.device_count()}")
    
    if torch.cuda.is_available():
        print("\nDetected GPUs:")
        for i in range(torch.cuda.device_count()):
            props = torch.cuda.get_device_properties(i)
            print(f"  GPU {i}: {props.name}")
            print(f"    Memory: {props.total_memory / 1024**3:.2f} GB")
            print(f"    Compute Capability: {props.major}.{props.minor}")
    else:
        print("\nCUDA not available. Possible reasons:")
        print("  1. PyTorch installed without CUDA support (CPU-only version)")
        print("  2. CUDA driver not installed or outdated")
        print("  3. GPU not properly detected by the system")
        print("  4. Environment variables not set correctly")
        
        # Check if this might be a CPU-only PyTorch
        if 'cpu' in torch.__version__ or not hasattr(torch.version, 'cuda'):
            print("\n  ** This appears to be a CPU-only PyTorch installation **")
            print("  To use CUDA, reinstall PyTorch with CUDA support:")
            print("  Visit https://pytorch.org/get-started/locally/ for the correct command")
            
except ImportError:
    print("PyTorch is not installed!")
except Exception as e:
    print(f"Error checking PyTorch: {e}")

print("\n" + "="*40)