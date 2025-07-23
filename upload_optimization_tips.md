# Open-WebUI Upload Optimization Guide

## Performance Improvements Implemented

### 1. Parallel Upload Script (`open-webui-parallel.py`)
- Uses ThreadPoolExecutor for concurrent uploads
- Configurable number of workers (default: 4)
- Thread-safe statistics tracking
- Progress reporting during upload

### 2. Key Optimizations

#### Concurrent Processing
- Multiple pages uploaded simultaneously
- Reduces waiting time for HTTP requests
- Optimal for I/O-bound operations

#### Usage Examples
```bash
# Default 4 workers
python open-webui-parallel.py

# Use 8 workers for faster processing
python open-webui-parallel.py --workers 8

# Upload text only with 10 workers
python open-webui-parallel.py --format txt --workers 10
```

## Additional Optimization Strategies

### 1. Batch Upload API (if available)
If Open-WebUI supports batch uploads, modify the client to:
```python
def upload_batch(self, documents: List[Dict], collection_id: str) -> bool:
    """Upload multiple documents in a single request"""
    # Implementation depends on API support
```

### 2. Connection Pooling
The current implementation uses requests.Session() which provides connection pooling by default.

### 3. Content Compression
Add gzip compression for large documents:
```python
import gzip
import io

def compress_content(content: str) -> bytes:
    """Compress content using gzip"""
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode='w') as f:
        f.write(content.encode('utf-8'))
    return out.getvalue()
```

### 4. Async Implementation
For even better performance, consider using aiohttp:
```python
import aiohttp
import asyncio

async def upload_async(session, title, content, collection_id):
    # Async upload implementation
    pass
```

### 5. Database-Level Optimizations
- Pre-process and store cleaned text in pickles
- Cache processed content to avoid re-processing
- Use smaller pickle files for better memory usage

## Performance Tuning

### Optimal Worker Count
- **CPU-bound tasks**: workers = CPU cores
- **I/O-bound tasks**: workers = 2-4x CPU cores
- **Network-bound**: Start with 4-8, increase gradually
- Monitor server response times

### Memory Considerations
- Each worker loads pages into memory
- Monitor memory usage with large spaces
- Consider chunking large spaces

### Network Optimization
- Ensure good network connectivity to Open-WebUI
- Consider running on same network/machine as Open-WebUI
- Use local Open-WebUI instance for best performance

## Benchmarking

Run the benchmark script to find optimal settings:
```bash
python benchmark_upload.py
```

This will test:
- Sequential upload (baseline)
- Parallel with 2, 4, 8 workers
- Show speedup comparisons

## Monitoring

The parallel script provides:
- Real-time progress updates
- Pages per second metrics
- Success/failure counts
- Thread-safe logging

## Best Practices

1. **Start Conservative**: Begin with 4 workers, increase gradually
2. **Monitor Server**: Watch Open-WebUI server load
3. **Check Errors**: Higher concurrency may cause more failures
4. **Network Limits**: Some networks limit concurrent connections
5. **Batch Operations**: Group small documents when possible

## Troubleshooting

### High Failure Rate
- Reduce worker count
- Check server logs
- Verify API rate limits

### Memory Issues
- Process smaller spaces
- Reduce worker count
- Increase system RAM

### Slow Performance
- Increase workers (if server allows)
- Check network latency
- Verify server performance

## Future Enhancements

1. **Smart Chunking**: Automatically split large spaces
2. **Retry Logic**: Automatic retry for failed uploads
3. **Resume Enhancement**: Page-level resume (not just space-level)
4. **Compression**: Automatic content compression
5. **Caching**: Skip already-uploaded content
6. **Metrics Dashboard**: Real-time performance monitoring