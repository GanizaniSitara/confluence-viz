# Hackathon Quick Start

Fast Confluence MCP server for 30+ concurrent users. No authentication, simple setup.

## Quick Setup (5 minutes)

### 1. Install Dependencies

```bash
cd confluence-fast-mcp
pip install -r requirements.txt
```

### 2. Configure

```bash
cp settings.example.ini settings.ini
# Edit if your pickles are not in ../temp/
```

### 3. Start Server

```bash
python3 simple_server.py --http 8070
```

Server starts on http://localhost:8070

### 4. Add to Claude Code

**Edit** `~/.claude/mcp_settings.json`:
```json
{
  "mcpServers": {
    "confluence": {
      "transport": "http",
      "url": "http://localhost:8070/sse"
    }
  }
}
```

The server runs independently. Restart Claude Code to connect.

### 5. Share with Team

Team members edit their `~/.claude/mcp_settings.json`:

```json
{
  "mcpServers": {
    "confluence": {
      "transport": "http",
      "url": "http://YOUR_IP:8070/sse"
    }
  }
}
```

Replace `YOUR_IP` with your machine's IP (use `hostname -I` on Linux).

Restart Claude Code to connect.

## Load Testing

Test with 30 concurrent users:

```bash
python3 load_test.py --users 30 --requests 10
```

Expected results:
- Success rate: >95%
- Average response: <2 seconds
- Throughput: 15+ requests/second

## Performance

- Handles 80K pages in memory
- 30+ concurrent users
- Sub-second response times
- No authentication (perfect for hackathons)

## Firewall Setup (if needed)

Allow port 8070:

```bash
# Linux
sudo ufw allow 8070

# macOS
# System Preferences > Security & Privacy > Firewall > Firewall Options
```

## Troubleshooting

**"Connection refused"**
- Check server is running: `curl http://localhost:8070/health`
- Check firewall allows port 8070

**"Slow responses"**
- Run load test to diagnose
- Check server logs for errors
- Ensure pickles are loaded (check startup logs)

**"Too many connections"**
- Default supports 100+ concurrent connections
- If needed, increase OS limits: `ulimit -n 4096`

## Architecture

```
Team Members (30+)
    |
    v
Claude Code (HTTP/SSE)
    |
    v
Simple MCP Server (Port 8070)
    |
    v
In-Memory Cache (80K pages)
    |
    v
Pickle Files (../temp/)
```

Fast, simple, no database needed.
