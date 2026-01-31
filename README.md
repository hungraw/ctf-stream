<h1 align="center">CTF Activity Stream</h1>

<p align="center">
  <a href="https://github.com/hungraw/ctf-stream/releases"><img src="https://img.shields.io/github/v/release/hungraw/ctf-stream?style=for-the-badge&color=blue" alt="Release"></a>
  <img src="https://img.shields.io/badge/python-3.10+-blue?style=for-the-badge" alt="Python 3.10+">
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge" alt="License: MIT"></a>
</p>

<p align="center"><b>Simple activity streaming SDK for prediction markets.</b></p>

## Features

- **Fast** - Trades arrive as blocks are mined
- **Simple** - No extra API
- **Reconnection** - Exponential backoff w/ jitter, multi-endpoint failover
- **Wallet filtering** - Track specific wallets
- **CLI included** - Simple command-line tool

## Installation

```bash
# Core (no Redis)
pip install ctf-stream

# With Redis cache support
pip install ctf-stream[redis]

# With CLI tools
pip install ctf-stream[cli]

# Everything
pip install ctf-stream[all]
```

## Quick Start

```python
import asyncio
from ctf_stream import ActivityClient

async def main():
    # Initialize client with RPC endpoints
    client = ActivityClient(
        rpc_endpoints=["wss://polygon-bor-rpc.publicnode.com"]
    )
    
    # Option 1: Track specific Safe address
    await client.track_wallet("0x1234567890123456789012345678901234567890")
    # Option 2: Stream all trades
    # client.set_track_all(True)
    
    # Stream trades
    async for trade in client.stream_trades():
        print(f"{trade.side} ${trade.size_usdc:.2f} | {trade.outcome} @ {trade.price:.2%}")
        print(f"  Market: {trade.market_title}")
        print(f"  Wallet: {trade.wallet}")

asyncio.run(main())
```

### With Context Manager

```python
async with ActivityClient(rpc_endpoints=["wss://..."]) as client:
    client.set_track_all(True)
    async for trade in client.stream_trades():
        process(trade)
```

## CLI Usage

```bash
# Stream all trades (default endpoints)
ctf-stream stream

# Use specific RPC endpoint
ctf-stream stream --rpc wss://polygon-bor-rpc.publicnode.com

# Track specific Safe address/wallet
ctf-stream stream --wallet 0x1234...

# Save to file (JSONL)
ctf-stream stream --output trades.jsonl

# Test connection
ctf-stream ping --rpc wss://polygon-bor-rpc.publicnode.com
```

### CLI Output

```
🟥 Running...
Press Ctrl+C to stop

12:34:56 BUY       $500.00 Yes             @ 45.0%  | Will it rain tomorrow?
12:34:58 SELL      $250.00 No              @ 55.0%  | Bitcoin above $100k by March?
```
## API Reference

### ActivityClient

Main client for streaming trades.

```python
from ctf_stream import ActivityClient, Config

# Basic initialization
client = ActivityClient(
    rpc_endpoints=["wss://polygon-bor-rpc.publicnode.com"],
    redis_url="redis://localhost:6379",  # Optional
)

# Or with full config
config = Config(
    rpc_endpoints=["wss://polygon-bor-rpc.publicnode.com"],
    ws_idle_timeout=60,
    token_cache_ttl=3600,
)
client = ActivityClient(config=config)

# Methods
await client.track_wallet("0x...")      # Track wallet
await client.untrack_wallet("0x...")    # Untrack wallet
await client.track_wallets(["0x...", "0x..."])  # Track multiple
client.set_track_all(True)              # Stream all trades

# Stream trades
async for trade in client.stream_trades(enrich=True):
    print(trade)

# Get statistics
stats = client.get_stats()
print(stats.websocket.connected)
print(stats.processor.events_processed)
```

### Trade Object

```python
class Trade:
    tx_hash: str
    block_number: int
    timestamp: int
    wallet: str
    token_id: str
    side: str              # "BUY" or "SELL"
    price: float           # 0.0 to 1.0
    size_usdc: float
    size_shares: float
    market_slug: str | None
    market_title: str | None
    outcome: str | None
```

### Config

```python
Config(
    rpc_endpoints=["wss://..."],        # WebSocket RPC endpoints
    redis_url="redis://localhost:6379", # Optional Redis for caching
    ws_idle_timeout=60,                  # Seconds before reconnect
    token_cache_ttl=3600,                # Memory cache TTL
)
```

## RPC Endpoints

The SDK works with any Polygon RPC endpoint that supports WebSocket. Some free options:

| Provider | Endpoint | Notes |
|----------|----------|-------|
| publicnode by Allnodes | `wss://polygon-bor-rpc.publicnode.com` | Free, reliable |
| dRPC | `wss://polygon.drpc.org` | Free tier available |
| Alchemy | `wss://polygon-mainnet.g.alchemy.com/v2/KEY` | Requires API key |
| Infura | `wss://polygon-mainnet.infura.io/ws/v3/KEY` | Requires API key |
| ValidationCloud | `wss://mainnet.polygon.validationcloud.io/v1/KEY` | Requires API key |

**Tip:** Add multiple endpoints for better reliability:

```python
client = ActivityClient(
    rpc_endpoints=[
        "wss://polygon-bor-rpc.publicnode.com",
        "wss://polygon.drpc.org",
    ]
)
```

## License

MIT License - see [LICENSE](LICENSE) for details.
