#!/usr/bin/env python3
"""
This example streams all trades from CTF Exchange contracts.

Use default RPCs if not configurated.

Requirements:
    pip install ctf-stream
"""

import asyncio
from ctf_stream import ActivityClient


async def main():    
    async with ActivityClient() as client:
        print("Streaming all trades... (Ctrl+C to stop)\n")
        
        trade_count = 0
        async for trade in client.stream_trades():
            trade_count += 1
            
            # Format trade info
            side_emoji = "🟢" if trade.side == "BUY" else "🔴"
            outcome = trade.outcome or "Unknown"
            market = trade.market_title or "Unknown Market"
            
            # Truncate long market titles
            if len(market) > 60:
                market = market[:57] + "..."
            
            print(
                f"{side_emoji} {trade.side:4} ${trade.size_usdc:>10,.2f} | "
                f"{outcome:15} @ {trade.price:>6.2%} | "
                f"{market}"
            )
            
            # Show stats every 100 trades
            if trade_count % 100 == 0:
                stats = client.get_stats()
                ws = stats.websocket
                print(f"\nSTATS: {trade_count} trades | Reconnects: {ws.reconnect_count} times\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nStopped.")
