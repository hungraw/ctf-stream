#!/usr/bin/env python3
"""
This example only streams trades from specified Safe wallet.

Use default RPCs if not configurated.

Requirements:
    pip install ctf-stream
"""

import asyncio
from ctf_stream import ActivityClient


# Use Safe Wallet addresses (not regular EOAs).
WALLETS_TO_TRACK = [
    "0x7c3db723f1d4d8cb9c550095203b686cb11e5c6b",  # Car
]


async def main():    
    async with ActivityClient() as client:
        for wallet in WALLETS_TO_TRACK:
            client.track_wallet(wallet)
            print(f"Tracking: {wallet[:10]}...{wallet[-6:]}")
        
        print(f"\nTracking {len(WALLETS_TO_TRACK)} wallets... (Ctrl+C to stop)\n")
        
        async for trade in client.stream_trades():
            side_emoji = "🟢" if trade.side == "BUY" else "🔴"
            outcome = trade.outcome or "Unknown"
            market = trade.market_title or "Unknown Market"
            
            # Show which wallet traded
            wallet_short = f"{trade.wallet[:6]}...{trade.wallet[-4:]}"
            
            print(f"{side_emoji} [{wallet_short}] {trade.side} ${trade.size_usdc:.2f}")
            print(f"   └─ {outcome} @ {trade.price:.2%} | {market[:50]}")
            print()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nStopped.")
