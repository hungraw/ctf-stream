"""CLI for ActivityStream."""

from __future__ import annotations
import asyncio
import json
import sys
from datetime import datetime
from typing import Annotated
from . import ActivityClient, Config
try:
    import typer
    from rich.console import Console
    from rich.live import Live
    from rich.text import Text
except ImportError:
    print("CLI dependencies not installed. Run: pip install ctf-stream[cli]")
    sys.exit(1)

app = typer.Typer(
    name="ctf-stream",
    help="Real-time CTF activity streaming for prediction markets.",
    no_args_is_help=True,
)

console = Console()

def format_trade_row(trade) -> list[str]:
    if trade.side == "BUY":
        side = "[green]BUY[/green]"
    else:
        side = "[red]SELL[/red]"

    price = f"{trade.price * 100:.0f}¢"

    amount = f"${trade.size_usdc:,.2f}"

    market = trade.market_title or "Resolving..."
    if len(market) > 40:
        market = market[:37] + "..."

    outcome = trade.outcome or "..."

    wallet = f"{trade.wallet[:8]}..."
    return [
        datetime.now().strftime("%H:%M:%S"),
        side,
        amount,
        outcome,
        price,
        market,
        wallet,
    ]


@app.command()
def stream(
    rpc: Annotated[
        list[str],
        typer.Option(
            "--rpc",
            "-r",
            help="WebSocket RPC endpoint(s). Can specify multiple.",
        ),
    ] = None,
    wallet: Annotated[
        list[str],
        typer.Option(
            "--wallet",
            "-w",
            help="Wallet address(es) to track. If not specified, streams all trades.",
        ),
    ] = None,
    output: Annotated[
        str,
        typer.Option(
            "--output",
            "-o",
            help="Output file path (JSONL format). If not specified, prints to console.",
        ),
    ] = None,
    no_enrich: Annotated[
        bool,
        typer.Option(
            "--no-enrich",
            help="Disable market metadata enrichment from API. Better latency.",
        ),
    ] = False,
    redis_url: Annotated[
        str,
        typer.Option(
            "--redis",
            help="Redis URL for cache (optional).",
        ),
    ] = None,
    debug: Annotated[
        bool,
        typer.Option(
            "--debug",
            "-d",
            help="Enable debug.",
        ),
    ] = False,
):
    """Stream trades in real-time."""
    if debug:
        import logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        )
    if not rpc:
        rpc = ["wss://polygon-bor-rpc.publicnode.com", "wss://polygon.drpc.org"]
        console.print(f"[yellow]No RPC specified, using defaults: {rpc}[/yellow]")
    asyncio.run(_stream_async(
        rpc_endpoints=rpc,
        wallets=wallet,
        output_path=output,
        enrich=not no_enrich,
        redis_url=redis_url,
    ))

async def _stream_async(
    rpc_endpoints: list[str],
    wallets: list[str] | None,
    output_path: str | None,
    enrich: bool,
    redis_url: str | None,
):
    config = Config(
        rpc_endpoints=rpc_endpoints,
        redis_url=redis_url,
    )
    
    client = ActivityClient(config=config)
    
    if wallets:
        for w in wallets:
            await client.track_wallet(w)
        console.print(f"[green]Tracking {len(wallets)} wallet(s)[/green]")
    else:
        client.set_track_all(True)
        console.print("[yellow]Streaming all trades (no wallet filter)[/yellow]")
    
    output_file = None
    if output_path:
        output_file = open(output_path, "a")
        console.print(f"[green]Writing to {output_path}[/green]")
    
    console.print("\n[bold blue]🟥 Running...[/bold blue]")
    console.print("Press Ctrl+C to stop\n")
    
    try:
        trade_count = 0
        async for trade in client.stream_trades(enrich=enrich):
            trade_count += 1            
            if output_file:
                output_file.write(json.dumps(trade.to_dict()) + "\n")
                output_file.flush()
            
            row = format_trade_row(trade)
            side_color = "green" if trade.side == "BUY" else "red"
            console.print(
                f"[dim]{row[0]}[/dim] "
                f"[{side_color}]{trade.side:4}[/{side_color}] "
                f"[bold]{row[2]:>12}[/bold] "
                f"[cyan]{row[3]:15}[/cyan] "
                f"@ {row[4]:>7} "
                f"| {row[5]}"
            )
            
    except KeyboardInterrupt:
        console.print(f"\n[yellow]Stopped. Received {trade_count} trades.[/yellow]")
    finally:
        await client.stop()
        if output_file:
            output_file.close()

@app.command()
def ping(
    rpc: Annotated[
        str,
        typer.Option(
            "--rpc",
            "-r",
            help="WebSocket RPC endpoint to test.",
        ),
    ] = "wss://polygon-bor-rpc.publicnode.com",
):
    """Test connection to an RPC endpoint."""
    asyncio.run(_ping_async(rpc))


async def _ping_async(rpc: str):
    console.print(f"Testing connection to [cyan]{rpc}[/cyan]...")
    config = Config(rpc_endpoints=[rpc])
    client = ActivityClient(config=config)
    client.set_track_all(True)
    try:
        async for trade in client.stream_trades(enrich=False):
            console.print(f"[green]Connection OK! Received trade:[/green]")
            console.print(f"  TX: {trade.tx_hash[:20]}...")
            console.print(f"  Side: {trade.side}")
            console.print(f"  Amount: ${trade.size_usdc:.2f}")
            break
    except Exception as e:
        console.print(f"[red]Connection failed: {e}[/red]")
        raise typer.Exit(1)
    finally:
        await client.stop()

@app.command()  
def version():
    """Show version info."""
    from . import __version__
    console.print(f"ctf-stream v{__version__}")

if __name__ == "__main__":
    app()
