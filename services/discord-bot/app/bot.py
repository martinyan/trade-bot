import os

import discord
import httpx
from discord import app_commands
from formatters import fmt_compact, fmt_price, fmt_change, fmt_range

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
DISCORD_GUILD_ID = os.getenv("DISCORD_GUILD_ID", "")
STRATEGY_ENGINE_URL = os.getenv("STRATEGY_ENGINE_URL", "http://strategy-engine:8002").rstrip("/")

GUILD_OBJECT = discord.Object(id=int(DISCORD_GUILD_ID)) if DISCORD_GUILD_ID else None


class TradeBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self) -> None:
        try:
            if GUILD_OBJECT:
                synced = await self.tree.sync(guild=GUILD_OBJECT)
                print(f"Synced {len(synced)} guild command(s) to {DISCORD_GUILD_ID}")
            else:
                synced = await self.tree.sync()
                print(f"Synced {len(synced)} global command(s)")

            for cmd in synced:
                print(f"- {cmd.name}")

        except Exception as e:
            print(f"Command sync failed: {e}")

    async def on_ready(self) -> None:
        print(f"Logged in as {self.user} (ID: {self.user.id})")


bot = TradeBot()


@bot.tree.command(name="ping", description="Health check command", guild=GUILD_OBJECT)
async def ping(interaction: discord.Interaction) -> None:
    await interaction.response.send_message("pong")


@bot.tree.command(name="brief", description="Get a brief quote summary for a symbol", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. AAPL")
async def brief(interaction: discord.Interaction, symbol: str) -> None:
    await interaction.response.defer(thinking=True)

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(f"{STRATEGY_ENGINE_URL}/v1/brief", params={"symbol": symbol})

    if resp.status_code >= 400:
        await interaction.followup.send(f"brief failed: {resp.text}")
        return

    data = resp.json()
    message = (
        f"**{data['symbol']}**\n"
        f"Price: {fmt_price(data.get('price'))}\n"
        f"Change: {fmt_change(data.get('change'), data.get('changesPercentage'))}\n"
        f"Range: {fmt_range(data.get('dayLow'), data.get('dayHigh'), price=True)}\n"
        f"Volume: {fmt_compact(data.get('volume'))}"
    )
    await interaction.followup.send(message)

@bot.tree.command(name="quote_detail", description="Get a detailed quote snapshot", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. NVDA")
async def quote_detail(interaction: discord.Interaction, symbol: str) -> None:
    await interaction.response.defer(thinking=True)

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(
            f"{STRATEGY_ENGINE_URL}/v1/quote-detail",
            params={"symbol": symbol},
        )

    if resp.status_code >= 400:
        await interaction.followup.send(f"quote_detail failed: {resp.text}")
        return

    data = resp.json()

    lines = [
        f"**{data.get('symbol', symbol.upper())}**",
        f"Company: {data.get('companyName', 'n/a')}",
        f"Exchange: {data.get('exchangeShortName', 'n/a')}",
        "",
        f"Price: {fmt_price(data.get('price'))}",
        f"Change: {fmt_change(data.get('change'), data.get('changesPercentage'))}",
        "",
        f"Open: {fmt_price(data.get('open'))}",
        f"Previous Close: {fmt_price(data.get('previousClose'))}",
        "",
        f"Day Range: {fmt_range(data.get('dayLow'), data.get('dayHigh'), price=True)}",
        f"52W Range: {fmt_range(data.get('yearLow'), data.get('yearHigh'), price=True)}",
        "",
        f"Volume: {fmt_compact(data.get('volume'))}",
        f"Avg Volume: {fmt_compact(data.get('avgVolume'))}",
        f"Market Cap: {fmt_compact(data.get('marketCap'))}",
        "",
    ]

    await interaction.followup.send("\n".join(lines))

@bot.tree.command(name="scan_premarket", description="Scan premarket movers", guild=GUILD_OBJECT)
@app_commands.describe(limit="Max number of symbols to return (1-20)")
async def scan_premarket(
    interaction: discord.Interaction,
    limit: app_commands.Range[int, 1, 20] = 10,
) -> None:
    await interaction.response.defer(thinking=True)

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(f"{STRATEGY_ENGINE_URL}/v1/scan/premarket", params={"limit": limit})

    if resp.status_code >= 400:
        await interaction.followup.send(f"scan_premarket failed: {resp.text}")
        return

    rows = []
    for item in resp.json().get("data", []):
        symbol = item.get("symbol", "?")
        price = item.get("price", "?")
        change = item.get("changePercentage", item.get("changesPercentage", item.get("change", "?")))
        rows.append(f"{symbol}: {price} ({change})")

    if not rows:
        await interaction.followup.send("No movers returned.")
        return

    await interaction.followup.send("\n".join(rows[:limit]))


@bot.tree.command(name="watch_add", description="Add a symbol to your watchlist", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. TSLA")
async def watch_add(interaction: discord.Interaction, symbol: str) -> None:
    await interaction.response.defer(thinking=True)
    payload = {"user_id": str(interaction.user.id), "symbol": symbol}

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(f"{STRATEGY_ENGINE_URL}/v1/watchlist/add", json=payload)

    if resp.status_code >= 400:
        await interaction.followup.send(f"watch_add failed: {resp.text}")
        return

    watchlist = resp.json().get("watchlist", [])
    await interaction.followup.send(f"Watchlist updated: {', '.join(watchlist)}")


def main() -> None:
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN is required")
    bot.run(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()