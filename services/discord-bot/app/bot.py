import os
import asyncio
from datetime import datetime, time
from zoneinfo import ZoneInfo

import discord
import httpx
from discord import app_commands
from formatters import fmt_compact, fmt_price, fmt_change, fmt_range, fmt_percent

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
DISCORD_GUILD_ID = os.getenv("DISCORD_GUILD_ID", "")
STRATEGY_ENGINE_URL = os.getenv("STRATEGY_ENGINE_URL", "http://strategy-engine:8002").rstrip("/")

GUILD_OBJECT = discord.Object(id=int(DISCORD_GUILD_ID)) if DISCORD_GUILD_ID else None
DISCORD_PRIVATE_BY_DEFAULT = os.getenv("DISCORD_PRIVATE_BY_DEFAULT", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
WATCHLIST_EOD_SUMMARY_ENABLED = os.getenv("WATCHLIST_EOD_SUMMARY_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
WATCHLIST_EOD_HOUR_ET = int(os.getenv("WATCHLIST_EOD_HOUR_ET", "16"))
WATCHLIST_EOD_MINUTE_ET = int(os.getenv("WATCHLIST_EOD_MINUTE_ET", "10"))
WATCHLIST_EOD_WINDOW_MINUTES = int(os.getenv("WATCHLIST_EOD_WINDOW_MINUTES", "20"))
EASTERN_TZ = ZoneInfo("America/New_York")


class ShareToChannelView(discord.ui.View):
    def __init__(self, owner_id: int, public_message: str) -> None:
        super().__init__(timeout=900)
        self.owner_id = owner_id
        self.public_message = public_message[:1800]

    @discord.ui.button(label="Share To Channel", style=discord.ButtonStyle.primary)
    async def share_to_channel(
        self,
        interaction: discord.Interaction,
        _: discord.ui.Button,
    ) -> None:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the user who requested this result can share it.",
                ephemeral=True,
            )
            return

        if interaction.channel is None:
            await interaction.response.send_message(
                "This result cannot be shared in the current context.",
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(
            ShareCommentModal(
                owner_id=self.owner_id,
                public_message=self.public_message,
            )
        )


class ShareCommentModal(discord.ui.Modal, title="Share Result"):
    comment = discord.ui.TextInput(
        label="Add a comment (optional)",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=500,
        placeholder="Why are you sharing this?",
    )

    def __init__(self, owner_id: int, public_message: str) -> None:
        super().__init__()
        self.owner_id = owner_id
        self.public_message = public_message

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the user who requested this result can share it.",
                ephemeral=True,
            )
            return

        if interaction.channel is None:
            await interaction.response.send_message(
                "This result cannot be shared in the current context.",
                ephemeral=True,
            )
            return

        comment_text = str(self.comment).strip()
        content = self.public_message
        if comment_text:
            content = f"{self.public_message}\n\n**Comment:** {comment_text}"

        await interaction.channel.send(content[:1900])
        await interaction.response.send_message("Shared to channel.", ephemeral=True)


def _share_view(interaction: discord.Interaction, title: str, body: str) -> ShareToChannelView:
    public_message = (
        f"**{title}**\n"
        f"Shared by <@{interaction.user.id}>\n"
        f"{body}"
    )
    return ShareToChannelView(owner_id=interaction.user.id, public_message=public_message)


async def _defer(interaction: discord.Interaction) -> None:
    await interaction.response.defer(thinking=True, ephemeral=DISCORD_PRIVATE_BY_DEFAULT)


async def _send(
    interaction: discord.Interaction,
    content: str | None = None,
    *,
    embed: discord.Embed | None = None,
    view: discord.ui.View | None = None,
    ephemeral: bool | None = None,
) -> None:
    kwargs: dict[str, object] = {
        "content": content,
        "embed": embed,
        "ephemeral": DISCORD_PRIVATE_BY_DEFAULT if ephemeral is None else ephemeral,
    }
    if view is not None:
        kwargs["view"] = view
    await interaction.followup.send(**kwargs)


class TradeBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.watchlist_summary_task: asyncio.Task | None = None
        self.watchlist_summary_last_date: str | None = None

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
        if WATCHLIST_EOD_SUMMARY_ENABLED and self.watchlist_summary_task is None:
            self.watchlist_summary_task = asyncio.create_task(self._watchlist_summary_loop())

    async def _watchlist_summary_loop(self) -> None:
        while True:
            try:
                now_et = datetime.now(EASTERN_TZ)
                if now_et.weekday() < 5:
                    target = time(hour=WATCHLIST_EOD_HOUR_ET, minute=WATCHLIST_EOD_MINUTE_ET)
                    target_dt = now_et.replace(
                        hour=target.hour,
                        minute=target.minute,
                        second=0,
                        microsecond=0,
                    )
                    minutes_since = (now_et - target_dt).total_seconds() / 60.0
                    date_key = now_et.date().isoformat()
                    in_window = 0 <= minutes_since <= WATCHLIST_EOD_WINDOW_MINUTES

                    if in_window and self.watchlist_summary_last_date != date_key:
                        await self._send_watchlist_summaries()
                        self.watchlist_summary_last_date = date_key
            except Exception as e:
                print(f"watchlist summary loop error: {e}")

            await asyncio.sleep(300)

    async def _send_watchlist_summaries(self) -> None:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{STRATEGY_ENGINE_URL}/v1/watchlist/all")
            resp.raise_for_status()
            payload = resp.json()
            users = payload.get("data", {}) if isinstance(payload, dict) else {}

            if not isinstance(users, dict) or not users:
                return

            for user_id, symbols in users.items():
                if not isinstance(symbols, list) or not symbols:
                    continue
                try:
                    uid = int(user_id)
                except ValueError:
                    continue

                lines = ["**End of Day Watchlist Summary**", ""]
                for symbol in symbols[:5]:
                    try:
                        brief_resp = await client.get(
                            f"{STRATEGY_ENGINE_URL}/v1/brief",
                            params={"symbol": symbol},
                        )
                        if brief_resp.status_code >= 400:
                            lines.append(f"{symbol}: unavailable")
                            continue
                        item = brief_resp.json()
                        lines.append(
                            f"{symbol} | Price: {fmt_price(item.get('price'))} | "
                            f"Change: {fmt_change(item.get('change'), item.get('changePercentage'))} | "
                            f"Range: {fmt_range(item.get('dayLow'), item.get('dayHigh'), price=True)} | "
                            f"Volume: {fmt_compact(item.get('volume'))}"
                        )
                    except Exception:
                        lines.append(f"{symbol}: unavailable")

                message = "\n".join(lines)[:1900]
                try:
                    user = await self.fetch_user(uid)
                    await user.send(message)
                except Exception as e:
                    print(f"Failed to DM watchlist summary to {uid}: {e}")


bot = TradeBot()


@bot.tree.command(name="ping", description="Health check command", guild=GUILD_OBJECT)
async def ping(interaction: discord.Interaction) -> None:
    await interaction.response.send_message("pong", ephemeral=DISCORD_PRIVATE_BY_DEFAULT)


@bot.tree.command(name="quote", description="Get a brief quote summary for a symbol", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. AAPL")
async def brief(interaction: discord.Interaction, symbol: str) -> None:
    await _defer(interaction)

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(f"{STRATEGY_ENGINE_URL}/v1/brief", params={"symbol": symbol})

    if resp.status_code >= 400:
        await _send(interaction, f"brief failed: {resp.text}")
        return

    data = resp.json()
    message = (
        f"**{data['symbol']}**\n"
        f"Price: {fmt_price(data.get('price'))} | "
        f"Change: {fmt_change(data.get('change'), data.get('changePercentage'))}\n"
        f"Range: {fmt_range(data.get('dayLow'), data.get('dayHigh'), price=True)} | "
        f"Volume: {fmt_compact(data.get('volume'))}"
    )
    await _send(
        interaction,
        message,
        view=_share_view(interaction, f"{data['symbol']} Brief", message),
    )

@bot.tree.command(name="quote_detail", description="Get a detailed quote snapshot", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. NVDA")
async def quote_detail(interaction: discord.Interaction, symbol: str) -> None:
    await _defer(interaction)

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(
            f"{STRATEGY_ENGINE_URL}/v1/quote-detail",
            params={"symbol": symbol},
        )

    if resp.status_code >= 400:
        await _send(interaction, f"quote_detail failed: {resp.text}")
        return

    data = resp.json()

    lines = [
        f"**{data.get('symbol', symbol.upper())}**",
        f"Company: {data.get('companyName', 'n/a')} | "
        f"Exchange: {data.get('exchangeShortName', 'n/a')}",
        f"Price: {fmt_price(data.get('price'))} | "
        f"Change: {fmt_change(data.get('change'), data.get('changesPercentage'))}",
        f"Open: {fmt_price(data.get('open'))} | "
        f"Previous Close: {fmt_price(data.get('previousClose'))}",
        f"Day Range: {fmt_range(data.get('dayLow'), data.get('dayHigh'), price=True)} | "
        f"52W Range: {fmt_range(data.get('yearLow'), data.get('yearHigh'), price=True)}",
        f"Volume: {fmt_compact(data.get('volume'))} | "
        f"Avg Volume: {fmt_compact(data.get('avgVolume'))}",
        f"Market Cap: {fmt_compact(data.get('marketCap'))}",
    ]

    message = "\n".join(lines)
    await _send(
        interaction,
        message,
        view=_share_view(interaction, f"{data.get('symbol', symbol.upper())} Quote Detail", message),
    )

@bot.tree.command(name="scan_premarket", description="Scan premarket movers", guild=GUILD_OBJECT)
@app_commands.describe(limit="Max number of symbols to return (1-20)")
async def scan_premarket(
    interaction: discord.Interaction,
    limit: app_commands.Range[int, 1, 20] = 10,
) -> None:
    await _defer(interaction)

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(f"{STRATEGY_ENGINE_URL}/v1/scan/premarket", params={"limit": limit})

    if resp.status_code >= 400:
        await _send(interaction, f"scan_premarket failed: {resp.text}")
        return

    rows = []
    for item in resp.json().get("data", []):
        symbol = item.get("symbol", "?")
        price = item.get("price", "?")
        change = item.get("changePercentage", item.get("changesPercentage", item.get("change", "?")))
        rows.append(f"{symbol}: {price} ({change})")

    if not rows:
        await _send(interaction, "No movers returned.")
        return

    message = "\n".join(rows[:limit])
    await _send(
        interaction,
        message,
        view=_share_view(interaction, "Premarket Movers", message),
    )


@bot.tree.command(name="watch_add", description="Add a symbol to your watchlist", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. TSLA")
async def watch_add(interaction: discord.Interaction, symbol: str) -> None:
    await _defer(interaction)
    payload = {"user_id": str(interaction.user.id), "symbol": symbol}

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(f"{STRATEGY_ENGINE_URL}/v1/watchlist/add", json=payload)

    if resp.status_code >= 400:
        detail = ""
        try:
            err = resp.json()
            detail = err.get("detail", "") if isinstance(err, dict) else str(err)
        except Exception:
            detail = resp.text
        if resp.status_code == 400 and "max 5" in detail.lower():
            await _send(interaction, "Watchlist limit reached. You can track up to 5 symbols.")
        else:
            await _send(interaction, f"watch_add failed: {detail or resp.text}")
        return

    watchlist = resp.json().get("watchlist", [])
    await _send(interaction, f"Watchlist updated: {', '.join(watchlist)}")


@bot.tree.command(name="watch_remove", description="Remove a symbol from your watchlist", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. TSLA")
async def watch_remove(interaction: discord.Interaction, symbol: str) -> None:
    await _defer(interaction)
    payload = {"user_id": str(interaction.user.id), "symbol": symbol}

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(f"{STRATEGY_ENGINE_URL}/v1/watchlist/remove", json=payload)

    if resp.status_code >= 400:
        await _send(interaction, f"watch_remove failed: {resp.text}")
        return

    data = resp.json()
    removed = bool(data.get("removed"))
    watchlist = data.get("watchlist", [])
    if removed:
        await _send(interaction, f"Removed {symbol.upper()}. Watchlist: {', '.join(watchlist) or 'empty'}")
    else:
        await _send(interaction, f"{symbol.upper()} was not in your watchlist. Current: {', '.join(watchlist) or 'empty'}")


@bot.tree.command(name="watch_list", description="Show your watchlist", guild=GUILD_OBJECT)
async def watch_list(interaction: discord.Interaction) -> None:
    await _defer(interaction)

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(
            f"{STRATEGY_ENGINE_URL}/v1/watchlist",
            params={"user_id": str(interaction.user.id)},
        )

    if resp.status_code >= 400:
        await _send(interaction, f"watch_list failed: {resp.text}")
        return

    watchlist = resp.json().get("watchlist", [])
    await _send(interaction, f"Your watchlist ({len(watchlist)}/5): {', '.join(watchlist) or 'empty'}")

@bot.tree.command(name="news", description="Get recent stock news for a symbol", guild=GUILD_OBJECT)
@app_commands.describe(
    symbol="Ticker symbol, e.g. NVDA",
    limit="Number of articles to return (1-10)",
)
async def news_command(
    interaction: discord.Interaction,
    symbol: str,
    limit: app_commands.Range[int, 1, 10] = 5,
) -> None:
    await _defer(interaction)

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(
                f"{STRATEGY_ENGINE_URL}/v1/news",
                params={"symbol": symbol.upper(), "limit": limit},
            )
            resp.raise_for_status()

        payload = resp.json()
        items = payload.get("data", [])

        if not items:
            await _send(interaction, f"No recent news found for {symbol.upper()}.")
            return

        embed = discord.Embed(
            title=f"{symbol.upper()} news",
            description=f"Top {min(limit, len(items))} recent articles",
        )

        for idx, item in enumerate(items[:limit], start=1):
            title = item.get("title") or "Untitled"
            site = item.get("site") or "Unknown source"
            published = item.get("published_date") or "Unknown date"
            url = item.get("url") or ""

            value = f"**Source:** {site}\n**Published:** {published}"
            if url:
                value += f"\n[Open article]({url})"

            field_name = f"{idx}. {title}"
            if len(field_name) > 250:
                field_name = field_name[:247] + "..."

            if len(value) > 1000:
                value = value[:997] + "..."

            embed.add_field(
                name=field_name,
                value=value,
                inline=False,
            )

        summary = f"Top {min(limit, len(items))} news items for {symbol.upper()}."
        await _send(
            interaction,
            embed=embed,
            view=_share_view(interaction, f"{symbol.upper()} News", summary),
        )

    except httpx.HTTPStatusError as e:
        detail = e.response.text[:300] if e.response is not None else str(e)
        await _send(
            interaction,
            f"Failed to fetch news for {symbol.upper()}: upstream returned an error.\n{detail}"
        )
    except httpx.RequestError as e:
        await _send(
            interaction,
            f"Failed to fetch news for {symbol.upper()}: could not reach strategy-engine. {e}"
        )
    except Exception as e:
        await _send(interaction, f"Failed to fetch news for {symbol.upper()}: {e}")


@bot.tree.command(name="insider_trades", description="Get the latest insider trade for a symbol", guild=GUILD_OBJECT)
@app_commands.describe(
    symbol="Ticker symbol, e.g. AAPL",
    limit="Number of recent records to return (1-20)",
)
async def insider_trades(
    interaction: discord.Interaction,
    symbol: str,
    limit: app_commands.Range[int, 1, 20] = 10,
) -> None:
    await _defer(interaction)

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(
                f"{STRATEGY_ENGINE_URL}/v1/insider-trades/latest",
                params={
                    "symbol": symbol.upper(),
                    "limit": limit,
                    "days": 60,
                },
            )
            resp.raise_for_status()

        payload = resp.json()
        trades = payload.get("data", [])
        symbol_value = payload.get("symbol", symbol.upper())
        fmp_stats = payload.get("fmp_statistics")

        def pick(*keys: str):
            if not isinstance(fmp_stats, dict):
                return None
            for key in keys:
                if key in fmp_stats and fmp_stats.get(key) is not None:
                    return fmp_stats.get(key)
            return None

        year = pick("year")
        quarter = pick("quarter")
        acquired_tx = pick("acquiredTransactions", "totalPurchases")
        disposed_tx = pick("disposedTransactions", "totalSales")
        acquired_ratio = pick("acquiredDisposedRatio")
        total_acquired = pick("totalAcquired", "totalBuyValue")
        total_disposed = pick("totalDisposed", "totalSellValue")
        avg_acquired = pick("averageAcquired", "averageBuyValue")
        avg_disposed = pick("averageDisposed", "averageSellValue")
        total_shares_traded = pick("totalSharesTraded")
        avg_value = pick("averageValue")

        if not isinstance(trades, list) or not trades:
            await _send(interaction, f"No insider trades found for {symbol.upper()}.")
            return

        window_days = payload.get("window_days", 60)
        total_recent = payload.get("total_recent", len(trades))

        header_lines = [f"**{symbol_value} Insider Trading Statistics + Recent Trades**"]
        if year is not None and quarter is not None:
            header_lines.append(f"Period: {year} Q{quarter}")
        elif year is not None:
            header_lines.append(f"Year: {year}")

        header_lines.append("")
        header_lines.append("**Statistics**")
        header_lines.append(f"Acquired: {fmt_compact(acquired_tx)}")
        header_lines.append(f"Disposed: {fmt_compact(disposed_tx)}")
        if acquired_ratio is not None:
            header_lines.append(f"Acquired/Disposed Ratio: {acquired_ratio}")
        header_lines.append(f"Total Acquired: {fmt_compact(total_acquired)}")
        header_lines.append(f"Total Disposed: {fmt_compact(total_disposed)}")
        header_lines.append(f"Avg Acquired: {fmt_compact(avg_acquired)}")
        header_lines.append(f"Avg Disposed: {fmt_compact(avg_disposed)}")
        if total_shares_traded is not None:
            header_lines.append(f"Total Shares Traded: {fmt_compact(total_shares_traded)}")
        if avg_value is not None:
            header_lines.append(f"Average Value: {fmt_compact(avg_value)}")
        raw_link = pick("link", "url")
        if raw_link:
            header_lines.append(f"Source: {raw_link}")

        header_lines.append("")
        header_lines.append(f"**Transactions (last {window_days} days)**")
        header_lines.append(f"Showing {min(limit, len(trades))} of {total_recent} records")
        header = "\n".join(header_lines)

        entries: list[str] = []

        for idx, trade in enumerate(trades[:limit], start=1):
            reporter = trade.get("reporting_name") or "Unknown"
            tx_type = trade.get("type") or "n/a"
            transaction_date = trade.get("transaction_date") or "n/a"
            filing_date = trade.get("filing_date") or "n/a"
            shares = fmt_compact(trade.get("securities_transacted"))
            price = fmt_price(trade.get("price"))
            value = fmt_compact(trade.get("value"))
            filing_url = trade.get("filing_url")

            entry_lines = [
                f"{idx}. {transaction_date} | {tx_type} | {reporter} | "
                f"Shares: {shares} | Price: {price} | Value: {value}"
            ]
            entry_lines.append(f"   Filing Date: {filing_date}")
            if filing_url:
                entry_lines.append(f"   Filing: {filing_url}")
            entries.append("\n".join(entry_lines))

        chunks: list[str] = []
        current = header
        for entry in entries:
            candidate = f"{current}\n\n{entry}" if current else entry
            if len(candidate) > 1900:
                chunks.append(current)
                current = entry
            else:
                current = candidate
        if current:
            chunks.append(current)

        for chunk in chunks:
            await _send(interaction, chunk)

    except httpx.HTTPStatusError as e:
        status = e.response.status_code if e.response is not None else 0
        detail = ""
        if e.response is not None:
            try:
                payload = e.response.json()
                detail = payload.get("detail", "") if isinstance(payload, dict) else str(payload)
            except Exception:
                detail = (e.response.text or "").strip()

        if status == 404 or "no insider trades found" in detail.lower():
            await _send(
                interaction,
                f"I couldn't find insider trade records for `{symbol.upper()}` in the last 60 days.\n"
                "Please check the ticker and try again (for example: `AAPL`, `MSFT`, `NVDA`)."
            )
        elif status == 422:
            await _send(
                interaction,
                "That ticker format looks invalid. Please use a valid stock symbol (for example: `AAPL`)."
            )
        else:
            short_detail = detail[:220] if detail else "Unexpected upstream error."
            await _send(
                interaction,
                f"Couldn't fetch insider trades for `{symbol.upper()}` right now.\n{short_detail}"
            )
    except httpx.RequestError as e:
        await _send(
            interaction,
            f"Failed to fetch insider trades for {symbol.upper()}: could not reach strategy-engine. {e}"
        )
    except Exception as e:
        await _send(interaction, f"Failed to fetch insider trades for {symbol.upper()}: {e}")


@bot.tree.command(name="earnings_risk", description="Get earnings risk score and drivers for a symbol", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. AAPL")
async def earnings_risk(interaction: discord.Interaction, symbol: str) -> None:
    await _defer(interaction)

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(
                f"{STRATEGY_ENGINE_URL}/v1/earnings-risk",
                params={"symbol": symbol.upper()},
            )
            resp.raise_for_status()

        payload = resp.json()
        symbol_value = payload.get("symbol", symbol.upper())
        score = payload.get("score")
        label = payload.get("label", "Unknown")
        next_event = payload.get("next_earnings", {}) if isinstance(payload.get("next_earnings"), dict) else {}
        history = payload.get("history", {}) if isinstance(payload.get("history"), dict) else {}
        market = payload.get("market_context", {}) if isinstance(payload.get("market_context"), dict) else {}
        components = payload.get("components", {}) if isinstance(payload.get("components"), dict) else {}

        lines = [
            f"**{symbol_value} Earnings Risk: {score}/100 ({label})**",
            "",
            "**Next Earnings**",
            f"Date: {next_event.get('date', 'n/a')} ({next_event.get('days_to_event', 'n/a')} days)",
            f"EPS Est: {next_event.get('eps_estimated', 'n/a')}",
            f"Revenue Est: {fmt_compact(next_event.get('revenue_estimated'))}",
            "",
            "**Recent History (up to 4 quarters)**",
            f"Beats/Misses: {history.get('beat_count', 'n/a')}/{history.get('miss_count', 'n/a')}",
            f"Avg Abs EPS Surprise: {fmt_percent(history.get('avg_abs_eps_surprise_pct'))}",
            "",
            "**Market Context**",
            f"Price: {fmt_price(market.get('price'))}",
            f"Change %: {fmt_percent(market.get('change_percentage'))}",
            f"Day Range %: {fmt_percent(market.get('day_range_pct'))}",
            "",
            "**Risk Components**",
            f"Proximity: {components.get('proximity_0_35', 'n/a')}/35",
            f"Surprise Variability: {components.get('surprise_variability_0_25', 'n/a')}/25",
            f"Miss History: {components.get('miss_history_0_20', 'n/a')}/20",
            f"Intraday Volatility: {components.get('intraday_volatility_0_20', 'n/a')}/20",
            f"Momentum Shock: {components.get('momentum_shock_0_10', 'n/a')}/10",
        ]

        message = "\n".join(lines)
        await _send(
            interaction,
            message,
            view=_share_view(interaction, f"{symbol_value} Earnings Risk", message),
        )

    except httpx.HTTPStatusError as e:
        status = e.response.status_code if e.response is not None else 0
        detail = ""
        if e.response is not None:
            try:
                err_payload = e.response.json()
                detail = err_payload.get("detail", "") if isinstance(err_payload, dict) else str(err_payload)
            except Exception:
                detail = (e.response.text or "").strip()

        if status == 404:
            await _send(
                interaction,
                f"I couldn't find enough earnings data for `{symbol.upper()}` to calculate risk."
            )
        elif status == 422:
            await _send(
                interaction,
                "That ticker format looks invalid. Please use a valid stock symbol (for example: `AAPL`)."
            )
        else:
            short_detail = detail[:220] if detail else "Unexpected upstream error."
            await _send(
                interaction,
                f"Couldn't calculate earnings risk for `{symbol.upper()}` right now.\n{short_detail}"
            )
    except httpx.RequestError as e:
        await _send(
            interaction,
            f"Couldn't calculate earnings risk for `{symbol.upper()}`: could not reach strategy-engine. {e}"
        )
    except Exception as e:
        await _send(interaction, f"Couldn't calculate earnings risk for `{symbol.upper()}`: {e}")


@bot.tree.command(name="catalyst_brief", description="Get a combined catalyst brief (quote, earnings, insider, news)", guild=GUILD_OBJECT)
@app_commands.describe(
    symbol="Ticker symbol, e.g. AAPL",
    news_limit="Number of top news items (1-5)",
)
async def catalyst_brief(
    interaction: discord.Interaction,
    symbol: str,
    news_limit: app_commands.Range[int, 1, 5] = 3,
) -> None:
    await _defer(interaction)

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{STRATEGY_ENGINE_URL}/v1/catalyst-brief",
                params={"symbol": symbol.upper(), "news_limit": news_limit},
            )
            resp.raise_for_status()

        payload = resp.json()
        symbol_value = payload.get("symbol", symbol.upper())
        quote = payload.get("quote", {}) if isinstance(payload.get("quote"), dict) else {}
        risk = payload.get("earnings_risk", {}) if isinstance(payload.get("earnings_risk"), dict) else {}
        insider = payload.get("insider_summary", {}) if isinstance(payload.get("insider_summary"), dict) else {}
        news_items = payload.get("news", []) if isinstance(payload.get("news"), list) else []

        next_event = risk.get("next_earnings", {}) if isinstance(risk.get("next_earnings"), dict) else {}
        latest_trade = insider.get("latest_trade", {}) if isinstance(insider.get("latest_trade"), dict) else {}
        insider_stats = insider.get("statistics", {}) if isinstance(insider.get("statistics"), dict) else {}

        acquired_tx = insider_stats.get("acquiredTransactions", insider_stats.get("totalPurchases"))
        disposed_tx = insider_stats.get("disposedTransactions", insider_stats.get("totalSales"))
        acquired_ratio = insider_stats.get("acquiredDisposedRatio")
        ratio_text = str(acquired_ratio) if acquired_ratio is not None else "N/A"

        lines = [
            f"**{symbol_value} Catalyst Brief**",
            "",
            "**Price Snapshot**",
            f"Price: {fmt_price(quote.get('price'))}",
            f"Change: {fmt_change(quote.get('change'), quote.get('changePercentage'))}",
            f"Day Range: {fmt_range(quote.get('dayLow'), quote.get('dayHigh'), price=True)}",
            "",
            "**Earnings Risk**",
            f"Score: {risk.get('score', 'n/a')}/100 ({risk.get('label', 'n/a')})",
            f"Next Earnings: {next_event.get('date', 'n/a')} ({next_event.get('days_to_event', 'n/a')} days)",
            "",
            "**Insider Signal (last 60d)**",
            f"Acquired/Disposed: {fmt_compact(acquired_tx)}/{fmt_compact(disposed_tx)} ({ratio_text})",
        ]

        if latest_trade:
            lines.append(
                f"Latest: {latest_trade.get('transaction_date', 'n/a')} | "
                f"{latest_trade.get('type', 'n/a')} | "
                f"{latest_trade.get('reporting_name', 'n/a')}"
            )

        lines.append("")
        lines.append("**Top News**")
        if news_items:
            for idx, item in enumerate(news_items[:news_limit], start=1):
                title = item.get("title") or "Untitled"
                if len(title) > 120:
                    title = title[:117] + "..."
                site = item.get("site") or "Unknown"
                published = item.get("published_date") or "Unknown date"
                url = item.get("url")
                if url:
                    lines.append(f"{idx}. [{title}]({url}) ({site}, {published})")
                else:
                    lines.append(f"{idx}. {title} ({site}, {published})")
        else:
            lines.append("No recent news found.")

        message = "\n".join(lines)
        if len(message) > 1900:
            message = message[:1897] + "..."

        await _send(
            interaction,
            message,
            view=_share_view(interaction, f"{symbol_value} Catalyst Brief", message[:1000]),
        )

    except httpx.HTTPStatusError as e:
        detail = e.response.text[:260] if e.response is not None else str(e)
        await _send(
            interaction,
            f"Couldn't build catalyst brief for `{symbol.upper()}`.\n{detail}"
        )
    except httpx.RequestError as e:
        await _send(
            interaction,
            f"Couldn't build catalyst brief for `{symbol.upper()}`: could not reach strategy-engine. {e}"
        )
    except Exception as e:
        await _send(interaction, f"Couldn't build catalyst brief for `{symbol.upper()}`: {e}")


def main() -> None:
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN is required")
    bot.run(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()
