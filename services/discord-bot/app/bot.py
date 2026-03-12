import os
import asyncio
import re
from datetime import datetime, time
from zoneinfo import ZoneInfo

import discord
import httpx
from discord import app_commands
from formatters import fmt_compact, fmt_price, fmt_change, fmt_range, fmt_percent, fmt_signed_compact

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
WATCHLIST_ALERT_ENABLED = os.getenv("WATCHLIST_ALERT_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
try:
    WATCHLIST_ALERT_THRESHOLD_PCT = float(os.getenv("WATCHLIST_ALERT_THRESHOLD_PCT", "8"))
except ValueError:
    WATCHLIST_ALERT_THRESHOLD_PCT = 8.0
WATCHLIST_ALERT_POLL_SECONDS = int(os.getenv("WATCHLIST_ALERT_POLL_SECONDS", "900"))
WATCHLIST_ALERT_START_HOUR_ET = int(os.getenv("WATCHLIST_ALERT_START_HOUR_ET", "9"))
WATCHLIST_ALERT_START_MINUTE_ET = int(os.getenv("WATCHLIST_ALERT_START_MINUTE_ET", "30"))
WATCHLIST_ALERT_END_HOUR_ET = int(os.getenv("WATCHLIST_ALERT_END_HOUR_ET", "16"))
WATCHLIST_ALERT_END_MINUTE_ET = int(os.getenv("WATCHLIST_ALERT_END_MINUTE_ET", "10"))
EASTERN_TZ = ZoneInfo("America/New_York")


class ShareToChannelView(discord.ui.View):
    def __init__(self, owner_id: int, public_messages: list[str]) -> None:
        super().__init__(timeout=900)
        self.owner_id = owner_id
        self.public_messages = public_messages

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
                public_messages=self.public_messages,
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

    def __init__(self, owner_id: int, public_messages: list[str]) -> None:
        super().__init__()
        self.owner_id = owner_id
        self.public_messages = public_messages

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
        messages = list(self.public_messages)
        if comment_text and messages:
            messages = _append_comment_to_share_messages(messages, comment_text)

        for content in messages:
            await interaction.channel.send(content[:1900])
        await interaction.response.send_message("Shared to channel.", ephemeral=True)


def _share_view(interaction: discord.Interaction, title: str, body: str) -> ShareToChannelView:
    public_messages = _split_share_messages(title, interaction.user.id, body)
    return ShareToChannelView(owner_id=interaction.user.id, public_messages=public_messages)


def _split_share_messages(title: str, user_id: int, body: str) -> list[str]:
    header = f"**{title}**\nShared by <@{user_id}>"
    messages: list[str] = []
    current = header

    for block in body.split("\n\n"):
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) <= 1900:
            current = candidate
            continue

        if current:
            messages.append(current)

        continuation = f"**{title}** (cont.)"
        if len(f"{continuation}\n{block}") <= 1900:
            current = f"{continuation}\n{block}"
            continue

        block_lines = block.splitlines() or [block]
        current = continuation
        for line in block_lines:
            while line:
                line_candidate = f"{current}\n{line}" if current else line
                if len(line_candidate) <= 1900:
                    current = line_candidate
                    break

                available = 1900 - len(current) - (1 if current else 0)
                if available <= 0:
                    if current:
                        messages.append(current)
                    current = continuation
                    available = 1900 - len(current) - 1

                chunk = line[:available]
                current = f"{current}\n{chunk}" if current else chunk
                messages.append(current)
                current = continuation
                line = line[available:]

    if current:
        messages.append(current)

    return messages


def _append_comment_to_share_messages(messages: list[str], comment_text: str) -> list[str]:
    if not messages:
        return messages

    comment_block = f"\n\n**Comment:** {comment_text}"
    if len(messages[0]) + len(comment_block) <= 1900:
        messages[0] = f"{messages[0]}{comment_block}"
        return messages

    comment_messages = _split_text_chunks(comment_text)
    if comment_messages:
        comment_messages[0] = f"**Comment:** {comment_messages[0]}"
    else:
        comment_messages = ["**Comment:**"]

    return [messages[0], *comment_messages, *messages[1:]]


def _split_text_chunks(text: str) -> list[str]:
    chunks: list[str] = []
    remaining = text
    while remaining:
        chunks.append(remaining[:1900])
        remaining = remaining[1900:]
    return chunks


def _build_news_share_body(symbol: str, items: list[dict], limit: int) -> str:
    lines = [f"Top {min(limit, len(items))} news items for {symbol.upper()}."]
    remaining = max(0, 1700 - len(f"{symbol.upper()} News") - len("\nShared by <@0>\n") - len(lines[0]))

    for idx, item in enumerate(items[:limit], start=1):
        title = (item.get("title") or "Untitled").strip()
        url = (item.get("url") or "").strip()
        site = (item.get("site") or "Unknown source").strip()
        published = (item.get("published_date") or "Unknown date").strip()

        entry = f"{idx}. {title}"
        if url:
            entry += f"\n{url}"
        entry += f"\n{site} | {published}"

        if len(entry) + 2 > remaining:
            break

        lines.append(entry)
        remaining -= len(entry) + 2

    return "\n\n".join(lines)


def _build_13f_share_body(symbol: str, payload: dict[str, object], limit: int) -> str:
    latest_period = payload.get("latest_report_period", "n/a")
    previous_period = payload.get("previous_report_period", "n/a")
    issuer_name = payload.get("issuer_name") or "Unknown issuer"
    rows = payload.get("data", [])
    if not isinstance(rows, list):
        rows = []

    lines = [
        f"{symbol.upper()} 13F holders delta",
        f"{issuer_name} | {latest_period} vs {previous_period}",
        f"Showing {min(limit, len(rows))} managers",
    ]

    for idx, item in enumerate(rows[:limit], start=1):
        if not isinstance(item, dict):
            continue
        lines.append(
            f"{idx}. {item.get('manager_name', 'Unknown')} | "
            f"{item.get('change_type', 'n/a')} | "
            f"Sh {fmt_signed_compact(item.get('share_delta'))} | "
            f"Val {fmt_signed_compact(item.get('value_delta_thousands'))}"
        )

    return "\n".join(lines)


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


def _as_float(value) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _trend_emoji(change=None, pct=None) -> str:
    probe = pct if pct is not None else change
    value = _as_float(probe)
    if value is None:
        return "⚪"
    if value > 0:
        return "🟢📈"
    if value < 0:
        return "🔴📉"
    return "⚪➖"


def _clip_cell(value: object, width: int) -> str:
    text = str(value)
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return f"{text[:width-1]}…"


def _render_table(headers: list[str], rows: list[list[object]], max_widths: list[int]) -> str:
    widths: list[int] = []
    for idx, header in enumerate(headers):
        cell_max = max((len(str(row[idx])) for row in rows), default=0)
        widths.append(min(max(len(header), cell_max), max_widths[idx]))

    def fmt_row(values: list[object]) -> str:
        cells = [_clip_cell(values[idx], widths[idx]).ljust(widths[idx]) for idx in range(len(values))]
        return " | ".join(cells)

    sep = "-+-".join("-" * width for width in widths)
    body = [fmt_row([str(h) for h in headers]), sep]
    body.extend(fmt_row(row) for row in rows)
    return "```text\n" + "\n".join(body) + "\n```"


def _extract_http_error(e: httpx.HTTPStatusError) -> tuple[int, str]:
    status = e.response.status_code if e.response is not None else 0
    detail = ""
    if e.response is not None:
        try:
            payload = e.response.json()
            detail = payload.get("detail", "") if isinstance(payload, dict) else str(payload)
        except Exception:
            detail = (e.response.text or "").strip()
    return status, detail


def _is_valid_symbol(symbol: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9.\-]{0,9}", symbol.strip()))


def _is_valid_quote_symbol(symbol: str) -> bool:
    value = symbol.strip()
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9.\-]{0,9}", value) or re.fullmatch(r"\d{1,5}", value))


async def _send_symbol_http_error(
    interaction: discord.Interaction,
    *,
    symbol: str,
    action: str,
    status: int,
    detail: str,
    not_found_message: str | None = None,
) -> None:
    symbol_u = symbol.upper()
    lowered = detail.lower()
    if status == 404 or any(tag in lowered for tag in ("not found", "no data", "no records", "no recent news")):
        if not_found_message:
            await _send(interaction, not_found_message)
        else:
            await _send(
                interaction,
                f"I couldn't find records for `{symbol_u}`.\n"
                "Please check the ticker and try again (for example: `AAPL`, `MSFT`, `NVDA`).",
            )
        return

    if status == 422:
        await _send(
            interaction,
            "That ticker format looks invalid. Please use a valid stock symbol (for example: `AAPL`).",
        )
        return

    short_detail = detail[:220] if detail else "Unexpected upstream error."
    await _send(
        interaction,
        f"Couldn't {action} for `{symbol_u}` right now.\n{short_detail}",
    )


class TradeBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.watchlist_summary_task: asyncio.Task | None = None
        self.watchlist_summary_last_date: str | None = None
        self.watchlist_alert_task: asyncio.Task | None = None
        self.watchlist_alert_last_date: str | None = None
        self.watchlist_alert_sent: set[str] = set()

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
        if WATCHLIST_ALERT_ENABLED and self.watchlist_alert_task is None:
            self.watchlist_alert_task = asyncio.create_task(self._watchlist_alert_loop())

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

    async def _watchlist_alert_loop(self) -> None:
        while True:
            try:
                now_et = datetime.now(EASTERN_TZ)
                if now_et.weekday() < 5:
                    date_key = now_et.date().isoformat()
                    if self.watchlist_alert_last_date != date_key:
                        self.watchlist_alert_last_date = date_key
                        self.watchlist_alert_sent.clear()

                    start_t = time(hour=WATCHLIST_ALERT_START_HOUR_ET, minute=WATCHLIST_ALERT_START_MINUTE_ET)
                    end_t = time(hour=WATCHLIST_ALERT_END_HOUR_ET, minute=WATCHLIST_ALERT_END_MINUTE_ET)
                    in_window = start_t <= now_et.time() <= end_t
                    if in_window:
                        await self._send_watchlist_threshold_alerts(date_key)
            except Exception as e:
                print(f"watchlist alert loop error: {e}")

            await asyncio.sleep(max(30, WATCHLIST_ALERT_POLL_SECONDS))

    async def _send_watchlist_threshold_alerts(self, date_key: str) -> None:
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

                for symbol in symbols[:5]:
                    symbol_u = str(symbol).upper()
                    try:
                        brief_resp = await client.get(
                            f"{STRATEGY_ENGINE_URL}/v1/brief",
                            params={"symbol": symbol_u},
                        )
                        if brief_resp.status_code >= 400:
                            continue
                        item = brief_resp.json()
                        change_pct = _as_float(item.get("changePercentage"))
                        if change_pct is None or abs(change_pct) < WATCHLIST_ALERT_THRESHOLD_PCT:
                            continue

                        direction = "up" if change_pct > 0 else "down"
                        alert_key = f"{date_key}:{uid}:{symbol_u}:{direction}"
                        if alert_key in self.watchlist_alert_sent:
                            continue

                        message = (
                            f"**Watchlist Alert: {symbol_u} {change_pct:+.2f}% today**\n"
                            f"Threshold: {WATCHLIST_ALERT_THRESHOLD_PCT:.2f}% | "
                            f"Price: {fmt_price(item.get('price'))} | "
                            f"Range: {fmt_range(item.get('dayLow'), item.get('dayHigh'), price=True)} | "
                            f"Volume: {fmt_compact(item.get('volume'))}"
                        )

                        user = await self.fetch_user(uid)
                        await user.send(message[:1900])
                        self.watchlist_alert_sent.add(alert_key)
                    except Exception as e:
                        print(f"Failed to send watchlist alert to {uid} for {symbol_u}: {e}")


bot = TradeBot()


@bot.tree.command(name="quote", description="Get a brief quote summary for a symbol", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. AAPL or 941 (optional: blank uses your watchlist)")
async def brief(interaction: discord.Interaction, symbol: str | None = None) -> None:
    await _defer(interaction)

    async with httpx.AsyncClient(timeout=20.0) as client:
        if symbol:
            if not _is_valid_quote_symbol(symbol):
                await _send(
                    interaction,
                    "That ticker format looks invalid. Use a US ticker like `AAPL` or an HK numeric ticker like `941`.",
                )
                return
            try:
                resp = await client.get(f"{STRATEGY_ENGINE_URL}/v1/brief", params={"symbol": symbol.upper()})
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                status, detail = _extract_http_error(e)
                await _send_symbol_http_error(
                    interaction,
                    symbol=symbol,
                    action="fetch quote",
                    status=status,
                    detail=detail,
                )
                return
            except httpx.RequestError as e:
                await _send(
                    interaction,
                    f"Failed to fetch quote for {symbol.upper()}: could not reach strategy-engine. {e}",
                )
                return

            data = resp.json()
            trend = _trend_emoji(data.get("change"), data.get("changePercentage"))
            title = data["symbol"]
            if data.get("chineseName"):
                title = f"{title} {data.get('chineseName')}"
            message = (
                f"**{trend} {title} Quote**\n"
                f"Price: {fmt_price(data.get('price'))} | "
                f"Change: {fmt_change(data.get('change'), data.get('changePercentage'))}\n"
                f"Range: {fmt_range(data.get('dayLow'), data.get('dayHigh'), price=True)} | "
                f"Volume: {fmt_compact(data.get('volume'))}"
            )
            await _send(
                interaction,
                message,
                view=_share_view(interaction, f"{title} Brief", message),
            )
            return

        watch_resp = await client.get(
            f"{STRATEGY_ENGINE_URL}/v1/watchlist",
            params={"user_id": str(interaction.user.id)},
        )
        if watch_resp.status_code >= 400:
            await _send(interaction, f"watch_list failed: {watch_resp.text}")
            return

        symbols = watch_resp.json().get("watchlist", [])
        if not isinstance(symbols, list) or not symbols:
            await _send(interaction, "Your watchlist is empty. Add symbols with `/watch_add`.")
            return

        async def load_one(sym: str) -> dict:
            one_resp = await client.get(f"{STRATEGY_ENGINE_URL}/v1/brief", params={"symbol": sym})
            if one_resp.status_code >= 400:
                return {"symbol": sym, "_error": True}
            payload = one_resp.json()
            payload["_error"] = False
            return payload

        payloads = await asyncio.gather(*(load_one(str(sym).upper()) for sym in symbols[:5]))
        rows: list[list[object]] = []
        for item in payloads:
            sym = item.get("symbol", "?")
            if item.get("_error"):
                rows.append([sym, "n/a", "n/a", "n/a", "n/a"])
                continue
            chg_pct = _as_float(item.get("changePercentage"))
            chg_pct_txt = f"{chg_pct:+.2f}%" if chg_pct is not None else "n/a"
            rows.append([
                sym,
                fmt_price(item.get("price")),
                chg_pct_txt,
                fmt_range(item.get("dayLow"), item.get("dayHigh"), price=True),
                fmt_compact(item.get("volume")),
            ])

        table = _render_table(
            ["SYM", "PRICE", "CHG%", "RANGE", "VOL"],
            rows,
            [6, 11, 7, 19, 10],
        )
        message = f"**Watchlist Quotes ({len(rows)})**\n{table}"
        await _send(
            interaction,
            message,
            view=_share_view(interaction, "Watchlist Quotes", message),
        )

@bot.tree.command(name="quote_detail", description="Get a detailed quote snapshot", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. NVDA or 941 (optional: blank uses your watchlist)")
async def quote_detail(interaction: discord.Interaction, symbol: str | None = None) -> None:
    await _defer(interaction)

    async with httpx.AsyncClient(timeout=20.0) as client:
        if symbol:
            if not _is_valid_quote_symbol(symbol):
                await _send(
                    interaction,
                    "That ticker format looks invalid. Use a US ticker like `AAPL` or an HK numeric ticker like `941`.",
                )
                return
            try:
                resp = await client.get(
                    f"{STRATEGY_ENGINE_URL}/v1/quote-detail",
                    params={"symbol": symbol.upper()},
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                status, detail = _extract_http_error(e)
                await _send_symbol_http_error(
                    interaction,
                    symbol=symbol,
                    action="fetch quote detail",
                    status=status,
                    detail=detail,
                )
                return
            except httpx.RequestError as e:
                await _send(
                    interaction,
                    f"Failed to fetch quote detail for {symbol.upper()}: could not reach strategy-engine. {e}",
                )
                return

            data = resp.json()
            trend = _trend_emoji(data.get("change"), data.get("changesPercentage"))
            if data.get("market") == "HK":
                lines = [
                    f"**{trend} {data.get('symbol', symbol.upper())} Quote Detail**",
                    f"Company: {data.get('companyName', 'n/a')} | Chinese: {data.get('chineseName', 'n/a')}",
                    f"Exchange: {data.get('exchangeShortName', 'n/a')}",
                    f"Price: {fmt_price(data.get('price'))} | Change: {fmt_change(data.get('change'), data.get('changesPercentage'))}",
                    f"Open: {fmt_price(data.get('open'))} | Previous Close: {fmt_price(data.get('previousClose'))}",
                    f"Day Range: {fmt_range(data.get('dayLow'), data.get('dayHigh'), price=True)} | "
                    f"52W Range: {fmt_range(data.get('yearLow'), data.get('yearHigh'), price=True)}",
                    f"Volume: {fmt_compact(data.get('volume'))}",
                    f"VWAP: {fmt_price(data.get('vwap'))} | Last Updated: {data.get('lastUpdated', 'n/a')}",
                ]
            else:
                lines = [
                    f"**{trend} {data.get('symbol', symbol.upper())} Quote Detail**",
                    f"Company: {data.get('companyName', 'n/a')} | Exchange: {data.get('exchangeShortName', 'n/a')}",
                    f"Price: {fmt_price(data.get('price'))} | Change: {fmt_change(data.get('change'), data.get('changesPercentage'))}",
                    f"Open: {fmt_price(data.get('open'))} | Previous Close: {fmt_price(data.get('previousClose'))}",
                    f"Day Range: {fmt_range(data.get('dayLow'), data.get('dayHigh'), price=True)} | "
                    f"52W Range: {fmt_range(data.get('yearLow'), data.get('yearHigh'), price=True)}",
                    f"Volume: {fmt_compact(data.get('volume'))} | Avg Volume: {fmt_compact(data.get('avgVolume'))}",
                    f"Market Cap: {fmt_compact(data.get('marketCap'))}",
                ]

            message = "\n".join(lines)
            await _send(
                interaction,
                message,
                view=_share_view(interaction, f"{data.get('symbol', symbol.upper())} Quote Detail", message),
            )
            return

        watch_resp = await client.get(
            f"{STRATEGY_ENGINE_URL}/v1/watchlist",
            params={"user_id": str(interaction.user.id)},
        )
        if watch_resp.status_code >= 400:
            await _send(interaction, f"watch_list failed: {watch_resp.text}")
            return

        symbols = watch_resp.json().get("watchlist", [])
        if not isinstance(symbols, list) or not symbols:
            await _send(interaction, "Your watchlist is empty. Add symbols with `/watch_add`.")
            return

        async def load_one_detail(sym: str) -> dict:
            one_resp = await client.get(
                f"{STRATEGY_ENGINE_URL}/v1/quote-detail",
                params={"symbol": sym},
            )
            if one_resp.status_code >= 400:
                return {"symbol": sym, "_error": True}
            payload = one_resp.json()
            payload["_error"] = False
            return payload

        payloads = await asyncio.gather(*(load_one_detail(str(sym).upper()) for sym in symbols[:5]))
        rows: list[list[object]] = []
        for item in payloads:
            sym = item.get("symbol", "?")
            if item.get("_error"):
                rows.append([sym, "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"])
                continue
            chg_pct = _as_float(item.get("changesPercentage"))
            chg_pct_txt = f"{chg_pct:+.2f}%" if chg_pct is not None else "n/a"
            rows.append([
                sym,
                fmt_price(item.get("price")),
                chg_pct_txt,
                f"{fmt_price(item.get('open'))}/{fmt_price(item.get('previousClose'))}",
                fmt_range(item.get("dayLow"), item.get("dayHigh"), price=True),
                f"{fmt_compact(item.get('volume'))}/{fmt_compact(item.get('avgVolume'))}",
                fmt_compact(item.get("marketCap")),
            ])

        table = _render_table(
            ["SYM", "PX", "CHG%", "O/PC", "DAY", "VOL/AVG", "MCAP"],
            rows,
            [6, 11, 7, 23, 19, 18, 10],
        )
        message = f"**Watchlist Quote Detail ({len(rows)})**\n{table}"
        await _send(
            interaction,
            message,
            view=_share_view(interaction, "Watchlist Quote Detail", message),
        )


@bot.tree.command(name="world_index", description="Show key world equity volatility indexes", guild=GUILD_OBJECT)
async def world_index(interaction: discord.Interaction) -> None:
    await _defer(interaction)

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(f"{STRATEGY_ENGINE_URL}/v1/world-indexes")
            resp.raise_for_status()

        payload = resp.json()
        rows_in = payload.get("data", []) if isinstance(payload.get("data"), list) else []
        rows: list[list[object]] = []
        for item in rows_in:
            if not isinstance(item, dict):
                continue
            price = _as_float(item.get("price"))
            change = _as_float(item.get("change"))
            change_pct = _as_float(item.get("changePercentage"))
            rows.append(
                [
                    item.get("label") or item.get("symbol", "?"),
                    fmt_price(price) if price is not None else "n/a",
                    f"{change:+.2f}" if change is not None else "n/a",
                    f"{change_pct:+.2f}%" if change_pct is not None else "n/a",
                ]
            )

        table = _render_table(
            ["INDEX", "PRICE", "CHG", "CHG%"],
            rows,
            [18, 12, 10, 10],
        )
        message = f"**World Indexes**\n{table}"
        await _send(
            interaction,
            message,
            view=_share_view(interaction, "World Indexes", message),
        )

    except httpx.HTTPStatusError as e:
        status = e.response.status_code if e.response is not None else 0
        detail = ""
        if e.response is not None:
            try:
                payload = e.response.json()
                detail = payload.get("detail", "") if isinstance(payload, dict) else str(payload)
            except Exception:
                detail = (e.response.text or "").strip()
        short_detail = detail[:220] if detail else "Unexpected upstream error."
        await _send(interaction, f"Couldn't fetch world indexes right now.\n{short_detail}")
    except httpx.RequestError as e:
        await _send(interaction, f"Couldn't fetch world indexes right now: could not reach strategy-engine. {e}")
    except Exception as e:
        await _send(interaction, f"Couldn't fetch world indexes right now: {e}")

@bot.tree.command(name="watch_add", description="Add a symbol to your watchlist", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. TSLA or 700")
async def watch_add(interaction: discord.Interaction, symbol: str) -> None:
    await _defer(interaction)
    if not _is_valid_quote_symbol(symbol):
        await _send(
            interaction,
            "That ticker format looks invalid. Use a US ticker like `AAPL` or an HK numeric ticker like `700`.",
        )
        return
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
        elif resp.status_code in (404, 422):
            await _send_symbol_http_error(
                interaction,
                symbol=symbol,
                action="add symbol to watchlist",
                status=resp.status_code,
                detail=detail,
            )
        else:
            await _send(interaction, f"watch_add failed: {detail or resp.text}")
        return

    watchlist = resp.json().get("watchlist", [])
    await _send(interaction, f"Watchlist updated: {', '.join(watchlist)}")


@bot.tree.command(name="watch_remove", description="Remove a symbol from your watchlist", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. TSLA or 700")
async def watch_remove(interaction: discord.Interaction, symbol: str) -> None:
    await _defer(interaction)
    if not _is_valid_quote_symbol(symbol):
        await _send(
            interaction,
            "That ticker format looks invalid. Use a US ticker like `AAPL` or an HK numeric ticker like `700`.",
        )
        return
    payload = {"user_id": str(interaction.user.id), "symbol": symbol}

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(f"{STRATEGY_ENGINE_URL}/v1/watchlist/remove", json=payload)

    if resp.status_code >= 400:
        await _send(interaction, f"watch_remove failed: {resp.text}")
        return

    data = resp.json()
    removed = bool(data.get("removed"))
    watchlist = data.get("watchlist", [])
    display_symbol = symbol.strip().upper() if not symbol.strip().isdigit() else str(int(symbol.strip()))
    if removed:
        await _send(interaction, f"Removed {display_symbol}. Watchlist: {', '.join(watchlist) or 'empty'}")
    else:
        await _send(interaction, f"{display_symbol} was not in your watchlist. Current: {', '.join(watchlist) or 'empty'}")


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
    symbol="Ticker symbol, e.g. NVDA or 941",
    limit="Number of articles to return (1-10)",
)
async def news_command(
    interaction: discord.Interaction,
    symbol: str,
    limit: app_commands.Range[int, 1, 10] = 5,
) -> None:
    await _defer(interaction)
    if not _is_valid_quote_symbol(symbol):
        await _send(
            interaction,
            "That ticker format looks invalid. Use a US ticker like `AAPL` or an HK numeric ticker like `941`.",
        )
        return

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(
                f"{STRATEGY_ENGINE_URL}/v1/news",
                params={"symbol": symbol.upper(), "limit": limit},
            )
            resp.raise_for_status()

        payload = resp.json()
        items = payload.get("data", [])

        display_symbol = payload.get("symbol", symbol.upper()) if isinstance(payload, dict) else symbol.upper()
        chinese_name = payload.get("chineseName") if isinstance(payload, dict) else None

        if not items:
            await _send(interaction, f"No recent news found for {display_symbol}.")
            return

        embed = discord.Embed(
            title=f"{display_symbol} news" if not chinese_name else f"{display_symbol} {chinese_name} news",
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

        share_body = _build_news_share_body(display_symbol, items, limit)
        await _send(
            interaction,
            embed=embed,
            view=_share_view(interaction, f"{symbol.upper()} News", share_body),
        )

    except httpx.HTTPStatusError as e:
        status, detail = _extract_http_error(e)
        await _send_symbol_http_error(
            interaction,
            symbol=symbol,
            action="fetch news",
            status=status,
            detail=detail,
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
    if not _is_valid_symbol(symbol):
        await _send(
            interaction,
            "That ticker format looks invalid. Please use a valid stock symbol (for example: `AAPL`).",
        )
        return

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

        header_lines = [f"**{symbol_value} Insider Trades**"]
        if year is not None and quarter is not None:
            header_lines.append(f"Period: {year} Q{quarter}")
        elif year is not None:
            header_lines.append(f"Year: {year}")
        header_lines.append(f"Window: last {window_days}d | Showing: {min(limit, len(trades))}/{total_recent}")

        stats_parts = [
            f"BuyTx {fmt_compact(acquired_tx)}",
            f"SellTx {fmt_compact(disposed_tx)}",
            f"BuyVal {fmt_compact(total_acquired)}",
            f"SellVal {fmt_compact(total_disposed)}",
            f"AvgBuy {fmt_compact(avg_acquired)}",
            f"AvgSell {fmt_compact(avg_disposed)}",
        ]
        if acquired_ratio is not None:
            stats_parts.append(f"B/S {acquired_ratio}")
        if total_shares_traded is not None:
            stats_parts.append(f"Shares {fmt_compact(total_shares_traded)}")
        if avg_value is not None:
            stats_parts.append(f"AvgVal {fmt_compact(avg_value)}")
        header_lines.append("Stats: " + " | ".join(stats_parts))
        raw_link = pick("link", "url")
        if raw_link:
            header_lines.append(f"Source: {raw_link}")
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
                f"{idx}. {transaction_date} {tx_type} {reporter} | "
                f"Sh {shares} @ {price} | Val {value} | Filed {filing_date}"
            ]
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
            await _send(
                interaction,
                chunk,
                view=_share_view(interaction, f"{symbol_value} Insider Trades", chunk),
            )

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


@bot.tree.command(name="13f_delta", description="Compare latest two 13F quarters for a symbol", guild=GUILD_OBJECT)
@app_commands.describe(
    symbol="Ticker symbol, e.g. AAPL",
    limit="Number of managers to show (1-20)",
)
async def holders_delta(
    interaction: discord.Interaction,
    symbol: str,
    limit: app_commands.Range[int, 1, 20] = 10,
) -> None:
    await _defer(interaction)
    if not _is_valid_symbol(symbol):
        await _send(
            interaction,
            "That ticker format looks invalid. Please use a valid stock symbol (for example: `AAPL`).",
        )
        return

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{STRATEGY_ENGINE_URL}/v1/13f/holdings-delta",
                params={"symbol": symbol.upper(), "limit": limit},
            )
            resp.raise_for_status()

        payload = resp.json()
        rows = payload.get("data", [])
        if not isinstance(rows, list) or not rows:
            await _send(interaction, f"No 13F holder comparison rows found for `{symbol.upper()}`.")
            return

        latest_period = payload.get("latest_report_period", "n/a")
        previous_period = payload.get("previous_report_period", "n/a")
        issuer_name = payload.get("issuer_name") or "Unknown issuer"

        table_rows: list[list[object]] = []
        for item in rows[:limit]:
            if not isinstance(item, dict):
                continue
            manager = item.get("manager_name") or "Unknown"
            table_rows.append([
                manager,
                item.get("change_type", "n/a"),
                fmt_signed_compact(item.get("share_delta")),
                fmt_compact(item.get("latest_shares")),
                fmt_signed_compact(item.get("value_delta_thousands")),
            ])

        table = _render_table(
            ["MANAGER", "TYPE", "DELTA_SH", "LATEST_SH", "DELTA_VAL"],
            table_rows,
            [32, 10, 12, 12, 12],
        )
        message = (
            f"**{symbol.upper()} 13F Holders Delta**\n"
            f"{issuer_name}\n"
            f"Window: {latest_period} vs {previous_period}\n"
            f"{table}"
        )
        share_body = _build_13f_share_body(symbol, payload, limit)
        await _send(
            interaction,
            message,
            view=_share_view(interaction, f"{symbol.upper()} 13F Holders Delta", share_body),
        )

    except httpx.HTTPStatusError as e:
        status, detail = _extract_http_error(e)
        await _send_symbol_http_error(
            interaction,
            symbol=symbol,
            action="fetch 13F holders delta",
            status=status,
            detail=detail,
            not_found_message=(
                f"I couldn't find 13F comparison data for `{symbol.upper()}`.\n"
                "Please check the ticker and note that 13F coverage is currently limited to the supported symbol set."
            ),
        )
    except httpx.RequestError as e:
        await _send(
            interaction,
            f"Failed to fetch 13F holders delta for {symbol.upper()}: could not reach strategy-engine. {e}"
        )
    except Exception as e:
        await _send(interaction, f"Failed to fetch 13F holders delta for {symbol.upper()}: {e}")


@bot.tree.command(name="earnings_risk", description="Get earnings risk score and drivers for a symbol", guild=GUILD_OBJECT)
@app_commands.describe(symbol="Ticker symbol, e.g. AAPL")
async def earnings_risk(interaction: discord.Interaction, symbol: str) -> None:
    await _defer(interaction)
    if not _is_valid_symbol(symbol):
        await _send(
            interaction,
            "That ticker format looks invalid. Please use a valid stock symbol (for example: `AAPL`).",
        )
        return

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
            f"Next Earnings: {next_event.get('date', 'n/a')} ({next_event.get('days_to_event', 'n/a')} days) | "
            f"EPS Est: {next_event.get('eps_estimated', 'n/a')} | "
            f"Revenue Est: {fmt_compact(next_event.get('revenue_estimated'))}",
            f"History: Beats/Misses {history.get('beat_count', 'n/a')}/{history.get('miss_count', 'n/a')} | "
            f"Avg Abs EPS Surprise: {fmt_percent(history.get('avg_abs_eps_surprise_pct'))}",
            f"Market: Price {fmt_price(market.get('price'))} | "
            f"Change % {fmt_percent(market.get('change_percentage'))} | "
            f"Day Range % {fmt_percent(market.get('day_range_pct'))}",
            f"Components: Proximity {components.get('proximity_0_35', 'n/a')}/35 | "
            f"Surprise Var {components.get('surprise_variability_0_25', 'n/a')}/25 | "
            f"Miss History {components.get('miss_history_0_20', 'n/a')}/20 | "
            f"Intraday Vol {components.get('intraday_volatility_0_20', 'n/a')}/20 | "
            f"Momentum Shock {components.get('momentum_shock_0_10', 'n/a')}/10",
        ]

        message = "\n".join(lines)
        await _send(
            interaction,
            message,
            view=_share_view(interaction, f"{symbol_value} Earnings Risk", message),
        )

    except httpx.HTTPStatusError as e:
        status, detail = _extract_http_error(e)
        await _send_symbol_http_error(
            interaction,
            symbol=symbol,
            action="calculate earnings risk",
            status=status,
            detail=detail,
            not_found_message=(
                f"I couldn't find enough earnings data for `{symbol.upper()}` to calculate risk."
            ),
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
    if not _is_valid_symbol(symbol):
        await _send(
            interaction,
            "That ticker format looks invalid. Please use a valid stock symbol (for example: `AAPL`).",
        )
        return

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
        trend_icon = _trend_emoji(quote.get("change"), quote.get("changePercentage"))
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
            f"**{trend_icon} {symbol_value} Catalyst Brief**",
            f"Price: {fmt_price(quote.get('price'))} | "
            f"Change: {fmt_change(quote.get('change'), quote.get('changePercentage'))} | "
            f"Day Range: {fmt_range(quote.get('dayLow'), quote.get('dayHigh'), price=True)}",
            f"Earnings Risk: {risk.get('score', 'n/a')}/100 ({risk.get('label', 'n/a')}) | "
            f"Next Earnings: {next_event.get('date', 'n/a')} ({next_event.get('days_to_event', 'n/a')} days)",
            f"Insider (60d): Acquired/Disposed {fmt_compact(acquired_tx)}/{fmt_compact(disposed_tx)} ({ratio_text})",
        ]

        if latest_trade:
            lines.append(
                f"Latest: {latest_trade.get('transaction_date', 'n/a')} | "
                f"{latest_trade.get('type', 'n/a')} | "
                f"{latest_trade.get('reporting_name', 'n/a')}"
            )

        lines.append("Top News:")
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
        status, detail = _extract_http_error(e)
        await _send_symbol_http_error(
            interaction,
            symbol=symbol,
            action="build catalyst brief",
            status=status,
            detail=detail,
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
