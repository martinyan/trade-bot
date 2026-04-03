import os
import asyncio
import re
from datetime import datetime, time
from zoneinfo import ZoneInfo

import discord
import httpx
import pandas_market_calendars as mcal
from discord import app_commands
from formatters import fmt_compact, fmt_price, fmt_change, fmt_range, fmt_percent, fmt_signed_compact

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
DISCORD_GUILD_ID = os.getenv("DISCORD_GUILD_ID", "")
STRATEGY_ENGINE_URL = os.getenv("STRATEGY_ENGINE_URL", "http://strategy-engine:8002").rstrip("/")
DASHBOARD_BASE_URL = os.getenv("DASHBOARD_BASE_URL", "").rstrip("/")
DASHBOARD_PUBLIC_URL = os.getenv("DASHBOARD_PUBLIC_URL", "").rstrip("/")
DASHBOARD_WARMUP_ENABLED = os.getenv("DASHBOARD_WARMUP_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DASHBOARD_WARMUP_POLL_SECONDS = int(os.getenv("DASHBOARD_WARMUP_POLL_SECONDS", "900"))
MARKETSNAP_BROADCAST_ENABLED = os.getenv("MARKETSNAP_BROADCAST_ENABLED", "false").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
MARKETSNAP_CHANNEL_ID = os.getenv("MARKETSNAP_CHANNEL_ID", "")
MARKETSNAP_HOUR_ET = int(os.getenv("MARKETSNAP_HOUR_ET", "9"))
MARKETSNAP_MINUTE_ET = int(os.getenv("MARKETSNAP_MINUTE_ET", "35"))
MARKETSNAP_WINDOW_MINUTES = int(os.getenv("MARKETSNAP_WINDOW_MINUTES", "20"))

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
NYSE_CALENDAR = mcal.get_calendar("NYSE")


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


def _fmt_signed_pct(value: object, digits: int = 1) -> str:
    num = _as_float(value)
    if num is None:
        return "n/a"
    return f"{num:+.{digits}f}%"


def _fmt_dashboard_time(raw_value: object) -> str:
    if not raw_value:
        return "n/a"
    text = str(raw_value)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except ValueError:
        return text


def _dashboard_page_url(path: str = "") -> str | None:
    if not DASHBOARD_PUBLIC_URL:
        return None
    if not path:
        return DASHBOARD_PUBLIC_URL
    return f"{DASHBOARD_PUBLIC_URL}{path}"


async def _dashboard_get_json(
    client: httpx.AsyncClient,
    path: str,
    *,
    timeout: float | None = None,
) -> dict:
    if not DASHBOARD_BASE_URL:
        raise RuntimeError("DASHBOARD_BASE_URL is not configured")
    resp = await client.get(
        f"{DASHBOARD_BASE_URL}{path}",
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected dashboard payload for {path}")
    return payload


async def _warm_dashboard_caches() -> None:
    if not DASHBOARD_BASE_URL:
        return

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            market_resp = await client.get(f"{DASHBOARD_BASE_URL}/api/market")
            print(f"dashboard warmup market: {market_resp.status_code}")
        except Exception as e:
            print(f"dashboard warmup market failed: {e}")

        try:
            status_resp = await client.get(f"{DASHBOARD_BASE_URL}/api/status")
            print(f"dashboard warmup status: {status_resp.status_code}")
            if status_resp.is_success:
                payload = status_resp.json()
                if isinstance(payload, dict) and payload.get("hasData"):
                    bullbear_resp = await client.get(f"{DASHBOARD_BASE_URL}/api/bull-bear")
                    print(f"dashboard warmup bullbear: {bullbear_resp.status_code}")
        except Exception as e:
            print(f"dashboard warmup bullbear failed: {e}")


def _build_ranked_line(
    items: list[tuple[str, object]],
    *,
    value_formatter,
    max_items: int | None = None,
) -> str:
    parts: list[str] = []
    use_items = items if max_items is None else items[:max_items]
    for label, value in use_items:
        parts.append(f"{label} {value_formatter(value)}")
    return " | ".join(parts) if parts else "n/a"


def _grade_token(stock: dict) -> str:
    ticker = str(stock.get("t", "?"))
    ext = str(stock.get("ext") or "")
    suffix = "!" if ext == "oe" else "*" if ext == "ex" else ""
    return f"{ticker}{suffix}"


def _pack_grade_line(grade: str, stocks: list[dict], max_width: int = 108) -> str:
    prefix = f"{grade:<2} ({len(stocks):>3}): "
    room = max(0, max_width - len(prefix))
    if room <= 0:
        return prefix.rstrip()

    used: list[str] = []
    consumed = 0
    for stock in stocks:
        token = _grade_token(stock)
        extra = len(token) if not used else len(token) + 1
        if consumed + extra > room:
            break
        used.append(token)
        consumed += extra

    remaining = len(stocks) - len(used)
    body = " ".join(used)
    if remaining > 0:
        more = f" +{remaining}"
        if len(body) + len(more) <= room:
            body += more
        elif used:
            while used and len(" ".join(used)) + len(more) > room:
                used.pop()
            body = " ".join(used)
            body = f"{body}{more}" if body else more.strip()

    return f"{prefix}{body}".rstrip()


def _build_bullbear_board(grades: dict[str, list[dict]], max_chars: int) -> tuple[str, int]:
    ordered_grades = [
        "A+",
        "A",
        "A-",
        "B+",
        "B",
        "B-",
        "C+",
        "C",
        "C-",
        "D",
        "D-",
        "E+",
        "E",
        "E-",
        "F+",
        "F",
        "F-",
        "G+",
        "G",
    ]
    lines: list[str] = []
    remaining_grades = 0
    consumed = 0

    for idx, grade in enumerate(ordered_grades):
        stocks = grades.get(grade, [])
        line = _pack_grade_line(grade, stocks)
        extra = len(line) + (1 if lines else 0)
        if consumed + extra > max_chars:
            remaining_grades = len(ordered_grades) - idx
            break
        lines.append(line)
        consumed += extra

    return "\n".join(lines), remaining_grades


def _is_nyse_trading_day(day_value) -> bool:
    date_text = day_value.isoformat() if hasattr(day_value, "isoformat") else str(day_value)
    schedule = NYSE_CALENDAR.schedule(start_date=date_text, end_date=date_text)
    return not schedule.empty


async def _build_marketsnap_message() -> str:
    async with httpx.AsyncClient(timeout=20.0) as client:
        payload = await _dashboard_get_json(client, "/api/market")

    tickers = payload.get("tickers", {}) if isinstance(payload.get("tickers"), dict) else {}
    movers = payload.get("movers", {}) if isinstance(payload.get("movers"), dict) else {}
    index_syms = ["SPY", "QQQ", "DIA", "QQQE", "RSP"]
    sector_syms = ["XLE", "XLU", "XLB", "XLI", "XLK", "XLRE", "XLF", "XLC", "XLY", "XLV", "XLP"]

    index_rows: list[list[object]] = []
    for sym in index_syms:
        item = tickers.get(sym)
        if not isinstance(item, dict):
            continue
        changes = item.get("c", {}) if isinstance(item.get("c"), dict) else {}
        index_rows.append([
            sym,
            fmt_price(item.get("price")),
            _fmt_signed_pct(changes.get("1D")),
            _fmt_signed_pct(changes.get("5D")),
            _fmt_signed_pct(changes.get("1M")),
            _fmt_signed_pct(changes.get("ytd")),
            _fmt_signed_pct(item.get("vsSma50")),
        ])

    sector_rank: list[tuple[str, float, float | None]] = []
    for sym in sector_syms:
        item = tickers.get(sym)
        if not isinstance(item, dict):
            continue
        changes = item.get("c", {}) if isinstance(item.get("c"), dict) else {}
        sector_rank.append((sym, _as_float(changes.get("1M")) or -9999.0, _as_float(changes.get("1D"))))
    sector_rank.sort(key=lambda row: row[1], reverse=True)

    sectors_line = _build_ranked_line(
        [(sym, one_month) for sym, one_month, _ in sector_rank],
        value_formatter=lambda value: _fmt_signed_pct(value),
    )
    sectors_day_line = _build_ranked_line(
        [(sym, one_day) for sym, _, one_day in sector_rank[:6]],
        value_formatter=lambda value: _fmt_signed_pct(value),
    )

    def mover_line(kind: str, *, price_instead_of_pct: bool = False) -> str:
        items = movers.get(kind, [])
        if not isinstance(items, list):
            return "n/a"
        pieces: list[str] = []
        for item in items[:5]:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "?")
            if price_instead_of_pct:
                pieces.append(f"{symbol} {fmt_compact(item.get('volume'))}")
            else:
                pieces.append(f"{symbol} {_fmt_signed_pct(item.get('changesPercentage'))}")
        return " | ".join(pieces) if pieces else "n/a"

    table = _render_table(
        ["ETF", "PX", "1D", "5D", "1M", "YTD", "v50"],
        index_rows,
        [5, 10, 7, 7, 7, 7, 7],
    )

    lines = [
        "**Market Snapshot**",
        f"Updated: {_fmt_dashboard_time(payload.get('updatedAt'))}",
        table,
        f"**Sectors 1M**\n{sectors_line}",
        f"**Sector Day Leaders**\n{sectors_day_line}",
        f"**Gainers** {mover_line('gainers')}",
        f"**Losers** {mover_line('losers')}",
        f"**Active** {mover_line('active', price_instead_of_pct=True)}",
    ]

    full_page_url = _dashboard_page_url()
    if full_page_url:
        lines.append(f"[Open full dashboard]({full_page_url})")

    message = "\n".join(lines)
    if len(message) > 1900:
        message = message[:1897] + "..."
    return message


async def _build_bullbear_message() -> str:
    async with httpx.AsyncClient(timeout=30.0) as client:
        status_payload = await _dashboard_get_json(client, "/api/status")
        if not status_payload.get("hasData"):
            if status_payload.get("isLoading"):
                extra = ""
                full_page_url = _dashboard_page_url("/bull-bear.html")
                if full_page_url:
                    extra = f"\n[Open full Bull vs Bear page]({full_page_url})"
                raise RuntimeError(
                    "Bull vs Bear data is still loading on the dashboard and is not ready yet.\n"
                    "The dashboard scraper can take about 20 minutes after a restart." + extra
                )
        payload = await _dashboard_get_json(client, "/api/bull-bear")

    grades = payload.get("grades", {}) if isinstance(payload.get("grades"), dict) else {}
    meta = payload.get("meta", {}) if isinstance(payload.get("meta"), dict) else {}

    bull_summary = _build_ranked_line(
        [
            ("A+", len(grades.get("A+", []))),
            ("A", len(grades.get("A", []))),
            ("A-", len(grades.get("A-", []))),
            ("B+", len(grades.get("B+", []))),
            ("B", len(grades.get("B", []))),
            ("B-", len(grades.get("B-", []))),
            ("C+", len(grades.get("C+", []))),
            ("C", len(grades.get("C", []))),
            ("C-", len(grades.get("C-", []))),
        ],
        value_formatter=lambda value: str(value),
    )
    bear_summary = _build_ranked_line(
        [
            ("D", len(grades.get("D", []))),
            ("D-", len(grades.get("D-", []))),
            ("E+", len(grades.get("E+", []))),
            ("E", len(grades.get("E", []))),
            ("E-", len(grades.get("E-", []))),
            ("F+", len(grades.get("F+", []))),
            ("F", len(grades.get("F", []))),
            ("F-", len(grades.get("F-", []))),
            ("G+", len(grades.get("G+", []))),
            ("G", len(grades.get("G", []))),
        ],
        value_formatter=lambda value: str(value),
    )

    header_lines = [
        "**Bull vs Bear**",
        (
            f"Bull {fmt_compact(meta.get('bullish'))} ({_fmt_signed_pct(meta.get('bullishPct'), digits=1).replace('+', '')})"
            f" | Bear {fmt_compact(meta.get('bearish'))} ({_fmt_signed_pct(meta.get('bearishPct'), digits=1).replace('+', '')})"
            f" | Total {fmt_compact(meta.get('total'))}"
        ),
        f"Updated: {_fmt_dashboard_time(meta.get('updatedAt'))}",
        f"**Bull Grades**\n{bull_summary}",
        f"**Bear Grades**\n{bear_summary}",
        "**Board**",
        "```text",
    ]

    full_page_url = _dashboard_page_url("/bull-bear.html")
    footer_lines = [
        "```",
        "Markers: * = extended (>12% 1M), ! = over-extended (>20% 1M)",
    ]
    if full_page_url:
        footer_lines.append(f"[Open full Bull vs Bear page]({full_page_url})")

    reserved = len("\n".join(header_lines + footer_lines)) + 2
    board_budget = max(0, 1900 - reserved)
    if board_budget <= 0:
        board_text = "Board omitted due to message size."
        hidden_grade_count = 0
    else:
        board_text, hidden_grade_count = _build_bullbear_board(grades, board_budget)
    if hidden_grade_count:
        board_text = f"{board_text}\n... {hidden_grade_count} more grade row(s) on full page"

    return "\n".join(header_lines + [board_text] + footer_lines)


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
        self.dashboard_warmup_task: asyncio.Task | None = None
        self.marketsnap_broadcast_task: asyncio.Task | None = None
        self.marketsnap_broadcast_last_date: str | None = None

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
        if DASHBOARD_BASE_URL and DASHBOARD_WARMUP_ENABLED and self.dashboard_warmup_task is None:
            self.dashboard_warmup_task = asyncio.create_task(self._dashboard_warmup_loop())
        if (
            DASHBOARD_BASE_URL
            and MARKETSNAP_BROADCAST_ENABLED
            and MARKETSNAP_CHANNEL_ID
            and self.marketsnap_broadcast_task is None
        ):
            self.marketsnap_broadcast_task = asyncio.create_task(self._marketsnap_broadcast_loop())

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

    async def _dashboard_warmup_loop(self) -> None:
        while True:
            try:
                await _warm_dashboard_caches()
            except Exception as e:
                print(f"dashboard warmup loop error: {e}")

            await asyncio.sleep(max(60, DASHBOARD_WARMUP_POLL_SECONDS))

    async def _marketsnap_broadcast_loop(self) -> None:
        while True:
            try:
                now_et = datetime.now(EASTERN_TZ)
                date_key = now_et.date().isoformat()
                target_dt = now_et.replace(
                    hour=MARKETSNAP_HOUR_ET,
                    minute=MARKETSNAP_MINUTE_ET,
                    second=0,
                    microsecond=0,
                )
                minutes_since = (now_et - target_dt).total_seconds() / 60.0
                in_window = 0 <= minutes_since <= MARKETSNAP_WINDOW_MINUTES

                if in_window and self.marketsnap_broadcast_last_date != date_key:
                    if _is_nyse_trading_day(now_et.date()):
                        await self._broadcast_dashboard_pair()
                    else:
                        print(f"dashboard broadcast skipped for non-trading day {date_key}")
                    self.marketsnap_broadcast_last_date = date_key
            except Exception as e:
                print(f"dashboard broadcast loop error: {e}")

            await asyncio.sleep(300)

    async def _broadcast_dashboard_pair(self) -> None:
        channel_id = int(MARKETSNAP_CHANNEL_ID)
        channel = self.get_channel(channel_id)
        if channel is None:
            channel = await self.fetch_channel(channel_id)

        market_message = await _build_marketsnap_message()
        bullbear_message = await _build_bullbear_message()
        market_message = market_message.replace("**Market Snapshot**", "**Market Snapshot | Auto Post**", 1)
        bullbear_message = bullbear_message.replace("**Bull vs Bear**", "**Bull vs Bear | Auto Post**", 1)
        await channel.send(market_message[:1900])
        await channel.send(bullbear_message[:1900])


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


@bot.tree.command(name="marketsnap", description="Show the dashboard market snapshot", guild=GUILD_OBJECT)
async def marketsnap(interaction: discord.Interaction) -> None:
    await _defer(interaction)

    if not DASHBOARD_BASE_URL:
        await _send(
            interaction,
            "Dashboard integration is not configured yet. Set `DASHBOARD_BASE_URL` for the Discord bot.",
        )
        return

    try:
        message = await _build_marketsnap_message()

        await _send(
            interaction,
            message,
            view=_share_view(interaction, "Market Snapshot", message),
        )

    except httpx.HTTPStatusError as e:
        status, detail = _extract_http_error(e)
        if status == 503:
            warm_url = _dashboard_page_url()
            extra = f"\n[Open full dashboard]({warm_url})" if warm_url else ""
            await _send(
                interaction,
                "Market dashboard data is still warming up and returned `503`.\n"
                "Please try `/marketsnap` again in a minute or two." + extra,
            )
            return
        short_detail = detail[:220] if detail else "Unexpected dashboard error."
        await _send(interaction, f"Couldn't fetch market snapshot right now.\n{short_detail}")
    except httpx.RequestError as e:
        await _send(
            interaction,
            f"Couldn't fetch market snapshot right now: could not reach dashboard. {e}",
        )
    except Exception as e:
        await _send(interaction, f"Couldn't fetch market snapshot right now: {e}")


@bot.tree.command(name="bullbear", description="Show the dashboard bull vs bear stock board", guild=GUILD_OBJECT)
async def bullbear(interaction: discord.Interaction) -> None:
    await _defer(interaction)

    if not DASHBOARD_BASE_URL:
        await _send(
            interaction,
            "Dashboard integration is not configured yet. Set `DASHBOARD_BASE_URL` for the Discord bot.",
        )
        return

    try:
        message = await _build_bullbear_message()

        await _send(
            interaction,
            message,
            view=_share_view(interaction, "Bull vs Bear", message),
        )

    except httpx.HTTPStatusError as e:
        status, detail = _extract_http_error(e)
        if status == 503:
            extra = ""
            full_page_url = _dashboard_page_url("/bull-bear.html")
            if full_page_url:
                extra = f"\n[Open full Bull vs Bear page]({full_page_url})"
            await _send(
                interaction,
                "Bull vs Bear data is still warming up and returned `503`.\n"
                "Please try `/bullbear` again shortly." + extra,
            )
            return
        short_detail = detail[:220] if detail else "Unexpected dashboard error."
        await _send(interaction, f"Couldn't fetch Bull vs Bear right now.\n{short_detail}")
    except httpx.RequestError as e:
        await _send(
            interaction,
            f"Couldn't fetch Bull vs Bear right now: could not reach dashboard. {e}",
        )
    except RuntimeError as e:
        await _send(interaction, str(e))
    except Exception as e:
        await _send(interaction, f"Couldn't fetch Bull vs Bear right now: {e}")

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


@bot.tree.command(
    name="insider_trades",
    description="Get insider trades for a symbol or scan recent purchases",
    guild=GUILD_OBJECT,
)
@app_commands.describe(
    symbol="Ticker symbol, e.g. AAPL (optional: blank scans top insider purchases from the last 5 days)",
    limit="Number of records to return (1-20, default 20)",
)
async def insider_trades(
    interaction: discord.Interaction,
    symbol: str | None = None,
    limit: app_commands.Range[int, 1, 20] = 20,
) -> None:
    await _defer(interaction)
    normalized_symbol = symbol.strip().upper() if symbol else ""

    if normalized_symbol and not _is_valid_symbol(normalized_symbol):
        await _send(
            interaction,
            "That ticker format looks invalid. Please use a valid stock symbol (for example: `AAPL`).",
        )
        return

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            request_days = 60 if normalized_symbol else 5
            params: dict[str, object] = {
                "limit": limit,
                "days": request_days,
            }
            if normalized_symbol:
                params["symbol"] = normalized_symbol
            resp = await client.get(
                f"{STRATEGY_ENGINE_URL}/v1/insider-trades/latest",
                params=params,
            )
            resp.raise_for_status()

        payload = resp.json()
        trades = payload.get("data", [])
        scan_mode = payload.get("mode") == "scan"
        symbol_value = payload.get("symbol", normalized_symbol)
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
            if scan_mode:
                await _send(interaction, "No insider purchase transactions were found in the scan window.")
            else:
                await _send(interaction, f"No insider trades found for {normalized_symbol}.")
            return

        window_days = payload.get("window_days", request_days)
        total_recent = payload.get("total_recent", len(trades))
        header_lines: list[str]
        if scan_mode:
            total_purchases = payload.get("total_purchases", len(trades))
            header_lines = ["**Recent Insider Purchase Scan**"]
            header_lines.append(
                f"Scanning insider purchases from the last {window_days} days | Purchases: {total_purchases} | Showing top {min(limit, len(trades))}"
            )
            header_lines.append(f"Trades scanned: {fmt_compact(total_recent)}")
        else:
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
            trade_symbol = trade.get("symbol") or "n/a"
            reporter = trade.get("reporting_name") or "Unknown"
            tx_type = trade.get("type") or "n/a"
            transaction_date = trade.get("transaction_date") or "n/a"
            filing_date = trade.get("filing_date") or "n/a"
            shares = fmt_compact(trade.get("securities_transacted"))
            price = fmt_price(trade.get("price"))
            value = fmt_compact(trade.get("value"))
            filing_url = trade.get("filing_url")

            if scan_mode:
                entry_lines = [
                    f"{idx}. {trade_symbol} | {transaction_date} {tx_type} {reporter} | "
                    f"Sh {shares} @ {price} | Val {value} | Filed {filing_date}"
                ]
            else:
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
                view=_share_view(
                    interaction,
                    f"{symbol_value or 'Recent'} Insider Trades",
                    chunk,
                ),
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
            if normalized_symbol:
                await _send(
                    interaction,
                    f"I couldn't find insider trade records for `{normalized_symbol}` in the last 60 days.\n"
                    "Please check the ticker and try again (for example: `AAPL`, `MSFT`, `NVDA`)."
                )
            else:
                await _send(interaction, "I couldn't find insider purchase transactions in the last 5 days.")
        elif status == 422:
            await _send(
                interaction,
                "That ticker format looks invalid. Please use a valid stock symbol (for example: `AAPL`)."
            )
        else:
            short_detail = detail[:220] if detail else "Unexpected upstream error."
            await _send(
                interaction,
                f"Couldn't fetch insider trades for `{normalized_symbol or 'recent scan'}` right now.\n{short_detail}"
            )
    except httpx.RequestError as e:
        await _send(
            interaction,
            f"Failed to fetch insider trades for {normalized_symbol or 'recent scan'}: could not reach strategy-engine. {e}"
        )
    except Exception as e:
        await _send(interaction, f"Failed to fetch insider trades for {normalized_symbol or 'recent scan'}: {e}")


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
