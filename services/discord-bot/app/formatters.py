def fmt_compact(value):
    if value is None:
        return "n/a"

    try:
        num = float(value)
        abs_num = abs(num)

        if abs_num >= 1_000_000_000_000:
            return f"{num/1_000_000_000_000:.2f}T"
        if abs_num >= 1_000_000_000:
            return f"{num/1_000_000_000:.2f}B"
        if abs_num >= 1_000_000:
            return f"{num/1_000_000:.2f}M"
        if abs_num >= 1_000:
            return f"{num/1_000:.2f}K"

        if num.is_integer():
            return f"{int(num)}"
        return f"{num:.2f}"

    except Exception:
        return str(value)


def fmt_price(value, currency="$"):
    if value is None:
        return "n/a"

    try:
        return f"{currency}{float(value):,.2f}"
    except Exception:
        return str(value)


def fmt_percent(value):
    if value is None:
        return "n/a"

    try:
        return f"{float(value):.2f}%"
    except Exception:
        return str(value)


def fmt_change(change, pct=None):
    if change is None and pct is None:
        return "n/a"

    try:
        if change is not None and pct is not None:
            return f"{float(change):+.2f} ({float(pct):+.2f}%)"
        if change is not None:
            return f"{float(change):+.2f}"
        return f"{float(pct):+.2f}%"
    except Exception:
        if pct is not None:
            return f"{change} ({pct})"
        return str(change)


def fmt_signed_compact(value):
    if value is None:
        return "n/a"

    try:
        num = float(value)
        sign = "+" if num > 0 else ""
        return f"{sign}{fmt_compact(num)}"
    except Exception:
        return str(value)


def fmt_range(low, high, price=False, currency="$"):
    if low is None or high is None:
        return "n/a"

    try:
        if price:
            return f"{currency}{float(low):,.2f} - {currency}{float(high):,.2f}"
        return f"{float(low):,.2f} - {float(high):,.2f}"
    except Exception:
        return f"{low} - {high}"
