# core/email_alerts.py
import os
import ssl
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
TO_EMAIL = os.getenv("TO_EMAIL")

SUBJECT_DEFAULT = "Option Pro – Ranked Ideas"


# ------------------------- Public API ------------------------- #
def send_email_alert(content: Any, subject: str | None = None) -> None:
    """
    Send a nicely formatted email.

    Args:
        content:
            - dict shaped like:
              {"tier1":[...], "tier2":[...], "watch":[...], "all":[...], "logs":[...]}
            - or a list of trade dicts
        subject: optional email subject. Defaults to SUBJECT_DEFAULT.

    Reads SMTP creds from env: EMAIL_ADDRESS, EMAIL_PASSWORD, TO_EMAIL
    """
    subj = subject or SUBJECT_DEFAULT
    body = _format_body(content)

    _send_email(
        from_addr=EMAIL_ADDRESS,
        to_addr=TO_EMAIL,
        subject=subj,
        body=body,
    )


# ------------------------- Helpers ------------------------- #
def _format_body(content: Any) -> str:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # 1) ranked dict shape
    if isinstance(content, dict) and any(k in content for k in ("tier1", "tier2", "watch", "all", "logs")):
        tier1 = content.get("tier1", []) or []
        tier2 = content.get("tier2", []) or []
        watch = content.get("watch", []) or []
        allc  = content.get("all", []) or []
        logs  = content.get("logs", []) or []

        lines = []
        lines.append(f"Option Pro – Ranked Ideas\n{ts}\n")
        lines.append(_section("Tier 1 (High Conviction)", tier1))
        lines.append(_section("Tier 2 (Moderate)", tier2))
        lines.append(_section("Watchlist (Top Fallback)", watch))

        # Optional: include full ranked list for traceability
        if allc:
            lines.append(_section("All (ranked)", allc, limit=None))

        # Logs / reasons
        if logs:
            lines.append("Debug / Logs")
            for l in logs:
                lines.append(f"- {l}")
            lines.append("")

        return "\n".join(lines).strip()

    # 2) plain list of trades
    if isinstance(content, list):
        lines = []
        lines.append(f"Option Pro – Trade Ideas\n{ts}\n")
        lines.append(_section("Trades", content, limit=None))
        return "\n".join(lines).strip()

    # 3) anything else – stringify
    return f"Option Pro – Output\n{ts}\n\n{str(content)}"


def _section(title: str, trades: List[Dict[str, Any]], limit: int | None = 10) -> str:
    """Format a section with up to `limit` trades (None = no limit)."""
    lines = [title]
    if not trades:
        lines.append("None\n")
        return "\n".join(lines)

    count = 0
    for t in trades:
        if limit is not None and count >= limit:
            lines.append(f"... and {len(trades) - limit} more")
            break
        lines.append(_fmt_trade_line(t))
        count += 1
    lines.append("")  # blank line after section
    return "\n".join(lines)


def _fmt_trade_line(t: Dict[str, Any]) -> str:
    """Make a single compact bullet for one option contract."""
    # safe getters
    sym   = t.get("symbol") or t.get("Ticker") or t.get("ticker") or "?"
    typ   = (t.get("type") or t.get("Type") or "?").upper()
    strike= t.get("strike") or t.get("Strike") or "?"
    exp   = t.get("expiry") or t.get("Expiration") or t.get("expiry_date") or "?"
    mid   = t.get("mid") or t.get("price") or t.get("Buy Price") or t.get("entry") or "?"

    # optional metrics
    iv    = t.get("iv")
    delta = t.get("delta")
    exp_roi = t.get("exp_roi") or t.get("score")

    bits = [
        f"[{sym}] {typ} {strike} @ {mid}",
        f"exp {exp}",
    ]
    if iv is not None:
        bits.append(f"IV {round_float(iv)}")
    if delta is not None:
        bits.append(f"Δ {round_float(delta)}")
    if exp_roi is not None:
        bits.append(f"ROI≈{round_float(exp_roi)}%")

    return "- " + " · ".join(bits)


def round_float(x: Any, n: int = 2) -> Any:
    try:
        return round(float(x), n)
    except Exception:
        return x


def _send_email(from_addr: str | None, to_addr: str | None, subject: str, body: str) -> None:
    if not from_addr or not to_addr or not EMAIL_PASSWORD:
        # Print instead of hard failing so the action log shows the message.
        print("⚠ Email credentials missing – printing message instead:")
        print(f"Subject: {subject}\n\n{body}")
        return

    # Build message
    msg = MIMEMultipart("alternative")
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject

    # Plain text part (always include)
    text_part = MIMEText(body, "plain")
    msg.attach(text_part)

    # Try a minimal HTML part for better readability in email clients
    html_body = "<br>".join(body.splitlines())
    html_part = MIMEText(f"<pre style='font-family: ui-monospace, SFMono-Regular, Menlo, monospace;'>{html_body}</pre>", "html")
    msg.attach(html_part)

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(from_addr, EMAIL_PASSWORD)
            server.send_message(msg)
        print("✅ Email sent.")
    except Exception as e:
        # Fail soft and dump content to logs for debugging.
        print(f"❌ Email send failed: {e}")
        print("—— Email content (fallback) ——")
        print(f"Subject: {subject}\n\n{body}")