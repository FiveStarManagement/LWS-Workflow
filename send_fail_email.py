import os
import sys
import smtplib
from email.message import EmailMessage
from datetime import datetime

# ==============================
# Office365 SMTP Configuration
# ==============================
EMAIL_CONFIG = {
    "smtp_server": os.getenv("SMTP_SERVER", "smtp.office365.com"),
    "smtp_port": int(os.getenv("SMTP_PORT", 587)),
    "smtp_username": os.getenv("SMTP_USERNAME", "smtpo365@fivestarmanagement.com"),
    "smtp_password": os.getenv("SMTP_PASSWORD", "00FSM@email00"),
    "from_addr": os.getenv("FROM_EMAIL", "smtpo365@fivestarmanagement.com"),
}

ADMIN_EMAILS = [
    e.strip()
    for e in os.getenv(
        "ADMIN_EMAILS",
        "ukalidas@fivestarmanagement.com, mbravo@fivestarmanagement.com"
    ).split(",")
    if e.strip()
]

# ✅ Marker sets (support all scripts)
# Each entry: (start_marker, end_marker, subject_label)
MARKER_SETS = [
    ("===== RUN START:", "===== RUN END:", "LWS Workflow Scheduler"),
    ("===== ADMIN START:", "===== ADMIN EXIT:", "LWS Admin Server"),
    ("===== HOLD SUMMARY START:", "===== HOLD SUMMARY END:", "LWS Daily HOLD Summary"),
]


def extract_last_marked_block(log_file: str, max_lines: int = 2000):
    """
    Returns: (chunk_text, subject_label)
    Extract the most recent block from any known marker set.
    If no markers are found, returns (last 200 lines, "Workflow").
    """
    try:
        with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except Exception as e:
        return f"(Could not read log file: {e})", "Workflow"

    if not lines:
        return "(Log file empty)", "Workflow"

    # Find the most recent START marker among all marker sets
    best_start_idx = None
    best_marker_set = None

    for start_marker, end_marker, label in MARKER_SETS:
        for i in range(len(lines) - 1, -1, -1):
            if start_marker in lines[i]:
                if best_start_idx is None or i > best_start_idx:
                    best_start_idx = i
                    best_marker_set = (start_marker, end_marker, label)
                break  # stop scanning once we find last start for that set

    if best_start_idx is None:
        # No markers found; fallback to last 200 lines
        return "".join(lines[-200:]), "Workflow"

    start_marker, end_marker, label = best_marker_set

    # Find the first END marker after that START marker
    end_idx = None
    for j in range(best_start_idx, len(lines)):
        if end_marker in lines[j]:
            end_idx = j
            break

    if end_idx is None:
        end_idx = len(lines) - 1

    chunk_lines = lines[best_start_idx:end_idx + 1]

    # Limit email size
    if len(chunk_lines) > max_lines:
        chunk_lines = chunk_lines[-max_lines:]

    return "".join(chunk_lines), label


def send_fail_email(log_file: str, exit_code: str):
    hostname = os.environ.get("COMPUTERNAME", "UnknownHost")
    username = os.environ.get("USERNAME", "UnknownUser")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ✅ Extract last block + determine what failed
    chunk, label = extract_last_marked_block(log_file)

    subject = f"🚨 {label} FAILED (Exit={exit_code})"
    body = f"""
{label} FAILED 🚨

Time: {now}
Host: {hostname}
User: {username}
Exit Code: {exit_code}

Log File:
{log_file}

--- Last Run Block Output ---
{chunk}
"""

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL_CONFIG["from_addr"]
    msg["To"] = ", ".join(ADMIN_EMAILS)
    msg.set_content(body)

    with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_CONFIG["smtp_username"], EMAIL_CONFIG["smtp_password"])
        smtp.send_message(msg)


if __name__ == "__main__":
    # Usage: send_fail_email.py <logfile> <exit_code>
    log_file = sys.argv[1] if len(sys.argv) > 1 else ""
    exit_code = sys.argv[2] if len(sys.argv) > 2 else "unknown"

    if not log_file:
        print("ERROR: log file not provided")
        sys.exit(2)

    send_fail_email(log_file, exit_code)
    print(f"Fail email sent to ADMIN_EMAILS. ExitCode={exit_code}")
