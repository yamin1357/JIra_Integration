#!/usr/bin/env python3
import sqlite3
import re
import json
import logging
from flask import Flask, request, jsonify
import requests
from typing import Optional, Tuple, Dict, Any
from io import BytesIO

# ----------------------
# Configuration - replace with your actual Jira URLs and tokens
# ----------------------
JIRA_A = { "url": "http://192.168.43.132:8081", "token": "ND****************************************ap", "project_key": "TEST" }
JIRA_B = { "url": "http://192.168.43.134:8081", "token": "Nj****************************************qW", "project_key": "TEST" }

SYNC_TAG_A_TO_B = "[SyncedFromA]"
SYNC_TAG_B_TO_A = "[SyncedFromB]"
DB_PATH = "./jira_sync_mappings.db"
REQUEST_TIMEOUT = 20  # افزایش برای دانلود/آپلود فایل

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
app = Flask(__name__)

# ----------------------
# SQLite helpers
# ----------------------
def ensure_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS issue_map (
        jira_a_key TEXT UNIQUE,
        jira_b_key TEXT UNIQUE,
        PRIMARY KEY(jira_a_key, jira_b_key)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS attachment_map (
        source_jira TEXT,
        source_issue_key TEXT,
        source_attachment_id TEXT,
        dest_jira TEXT,
        dest_issue_key TEXT,
        dest_attachment_id TEXT,
        PRIMARY KEY (source_jira, source_attachment_id)
    )""")
    con.commit()
    con.close()

def map_issue(jira_a_key: str, jira_b_key: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO issue_map (jira_a_key, jira_b_key) VALUES (?, ?)" , (jira_a_key, jira_b_key))
    con.commit()
    con.close()

def find_mapped_to_b(jira_a_key: str) -> Optional[str]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT jira_b_key FROM issue_map WHERE jira_a_key = ?", (jira_a_key,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else None

def find_mapped_to_a(jira_b_key: str) -> Optional[str]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT jira_a_key FROM issue_map WHERE jira_b_key = ?", (jira_b_key,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else None

def map_attachment(source_jira: str, source_issue_key: str, source_attachment_id: str,
                   dest_jira: str, dest_issue_key: str, dest_attachment_id: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO attachment_map
        (source_jira, source_issue_key, source_attachment_id, dest_jira, dest_issue_key, dest_attachment_id)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (source_jira, source_issue_key, source_attachment_id, dest_jira, dest_issue_key, dest_attachment_id))
    con.commit()
    con.close()

def find_mapped_attachment(source_jira: str, source_attachment_id: str) -> Optional[Tuple[str,str,str]]:
    """return (dest_jira, dest_issue_key, dest_attachment_id) or None"""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT dest_jira, dest_issue_key, dest_attachment_id
        FROM attachment_map
        WHERE source_jira = ? AND source_attachment_id = ?
    """, (source_jira, source_attachment_id))
    row = cur.fetchone()
    con.close()
    return (row[0], row[1], row[2]) if row else None

ensure_db()

# ----------------------
# Jira API helper
# ----------------------
def jira_api(jira: dict, method: str, endpoint: str, data=None, params=None, files=None, headers_extra=None):
    headers = { "Authorization": f"Bearer {jira['token']}" }
    if files is None:
        headers["Content-Type"] = "application/json"
    if headers_extra:
        headers.update(headers_extra)
    url = f"{jira['url'].rstrip('/')}/rest/api/2/{endpoint.lstrip('/')}"
    resp = requests.request(method, url, headers=headers, json=data if files is None else None,
                             params=params, files=files, timeout=REQUEST_TIMEOUT)
    return resp

# ----------------------
# Helpers
# ----------------------
def is_synced_from_a(text: Optional[str]) -> bool:
    return bool(text and SYNC_TAG_A_TO_B in text)

def is_synced_from_b(text: Optional[str]) -> bool:
    return bool(text and SYNC_TAG_B_TO_A in text)

def append_sync_tag(body: str, tag: str) -> str:
    if not body:
        body = ""
    if tag in body:
        return body
    return body + "\n\n" + tag

# ----------------------
# Core handlers (issue/comment/worklog)
# ----------------------
def handle_issue_created(payload: dict, source: dict, dest: dict, direction: str):
    issue = payload.get("issue", {}) or {}
    issue_key = issue.get("key")
    fields = issue.get("fields", {}) or {}
    summary = fields.get("summary", "") or ""
    description = fields.get("description", "") or ""

    # Check mapping
    if direction == "AtoB":
        dest_key = find_mapped_to_b(issue_key)
    else:
        dest_key = find_mapped_to_a(issue_key)

    if dest_key:
        return {"status": "already_mapped", "dest_key": dest_key}

    # create issue on destination (don't mutate summary)
    data = {
        "fields": {
            "project": {"key": dest['project_key']},
            "summary": summary,
            "description": description + f"\n\n(Synced from {direction[0]})",
            "issuetype": {"name": "Task"}  # ensure this exists on destination
        }
    }
    res = jira_api(dest, "POST", "issue", data)
    if res.status_code in (200,201):
        new_key = res.json().get("key")
        # save mapping
        if direction=="AtoB":
            map_issue(issue_key, new_key)
        else:
            map_issue(new_key, issue_key)
        logging.info("[%s] Created issue %s -> %s", direction, issue_key, new_key)
        return {"status":"created","dest_key":new_key}
    else:
        logging.error("[%s] Failed to create issue %s: %s", direction, issue_key, res.text)
        return {"status":"error","text":res.text}

def handle_comment_event(payload: dict, source: dict, dest: dict, direction: str):
    issue_key = (payload.get("issue") or {}).get("key")
    comment = payload.get("comment") or {}
    comment_body = comment.get("body", "") or ""
    if direction=="AtoB" and is_synced_from_b(comment_body):
        return {"status":"ignored"}
    if direction=="BtoA" and is_synced_from_a(comment_body):
        return {"status":"ignored"}

    dest_key = find_mapped_to_b(issue_key) if direction=="AtoB" else find_mapped_to_a(issue_key)
    if not dest_key:
        return {"status":"no_mapping"}
    tag = SYNC_TAG_A_TO_B if direction=="AtoB" else SYNC_TAG_B_TO_A
    new_body = append_sync_tag(comment_body, tag)
    res = jira_api(dest, "POST", f"issue/{dest_key}/comment", {"body": new_body})
    return {"status":"ok","code":res.status_code}

def handle_worklog_event(payload: dict, source: dict, dest: dict, direction: str):
    issue_key = (payload.get("issue") or {}).get("key")
    worklog = payload.get("worklog") or {}
    comment = worklog.get("comment", "") or ""
    time_spent = worklog.get("timeSpent") or worklog.get("timeSpentSeconds")
    if direction=="AtoB" and is_synced_from_b(comment):
        return {"status":"ignored"}
    if direction=="BtoA" and is_synced_from_a(comment):
        return {"status":"ignored"}

    dest_key = find_mapped_to_b(issue_key) if direction=="AtoB" else find_mapped_to_a(issue_key)
    if not dest_key:
        return {"status":"no_mapping"}
    tag = SYNC_TAG_A_TO_B if direction=="AtoB" else SYNC_TAG_B_TO_A
    comment_with_tag = append_sync_tag(comment, tag)
    data = {"comment": comment_with_tag}
    # prefer timeSpentSeconds if numeric
    if isinstance(time_spent, int):
        data["timeSpentSeconds"] = time_spent
    else:
        data["timeSpent"] = time_spent or "0s"
    res = jira_api(dest, "POST", f"issue/{dest_key}/worklog", data)
    return {"status":"ok","code":res.status_code}

# ----------------------
# Attachment handler
# ----------------------
def handle_attachment_created(payload: dict, source: dict, dest: dict, direction: str):
    """
    payload: webhook payload from Jira for attachment_created
    source/dest: jira dicts
    direction: "AtoB" or "BtoA"
    """
    issue = payload.get("issue") or {}
    issue_key = issue.get("key")
    # Jira webhook may include 'attachment' or 'attachments' depending on config
    attachment = payload.get("attachment") or None
    if not attachment:
        # try 'attachments' array
        attachments = payload.get("attachments") or payload.get("issue", {}).get("fields", {}).get("attachment") or []
        if isinstance(attachments, list) and len(attachments) > 0:
            attachment = attachments[-1]  # usually the last one is newly created
    if not attachment:
        return {"status":"ignored","reason":"no-attachment-in-payload"}

    attachment_id = str(attachment.get("id"))
    filename = attachment.get("filename") or attachment.get("name") or "attachment"
    content_url = attachment.get("content")  # absolute URL to download with auth

    if not content_url:
        return {"status":"ignored","reason":"no-content-url"}

    # check mapping — if this source attachment already mapped to a dest, ignore (prevents loop)
    source_jira_url = source['url'].rstrip('/')
    mapped = find_mapped_attachment(source_jira_url, attachment_id)
    if mapped:
        logging.info("[Attachment] already mapped: %s (source %s)", attachment_id, source_jira_url)
        return {"status":"ignored","reason":"already_mapped"}

    # find destination issue key mapping
    dest_issue_key = find_mapped_to_b(issue_key) if direction=="AtoB" else find_mapped_to_a(issue_key)
    if not dest_issue_key:
        logging.info("[Attachment] no issue mapping for %s -> cannot upload", issue_key)
        return {"status":"no_mapping"}

    # download attachment bytes from source (with auth)
    try:
        headers = {"Authorization": f"Bearer {source['token']}"}
        r = requests.get(content_url, headers=headers, stream=True, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        file_bytes = r.content
    except Exception as e:
        logging.error("[Attachment] download failed: %s", e)
        return {"status":"error","reason":"download_failed","error": str(e)}

    # upload to destination
    files = {"file": (filename, BytesIO(file_bytes))}
    headers_extra = {"X-Atlassian-Token": "no-check"}  # required for attachments
    try:
        res = jira_api(dest, "POST", f"issue/{dest_issue_key}/attachments", files=files, headers_extra=headers_extra)
        if res.status_code in (200,201):
            # response is usually a JSON array of uploaded attachments
            try:
                uploaded = res.json()
                if isinstance(uploaded, list) and len(uploaded) > 0:
                    dest_attachment_id = str(uploaded[0].get("id"))
                elif isinstance(uploaded, dict):
                    dest_attachment_id = str(uploaded.get("id") or uploaded.get("attachments", [{}])[0].get("id"))
                else:
                    dest_attachment_id = ""
            except Exception:
                dest_attachment_id = ""
            # map it (so we won't re-upload this attachment if dest also emits event)
            map_attachment(source_jira_url, issue_key, attachment_id,
                           dest['url'].rstrip('/'), dest_issue_key, dest_attachment_id)
            logging.info("[Attachment] Uploaded %s -> %s (dest attach id=%s)", filename, dest_issue_key, dest_attachment_id)
            return {"status":"uploaded","code":res.status_code,"dest_attachment_id":dest_attachment_id}
        else:
            logging.error("[Attachment] upload failed: %s", res.text)
            return {"status":"error","code":res.status_code,"text":res.text}
    except Exception as e:
        logging.error("[Attachment] upload exception: %s", e)
        return {"status":"error","exception":str(e)}

# ----------------------
# Webhooks
# ----------------------
@app.route("/webhook/A-to-B", methods=["POST"])
def webhook_a_to_b():
    payload = request.get_json(silent=True) or {}
    event = payload.get("webhookEvent", "")
    logging.info("[A->B] Received event: %s", event)

    # attachments
    if isinstance(event, str) and event.startswith("attachment_"):
        # we only handle created for now (attachment_created)
        if event == "attachment_created" or event.startswith("attachment_created"):
            return jsonify(handle_attachment_created(payload, JIRA_A, JIRA_B, "AtoB"))
        return jsonify({"status":"ignored"})

    # comment
    if isinstance(event, str) and event.startswith("comment_"):
        return jsonify(handle_comment_event(payload, JIRA_A, JIRA_B, "AtoB"))

    # worklog
    if isinstance(event, str) and event.startswith("worklog_"):
        return jsonify(handle_worklog_event(payload, JIRA_A, JIRA_B, "AtoB"))

    # issue created -> create on dest
    if event == "jira:issue_created":
        return jsonify(handle_issue_created(payload, JIRA_A, JIRA_B, "AtoB"))

    # ignore issue_updated by default
    return jsonify({"status":"ignored"})

@app.route("/webhook/B-to-A", methods=["POST"])
def webhook_b_to_a():
    payload = request.get_json(silent=True) or {}
    event = payload.get("webhookEvent", "")
    logging.info("[B->A] Received event: %s", event)

    # attachments
    if isinstance(event, str) and event.startswith("attachment_"):
        if event == "attachment_created" or event.startswith("attachment_created"):
            return jsonify(handle_attachment_created(payload, JIRA_B, JIRA_A, "BtoA"))
        return jsonify({"status":"ignored"})

    # comment
    if isinstance(event, str) and event.startswith("comment_"):
        return jsonify(handle_comment_event(payload, JIRA_B, JIRA_A, "BtoA"))

    # worklog
    if isinstance(event, str) and event.startswith("worklog_"):
        return jsonify(handle_worklog_event(payload, JIRA_B, JIRA_A, "BtoA"))

    # issue created -> create on dest
    if event == "jira:issue_created":
        return jsonify(handle_issue_created(payload, JIRA_B, JIRA_A, "BtoA"))

    return jsonify({"status":"ignored"})

# ----------------------
# Health
# ----------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status":"ok"})

# ----------------------
# Main
# ----------------------
if __name__ == "__main__":
    # NOTE: ensure port 5000 is open on the host (especially on Jira A machine if Jira will call A->B)
    app.run(host="0.0.0.0", port=5000, debug=False)