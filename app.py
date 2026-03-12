from flask import Flask, request, jsonify, render_template
from db_connect import get_conn
import pyodbc
import os
import json
import re
from datetime import datetime, timedelta



app = Flask(__name__)


def _ensure_courtroom_meta_table(cursor):
    cursor.execute("""
        IF OBJECT_ID('dbo.courtroom_meta', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.courtroom_meta (
                assignment_date DATE NOT NULL,
                courthouse NVARCHAR(100) NOT NULL,
                location_detail NVARCHAR(100) NOT NULL,
                part NVARCHAR(100) NOT NULL DEFAULT '',
                start_time NVARCHAR(16) NULL,
                restart_time NVARCHAR(16) NULL,
                adjourned_time NVARCHAR(16) NULL,
                is_down BIT NOT NULL DEFAULT 0,
                is_high_profile BIT NOT NULL DEFAULT 0,
                updated_at DATETIME NOT NULL DEFAULT GETDATE(),
                CONSTRAINT PK_courtroom_meta PRIMARY KEY (assignment_date, courthouse, location_detail, part)
            )
        END

        IF COL_LENGTH('dbo.courtroom_meta', 'is_high_profile') IS NULL
        BEGIN
            ALTER TABLE dbo.courtroom_meta ADD is_high_profile BIT NOT NULL CONSTRAINT DF_courtroom_meta_is_high_profile DEFAULT 0;
        END
    """)


def _normalize_status_type(status_type):
    if not status_type:
        return None
    normalized = status_type.strip().lower()
    if normalized in ["scheduled leave", "leave"]:
        return "Scheduled Leave"
    if normalized in ["unscheduled", "unscheduled leave", "callout sick", "unscheduled leave or callout sick", "sick"]:
        return "Unscheduled Leave"
    if normalized in ["training"]:
        return "Training"
    if normalized in ["unavailable"]:
        return "Unavailable"
    return status_type


def _parse_date_value(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _previous_weekday(target_date):
    cursor_date = target_date - timedelta(days=1)
    while cursor_date.weekday() >= 5:
        cursor_date -= timedelta(days=1)
    return cursor_date


def _parse_status_payload(status_text):
    if not status_text:
        return {"legacy": None, "ranges": [], "weekly_unavailable": []}

    try:
        parsed = json.loads(status_text)
        if isinstance(parsed, dict) and "ranges" in parsed:
            return {
                "legacy": parsed.get("legacy"),
                "ranges": parsed.get("ranges") or [],
                "weekly_unavailable": parsed.get("weekly_unavailable") or []
            }
    except (json.JSONDecodeError, TypeError):
        pass

    return {"legacy": status_text, "ranges": [], "weekly_unavailable": []}


def _effective_status_for_date(status_text, target_date):
    payload = _parse_status_payload(status_text)

    if target_date:
        target = _parse_date_value(target_date)
        active_statuses = []

        for status_range in payload.get("ranges", []):
            start = _parse_date_value(status_range.get("start_date"))
            end = _parse_date_value(status_range.get("end_date"))
            status_value = status_range.get("status")

            if not (start and end and status_value):
                continue

            if start <= target <= end and status_value not in active_statuses:
                active_statuses.append(status_value)

        weekday_name = target.strftime("%A") if target else None
        for rule in payload.get("weekly_unavailable", []):
            rule_day = (rule.get("day") or "").strip()
            rule_end = _parse_date_value(rule.get("end_date"))
            if not rule_day or not rule_end or not weekday_name:
                continue
            if target <= rule_end and (rule_day == "Any Day" or rule_day == weekday_name):
                if "Unavailable" not in active_statuses:
                    active_statuses.append("Unavailable")

        if active_statuses:
            return " / ".join(active_statuses)

    return payload.get("legacy")


def _is_off_for_assignment(status_text, target_date):
    effective_status = _effective_status_for_date(status_text, target_date)
    if not effective_status:
        return False

    normalized = effective_status.strip().lower()
    off_indicators = [
        "scheduled leave",
        "unscheduled leave",
        "leave",
        "unscheduled",
        "unavailable",
        "training",
        "sick",
        "vacation",
        "pto",
        "fmla",
        "holiday",
    ]
    return any(indicator in normalized for indicator in off_indicators)


def _serialize_status_payload(payload):
    if not payload.get("ranges") and not payload.get("legacy") and not payload.get("weekly_unavailable"):
        return None
    return json.dumps(payload)


def _parse_roster_name(full_name):
    if not full_name or "," not in full_name:
        return None

    parts = [p.strip() for p in full_name.split(",", 1)]
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None

    return {"last_name": parts[0], "first_name": parts[1]}


def _normalize_email_name_part(value):
    lowered = (value or "").lower()
    no_spaces = "".join(lowered.split())
    return "".join(ch for ch in no_spaces if ch.isalnum() or ch == "-")


def _build_baltimore_email(full_name):
    parsed = _parse_roster_name(full_name)
    if not parsed:
        return None

    first_name = _normalize_email_name_part(parsed["first_name"].split(" ")[0])
    last_name = _normalize_email_name_part(parsed["last_name"].split(" ")[0])
    if not first_name or not last_name:
        return None

    return f"{first_name}.{last_name}@baltimorecity.gov"


def _normalize_capacity_tag_key(value):
    return "".join(ch for ch in (value or "").upper() if ch.isalnum())


def _canonical_capacity_tag(value):
    raw = (value or "").strip()
    if not raw:
        return None

    normalized_key = _normalize_capacity_tag_key(raw)

    direct_options = {
        _normalize_capacity_tag_key("DEPUTY"): "DEPUTY",
        _normalize_capacity_tag_key("CSO/SPO-A: Court Trained (FT)"): "CSO/SPO-A: Court Trained (FT)",
        _normalize_capacity_tag_key("CSO/SPO-A: Court Trained (contractor)"): "CSO/SPO-A: Court Trained (contractor)",
        _normalize_capacity_tag_key("CSO/SPO-A: (FT)"): "CSO/SPO-A: (FT)",
        _normalize_capacity_tag_key("CSO/SPO-A: (contractor)"): "CSO/SPO-A: (contractor)",
        _normalize_capacity_tag_key("CSO/SPO: Court Trained(FT)"): "CSO/SPO: Court Trained(FT)",
        _normalize_capacity_tag_key("CSO/SPO Court Trained (contractor)"): "CSO/SPO Court Trained (contractor)",
        _normalize_capacity_tag_key("CSO: Non-SPO/CR"): "CSO: Non-SPO/CR",
        _normalize_capacity_tag_key("Cadet"): "Cadet",
    }

    if normalized_key in direct_options:
        return direct_options[normalized_key]

    return raw


def _ensure_deputy_transfers_table(cursor):
    cursor.execute("""
        IF OBJECT_ID('dbo.deputy_transfers', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.deputy_transfers (
                assignment_date DATE NOT NULL,
                full_name NVARCHAR(255) NOT NULL,
                transfer_out_time DATETIME NULL,
                transfer_in_time DATETIME NULL,
                transfer_history NVARCHAR(MAX) NULL,
                CONSTRAINT PK_deputy_transfers PRIMARY KEY (assignment_date, full_name)
            )
        END

        IF COL_LENGTH('dbo.deputy_transfers', 'transfer_history') IS NULL
        BEGIN
            ALTER TABLE dbo.deputy_transfers ADD transfer_history NVARCHAR(MAX) NULL;
        END
    """)


def _default_time_label():
    return datetime.now().strftime("%I:%M %p").lstrip("0")


def _normalize_time_label(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for pattern in ["%I:%M:%S %p", "%I:%M %p", "%I %p", "%H:%M:%S", "%H:%M", "%H%M"]:
        try:
            parsed = datetime.strptime(text.upper(), pattern)
            if parsed.second:
                return parsed.strftime("%I:%M:%S %p").lstrip("0")
            return parsed.strftime("%I:%M %p").lstrip("0")
        except ValueError:
            continue
    return text


def _parse_time_label(value):
    normalized = _normalize_time_label(value)
    if not normalized:
        return None
    for pattern in ["%I:%M:%S %p", "%I:%M %p"]:
        try:
            return datetime.strptime(normalized.upper(), pattern)
        except ValueError:
            continue
    return None


def _format_time_label(dt_value):
    if not dt_value:
        return None
    if dt_value.second:
        return dt_value.strftime("%I:%M:%S %p").lstrip("0")
    return dt_value.strftime("%I:%M %p").lstrip("0")


def _safe_transfer_history_load(raw_history, out_dt=None, in_dt=None):
    history = []
    if raw_history:
        try:
            parsed = json.loads(raw_history)
            if isinstance(parsed, list):
                history = [
                    {
                        "out": _normalize_time_label((entry or {}).get("out")),
                        "in": _normalize_time_label((entry or {}).get("in"))
                    }
                    for entry in parsed if isinstance(entry, dict)
                ]
        except (json.JSONDecodeError, TypeError):
            history = []

    if not history and out_dt:
        history.append({
            "out": out_dt.strftime("%I:%M %p").lstrip("0"),
            "in": in_dt.strftime("%I:%M %p").lstrip("0") if in_dt else None
        })

    return history[:3]


@app.route("/api/transfers")
def get_transfers():
    assignment_date = request.args.get("date")
    if not assignment_date:
        return jsonify([])

    conn = get_conn()
    cursor = conn.cursor()
    _ensure_deputy_transfers_table(cursor)
    cursor.execute("""
        SELECT full_name, transfer_out_time, transfer_in_time, transfer_history
        FROM dbo.deputy_transfers
        WHERE assignment_date = ?
    """, (assignment_date,))
    rows = cursor.fetchall()
    conn.close()

    return jsonify([
        {
            "full_name": r[0],
            "history": _safe_transfer_history_load(r[3], r[1], r[2]),
        }
        for r in rows
    ])


@app.route("/api/transfer-out", methods=["POST"])
def transfer_out():
    data = request.json or {}
    assignment_date = data.get("assignment_date")
    full_name = data.get("full_name")
    transfer_time = _normalize_time_label(data.get("transfer_time")) or _default_time_label()
    transfer_index = data.get("transfer_index")
    if not assignment_date or not full_name:
        return jsonify({"status": "error", "message": "missing assignment_date/full_name"}), 400

    conn = get_conn()
    cursor = conn.cursor()

    _ensure_deputy_transfers_table(cursor)
    cursor.execute("""
        SELECT transfer_history, transfer_out_time, transfer_in_time
        FROM dbo.deputy_transfers
        WHERE assignment_date = ? AND full_name = ?
    """, (assignment_date, full_name))
    existing = cursor.fetchone()

    history = _safe_transfer_history_load(existing[0], existing[1], existing[2]) if existing else []

    target_index = None
    try:
        if transfer_index is not None:
            target_index = int(transfer_index) - 1
    except (TypeError, ValueError):
        target_index = None

    if target_index is not None and 0 <= target_index < len(history):
        history[target_index]["out"] = transfer_time
    else:
        if len(history) >= 3:
            conn.close()
            return jsonify({"status": "error", "message": "Maximum of 3 transfers reached"}), 400
        history.append({"out": transfer_time, "in": None})
        target_index = len(history) - 1

    latest = history[target_index] if 0 <= target_index < len(history) else history[-1]

    cursor.execute("""
        MERGE dbo.deputy_transfers AS t
        USING (SELECT ? AS assignment_date, ? AS full_name) AS s
        ON t.assignment_date = s.assignment_date AND t.full_name = s.full_name
        WHEN MATCHED THEN
            UPDATE SET
                transfer_history = ?,
                transfer_out_time = NULL,
                transfer_in_time = NULL
        WHEN NOT MATCHED THEN
            INSERT (assignment_date, full_name, transfer_out_time, transfer_in_time, transfer_history)
            VALUES (?, ?, NULL, NULL, ?);
    """, (
        assignment_date,
        full_name,
        json.dumps(history),
        assignment_date,
        full_name,
        json.dumps(history),
    ))

    conn.commit()
    conn.close()
    return jsonify({
        "status": "success",
        "transfer_index": target_index + 1,
        "out_time": latest.get("out"),
        "history": history
    })


@app.route("/api/transfer-in", methods=["POST"])
def transfer_in():
    data = request.json or {}
    assignment_date = data.get("assignment_date")
    full_name = data.get("full_name")
    transfer_time = _normalize_time_label(data.get("transfer_time")) or _default_time_label()
    transfer_index = data.get("transfer_index")
    if not assignment_date or not full_name:
        return jsonify({"status": "error", "message": "missing assignment_date/full_name"}), 400

    conn = get_conn()
    cursor = conn.cursor()

    _ensure_deputy_transfers_table(cursor)
    cursor.execute("""
        SELECT transfer_history, transfer_out_time, transfer_in_time
        FROM dbo.deputy_transfers
        WHERE assignment_date = ? AND full_name = ?
    """, (assignment_date, full_name))
    existing = cursor.fetchone()

    history = _safe_transfer_history_load(existing[0], existing[1], existing[2]) if existing else []
    if not history:
        conn.close()
        return jsonify({"status": "error", "message": "No transfer-out found"}), 400

    target_index = None
    try:
        if transfer_index is not None:
            idx = int(transfer_index) - 1
            if 0 <= idx < len(history):
                target_index = idx
    except (TypeError, ValueError):
        target_index = None

    if target_index is None:
        for i in range(len(history) - 1, -1, -1):
            if history[i].get("out") and not history[i].get("in"):
                target_index = i
                break

    if target_index is None:
        conn.close()
        return jsonify({"status": "error", "message": "No open transfer-out found"}), 400

    out_time_value = _parse_time_label(history[target_index].get("out"))
    in_time_value = _parse_time_label(transfer_time)
    if out_time_value and in_time_value and in_time_value == out_time_value:
        in_time_value = in_time_value + timedelta(seconds=1)

    history[target_index]["in"] = _format_time_label(in_time_value) if in_time_value else transfer_time

    cursor.execute("""
        UPDATE dbo.deputy_transfers
        SET transfer_history = ?, transfer_out_time = NULL, transfer_in_time = NULL
        WHERE assignment_date = ? AND full_name = ?
    """, (json.dumps(history), assignment_date, full_name))

    conn.commit()
    conn.close()
    return jsonify({
        "status": "success",
        "transfer_index": target_index + 1,
        "in_time": history[target_index].get("in"),
        "history": history
    })
@app.route("/")
def index():
    return render_template("search.html")

@app.route("/simple-search")
def simple_search_page():
    return render_template("simple_search.html")
@app.route("/api/update-status", methods=["POST"])
def update_status():
    data = request.json

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT current_status
        FROM dbo.deputies
        WHERE full_name = ?
    """, (data["full_name"],))
    row = cursor.fetchone()
    current_status = row[0] if row else None
    payload = _parse_status_payload(current_status)

    payload["legacy"] = data["status"]

    cursor.execute("""
        UPDATE dbo.deputies
        SET current_status = ?
        WHERE full_name = ?
    """, (
        _serialize_status_payload(payload),
        data["full_name"]
    ))

    conn.commit()
    conn.close()

    return {"status": "success"}


@app.route("/api/update-status-range", methods=["POST"])
def update_status_range():
    data = request.json
    full_name = data.get("full_name")
    status = _normalize_status_type(data.get("status"))
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    conn = get_conn()
    cursor = conn.cursor()
    
    if status == "CLEAR_ALL":
        cursor.execute("""
            UPDATE dbo.deputies
            SET current_status = NULL
            WHERE full_name = ?
        """, (full_name,))
        
        conn.commit()
        conn.close()
        return jsonify({"status": "cleared", "removed_assignments": 0})
    

    cursor.execute("""
        SELECT current_status
        FROM dbo.deputies
        WHERE full_name = ?
    """, (full_name,))
    row = cursor.fetchone()
    current_status = row[0] if row else None
    payload = _parse_status_payload(current_status)

    ranges = payload.get("ranges", [])
    remove_only = bool(data.get("remove_only"))
    new_ranges = []

    target = _parse_date_value(start_date)

    for r in ranges:
        r_status = r.get("status")
        r_start = _parse_date_value(r.get("start_date"))
        r_end = _parse_date_value(r.get("end_date"))

        # Remove if same status AND date falls inside that range
        if (
            r_status == status and
            r_start and r_end and
            target and
            r_start <= target <= r_end
        ):
            continue

        new_ranges.append(r)

    ranges = new_ranges
    if not remove_only:
        ranges.append({
            "status": status,
            "start_date": start_date,
            "end_date": end_date
        })
    payload["ranges"] = ranges

    cursor.execute("""
        UPDATE dbo.deputies
        SET current_status = ?
        WHERE full_name = ?
    """, (
        _serialize_status_payload(payload),
        full_name
    ))
    removed_count = 0

    if status in ["Scheduled Leave", "Unscheduled Leave", "Unavailable", "Training"] and start_date and end_date:

        cursor.execute("""
            SELECT assignment_date, courthouse, assignment_type,
                location_group, location_detail, part, assigned_member
            FROM dbo.court_assignments
            WHERE assignment_date BETWEEN ? AND ?
            AND assigned_member LIKE ?
        """, (
            start_date,
            end_date,
            f"%{full_name}%"
        ))

        rows = cursor.fetchall()

        for row in rows:
            assignment_date = row[0]
            courthouse = row[1]
            assignment_type = row[2]
            location_group = row[3]
            location_detail = row[4]
            part = row[5]
            assigned_member = row[6] or ""

            names = [n.strip() for n in assigned_member.split("||") if n.strip()]
            names = [n for n in names if n != full_name]

            new_value = " || ".join(names) if names else None

            cursor.execute("""
                UPDATE dbo.court_assignments
                SET assigned_member = ?
                WHERE assignment_date = ?
                AND courthouse = ?
                AND assignment_type = ?
                AND ISNULL(location_group,'') = ISNULL(?, '')
                AND ISNULL(location_detail,'') = ISNULL(?, '')
                AND ISNULL(part,'') = ISNULL(?, '')
            """, (
                new_value,
                assignment_date,
                courthouse,
                assignment_type,
                location_group,
                location_detail,
                part
            ))

            removed_count += 1

    conn.commit()
    conn.close()

    return jsonify({
        "status": "success",
        "removed_assignments": removed_count
    })

@app.route("/api/update-unavailability", methods=["POST"])
def update_unavailability():
    data = request.json
    full_name = data.get("full_name")
    day = data.get("day")
    end_date = data.get("end_date")
    remove_all = bool(data.get("remove_all"))

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT current_status
        FROM dbo.deputies
        WHERE full_name = ?
    """, (full_name,))
    row = cursor.fetchone()
    current_status = row[0] if row else None
    payload = _parse_status_payload(current_status)

    rules = payload.get("weekly_unavailable", [])

    if remove_all:
        payload["weekly_unavailable"] = []
    else:
        rules = [r for r in rules if not (r.get("day") == day)]
        rules.append({"day": day, "end_date": end_date})
        payload["weekly_unavailable"] = rules

    cursor.execute("""
        UPDATE dbo.deputies
        SET current_status = ?
        WHERE full_name = ?
    """, (
        _serialize_status_payload(payload),
        full_name
    ))

    conn.commit()
    conn.close()

    return jsonify({"status": "success"})

@app.route("/api/upsert-deputy", methods=["POST"])
def upsert_deputy():
    data = request.json or {}
    original_full_name = data.get("original_full_name")
    full_name = data.get("full_name")
    canonical_capacity_tag = _canonical_capacity_tag(data.get("capacity_tag"))

    if not full_name:
        return jsonify({"status": "error", "message": "full_name is required"}), 400

    provided_email = (data.get("email") or "").strip() or None
    generated_email = _build_baltimore_email(full_name)
    email = provided_email or generated_email

    if not original_full_name and not email:
        return jsonify({"status": "error", "message": "full_name must be in 'Last name, First name' format"}), 400

    conn = get_conn()
    cursor = conn.cursor()

    if original_full_name:
        update_queries = [
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, email = ?, division = ?, rank = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, email, data.get("division"), data.get("rank"), canonical_capacity_tag, original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, email = ?, division = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, email, data.get("division"), canonical_capacity_tag, original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, division = ?, rank = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, data.get("division"), data.get("rank"), canonical_capacity_tag, original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, division = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, data.get("division"), canonical_capacity_tag, original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, email = ?, rank = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, email, data.get("rank"), canonical_capacity_tag, original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, rank = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, data.get("rank"), canonical_capacity_tag, original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, email = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, email, canonical_capacity_tag, original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, canonical_capacity_tag, original_full_name)),
        ]

        for query, params in update_queries:
            try:
                cursor.execute(query, params)
                break
            except (pyodbc.ProgrammingError, pyodbc.IntegrityError):
                continue
    else:
        insert_queries = [
            ("""
                INSERT INTO dbo.deputies (full_name, email, division, rank, capacity_tag, current_status)
                VALUES (?, ?, ?, ?, ?, NULL)
            """, (full_name, email, data.get("division"), data.get("rank"), canonical_capacity_tag)),
            ("""
                INSERT INTO dbo.deputies (full_name, email, division, capacity_tag, current_status)
                VALUES (?, ?, ?, ?, NULL)
            """, (full_name, email, data.get("division"), canonical_capacity_tag)),
            ("""
                INSERT INTO dbo.deputies (full_name, email, rank, capacity_tag, current_status)
                VALUES (?, ?, ?, ?, NULL)
            """, (full_name, email, data.get("rank"), canonical_capacity_tag)),
            ("""
                INSERT INTO dbo.deputies (full_name, email, capacity_tag, current_status)
                VALUES (?, ?, ?, NULL)
            """, (full_name, email, canonical_capacity_tag)),
            ("""
                INSERT INTO dbo.deputies (full_name, division, rank, capacity_tag, current_status)
                VALUES (?, ?, ?, ?, NULL)
            """, (full_name, data.get("division"), data.get("rank"), canonical_capacity_tag)),
            ("""
                INSERT INTO dbo.deputies (full_name, division, capacity_tag, current_status)
                VALUES (?, ?, ?, NULL)
            """, (full_name, data.get("division"), canonical_capacity_tag)),
            ("""
                INSERT INTO dbo.deputies (full_name, rank, capacity_tag, current_status)
                VALUES (?, ?, ?, NULL)
            """, (full_name, data.get("rank"), canonical_capacity_tag)),
            ("""
                INSERT INTO dbo.deputies (full_name, capacity_tag, current_status)
                VALUES (?, ?, NULL)
            """, (full_name, canonical_capacity_tag)),
        ]

        for query, params in insert_queries:
            try:
                cursor.execute(query, params)
                break
            except (pyodbc.ProgrammingError, pyodbc.IntegrityError):
                continue

    conn.commit()
    conn.close()

    return {"status": "success"}

@app.route("/api/delete-deputy", methods=["POST"])
def delete_deputy():
    data = request.json

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM dbo.deputies WHERE full_name = ?", (data.get("full_name"),))

    conn.commit()
    conn.close()

    return {"status": "success"}

@app.route("/staffing")
def staffing():
    return render_template("staffing.html")


@app.route("/executive-summary")
def executive_summary():
    return render_template("executive_summary.html")


@app.route("/api/import-previous-column", methods=["POST"])
def import_previous_column():
    data = request.json
    staffing_date = data["staffing_date"]
    column_name = data["column_name"]

    conn = get_conn()
    cursor = conn.cursor()

    # Find most recent previous date for this column
    cursor.execute("""
        SELECT MAX(staffing_date)
        FROM dbo.staffing_daily
        WHERE staffing_date < ?
          AND column_name = ?
    """, (staffing_date, column_name))

    row = cursor.fetchone()
    previous_date = row[0]

    if not previous_date:
        conn.close()
        return {"status": "no_previous_data"}

    # Get all rows from that previous date for this column
    cursor.execute("""
        SELECT row_number, deputy_name
        FROM dbo.staffing_daily
        WHERE staffing_date = ?
          AND column_name = ?
    """, (previous_date, column_name))

    previous_rows = cursor.fetchall()

    for r in previous_rows:
        row_number = r[0]
        deputy_name = r[1]

        cursor.execute("""
            MERGE dbo.staffing_daily AS target
            USING (SELECT ? AS staffing_date,
                          ? AS row_number,
                          ? AS column_name) AS source
            ON target.staffing_date = source.staffing_date
               AND target.row_number = source.row_number
               AND target.column_name = source.column_name

            WHEN MATCHED THEN
                UPDATE SET deputy_name = ?

            WHEN NOT MATCHED THEN
                INSERT (staffing_date, row_number, column_name, deputy_name)
                VALUES (?, ?, ?, ?);
        """, (
            staffing_date,
            row_number,
            column_name,
            deputy_name,
            staffing_date,
            row_number,
            column_name,
            deputy_name
        ))

    conn.commit()
    conn.close()

    return {"status": "success"}
@app.route("/api/get-staffing")
def get_staffing():
    staffing_date = request.args.get("date")

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT sd.row_number,
           sd.column_name,
           sd.deputy_name
    FROM dbo.staffing_daily sd
    INNER JOIN (
        SELECT row_number,
               column_name,
               MAX(staffing_date) AS max_date
        FROM dbo.staffing_daily
        WHERE staffing_date <= ?
        GROUP BY row_number, column_name
    ) latest
    ON sd.row_number = latest.row_number
       AND sd.column_name = latest.column_name
       AND sd.staffing_date = latest.max_date
""", (staffing_date,))

    rows = cursor.fetchall()
    conn.close()

    result = []

    for r in rows:
        result.append({
            "row_number": r[0],
            "column_name": r[1],
            "deputy_name": r[2]
        })

    return jsonify(result)
@app.route("/roster")
def roster_page():
    return render_template("roster.html")
@app.route("/api/update-staffing", methods=["POST"])
def update_staffing():
    data = request.json

    conn = get_conn()
    cursor = conn.cursor()

    # Upsert logic
    cursor.execute("""
        MERGE dbo.staffing_daily AS target
        USING (SELECT ? AS staffing_date,
                      ? AS row_number,
                      ? AS column_name) AS source
        ON target.staffing_date = source.staffing_date
           AND target.row_number = source.row_number
           AND target.column_name = source.column_name

        WHEN MATCHED THEN
            UPDATE SET deputy_name = ?

        WHEN NOT MATCHED THEN
            INSERT (staffing_date, row_number, column_name, deputy_name)
            VALUES (?, ?, ?, ?);
    """, (
        data["staffing_date"],
        data["row_number"],
        data["column_name"],
        data["deputy_name"],
        data["staffing_date"],
        data["row_number"],
        data["column_name"],
        data["deputy_name"]
    ))

    conn.commit()
    conn.close()

    return {"status": "success"}

@app.route("/api/deputies-available")
def get_available_deputies():
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT full_name, capacity_tag
        FROM dbo.deputies
        WHERE current_status IS NULL
        ORDER BY full_name
    """)

    deputies = [
        {
            "full_name": row[0],
            "capacity_tag": row[1]
        }
        for row in cursor.fetchall()
    ]

    conn.close()

    return jsonify(deputies)

@app.route("/api/update-assignment-notes", methods=["POST"])
def update_assignment_notes():
    data = request.json or {}

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE dbo.court_assignments
        SET assignment_notes = ?
        WHERE assignment_date = ?
          AND courthouse = ?
          AND assignment_type = ?
          AND ISNULL(location_detail, '') = ISNULL(?, '')
          AND LOWER(ISNULL(part, '')) = LOWER(ISNULL(?, ''))
    """, (
        data.get("assignment_notes"),
        data.get("assignment_date"),
        data.get("courthouse"),
        data.get("assignment_type"),
        data.get("location_detail"),
        data.get("part")
    ))

    if cursor.rowcount == 0:
        cursor.execute("""
            INSERT INTO dbo.court_assignments (
                assignment_date,
                courthouse,
                assignment_type,
                location_group,
                location_detail,
                part,
                judge_name,
                shift_time,
                assigned_member,
                assignment_notes,
                created_at
            )
            VALUES (?, ?, ?, NULL, ?, ?, NULL, NULL, NULL, ?, GETDATE())
        """, (
            data.get("assignment_date"),
            data.get("courthouse"),
            data.get("assignment_type"),
            data.get("location_detail"),
            data.get("part"),
            data.get("assignment_notes")
        ))

    conn.commit()
    conn.close()

    return {"status": "success"}

@app.route("/api/update-judge-name", methods=["POST"])
def update_judge_name():
    data = request.json or {}

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE dbo.court_assignments
        SET judge_name = ?
        WHERE assignment_date = ?
          AND courthouse = ?
          AND assignment_type = ?
          AND ISNULL(location_detail, '') = ISNULL(?, '')
          AND LOWER(ISNULL(part, '')) = LOWER(ISNULL(?, ''))
    """, (
        data.get("judge_name"),
        data.get("assignment_date"),
        data.get("courthouse"),
        data.get("assignment_type"),
        data.get("location_detail"),
        data.get("part")
    ))

    if cursor.rowcount == 0:
        cursor.execute("""
            INSERT INTO dbo.court_assignments (
                assignment_date,
                courthouse,
                assignment_type,
                location_group,
                location_detail,
                part,
                judge_name,
                shift_time,
                assigned_member,
                assignment_notes,
                created_at
            )
            VALUES (?, ?, ?, NULL, ?, ?, ?, NULL, NULL, NULL, GETDATE())
        """, (
            data.get("assignment_date"),
            data.get("courthouse"),
            data.get("assignment_type"),
            data.get("location_detail"),
            data.get("part"),
            data.get("judge_name")
        ))

    conn.commit()
    conn.close()

    return {"status": "success"}


@app.route("/api/update-shift-time", methods=["POST"])
def update_shift_time():
    data = request.json or {}
    assignment_type = data.get("assignment_type")

    conn = get_conn()
    cursor = conn.cursor()

    if assignment_type == "Fixed Post":
        cursor.execute("""
            UPDATE dbo.court_assignments
            SET shift_time = ?
            WHERE assignment_date = ?
              AND courthouse = ?
              AND assignment_type = ?
              AND ISNULL(location_group, '') = ISNULL(?, '')
              AND LOWER(ISNULL(part, '')) = LOWER(ISNULL(?, ''))
        """, (
            data.get("shift_time"),
            data.get("assignment_date"),
            data.get("courthouse"),
            assignment_type,
            data.get("location_group") or data.get("location_detail"),
            data.get("part")
        ))
    else:
        cursor.execute("""
            UPDATE dbo.court_assignments
            SET shift_time = ?
            WHERE assignment_date = ?
              AND courthouse = ?
              AND assignment_type = ?
              AND (
                    ISNULL(location_group, '') = ISNULL(?, '')
                    OR ISNULL(location_detail, '') = ISNULL(?, '')
                  )
              AND LOWER(ISNULL(part, '')) = LOWER(ISNULL(?, ''))
        """, (
            data.get("shift_time"),
            data.get("assignment_date"),
            data.get("courthouse"),
            assignment_type,
            data.get("location_group") or data.get("location_detail"),
            data.get("location_detail"),
            data.get("part")
        ))

        if cursor.rowcount == 0:
            cursor.execute("""
                UPDATE dbo.court_assignments
                SET shift_time = ?
                WHERE assignment_date = ?
                  AND courthouse = ?
                  AND assignment_type = ?
                  AND (
                        ISNULL(location_group, '') = ISNULL(?, '')
                        OR ISNULL(location_detail, '') = ISNULL(?, '')
                      )
            """, (
                data.get("shift_time"),
                data.get("assignment_date"),
                data.get("courthouse"),
                assignment_type,
                data.get("location_group") or data.get("location_detail"),
                data.get("location_detail")
            ))

    if cursor.rowcount == 0:
        if assignment_type == "Fixed Post":
            cursor.execute("""
                INSERT INTO dbo.court_assignments (
                    assignment_date,
                    courthouse,
                    assignment_type,
                    location_group,
                    location_detail,
                    part,
                    judge_name,
                    shift_time,
                    assigned_member,
                    assignment_notes,
                    created_at
                )
                VALUES (?, ?, ?, ?, NULL, ?, NULL, ?, NULL, NULL, GETDATE())
            """, (
                data.get("assignment_date"),
                data.get("courthouse"),
                assignment_type,
                data.get("location_group") or data.get("location_detail"),
                data.get("part"),
                data.get("shift_time")
            ))
        else:
            cursor.execute("""
                INSERT INTO dbo.court_assignments (
                    assignment_date,
                    courthouse,
                    assignment_type,
                    location_group,
                    location_detail,
                    part,
                    judge_name,
                    shift_time,
                    assigned_member,
                    assignment_notes,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, GETDATE())
            """, (
                data.get("assignment_date"),
                data.get("courthouse"),
                assignment_type,
                data.get("location_group") if data.get("location_group") else None,
                data.get("location_detail"),
                data.get("part"),
                data.get("shift_time")
            ))

    conn.commit()
    conn.close()

    return {"status": "success"}

@app.route("/api/deputies")
def get_deputies():
    target_date = request.args.get("date")
    conn = get_conn()
    cursor = conn.cursor()

    query_options = [
        ("""
            SELECT full_name, email, capacity_tag, current_status, division, rank
            FROM dbo.deputies
            ORDER BY full_name
        """, True, True),
        ("""
            SELECT full_name, email, capacity_tag, current_status, division
            FROM dbo.deputies
            ORDER BY full_name
        """, True, False),
        ("""
            SELECT full_name, email, capacity_tag, current_status, rank
            FROM dbo.deputies
            ORDER BY full_name
        """, False, True),
        ("""
            SELECT full_name, email, capacity_tag, current_status
            FROM dbo.deputies
            ORDER BY full_name
        """, False, False),
    ]

    rows = []
    has_division = False
    has_rank = False

    for query, query_has_division, query_has_rank in query_options:
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            has_division = query_has_division
            has_rank = query_has_rank
            break
        except pyodbc.ProgrammingError:
            continue

    deputies = []
    for row in rows:
        deputy = {
            "full_name": row[0],
            "email": row[1],
            "capacity_tag": row[2],
            "current_status": _effective_status_for_date(row[3], target_date),
            "status_raw": row[3],
            "division": None,
            "rank": None,
        }

        idx = 4
        if has_division and len(row) > idx:
            deputy["division"] = row[idx]
            idx += 1

        if has_rank and len(row) > idx:
            deputy["rank"] = row[idx]

        deputies.append(deputy)

    conn.close()

    return jsonify(deputies)

@app.route("/api/update-deputy", methods=["POST"])
def update_deputy():
    data = request.json

    conn = get_conn()
    cursor = conn.cursor()

    is_fixed_post = data.get("assignment_type") == "Fixed Post"
    is_courtroom = data.get("assignment_type") == "Courtroom"

    if is_fixed_post:
        cursor.execute("""
            UPDATE dbo.court_assignments
            SET assigned_member = ?
            WHERE assignment_date = ?
              AND courthouse = ?
              AND assignment_type = ?
              AND ISNULL(location_group, '') = ISNULL(?, '')
              AND ISNULL(part, '') = ISNULL(?, '')
        """, (
            data["assigned_member"],
            data["assignment_date"],
            data["courthouse"],
            data["assignment_type"],
            data["location_detail"],
            data.get("part")
        ))

        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO dbo.court_assignments (
                    assignment_date,
                    courthouse,
                    assignment_type,
                    location_group,
                    location_detail,
                    part,
                    judge_name,
                    shift_time,
                    assigned_member,
                    assignment_notes,
                    created_at
                )
                VALUES (?, ?, ?, ?, NULL, ?, NULL, NULL, ?, NULL, GETDATE())
            """, (
                data["assignment_date"],
                data["courthouse"],
                data["assignment_type"],
                data["location_detail"],
                data.get("part"),
                data["assigned_member"]
            ))
    elif is_courtroom:
        cursor.execute("""
            UPDATE dbo.court_assignments
            SET assigned_member = ?
            WHERE assignment_date = ?
              AND courthouse = ?
              AND assignment_type = ?
              AND ISNULL(location_detail, '') = ISNULL(?, '')
              AND ISNULL(part, '') = ISNULL(?, '')
        """, (
            data["assigned_member"],
            data["assignment_date"],
            data["courthouse"],
            data["assignment_type"],
            data["location_detail"],
            data.get("part")
        ))

        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO dbo.court_assignments (
                    assignment_date,
                    courthouse,
                    assignment_type,
                    location_group,
                    location_detail,
                    part,
                    judge_name,
                    shift_time,
                    assigned_member,
                    assignment_notes,
                    created_at
                )
                VALUES (?, ?, ?, NULL, ?, ?, NULL, NULL, ?, NULL, GETDATE())
            """, (
                data["assignment_date"],
                data["courthouse"],
                data["assignment_type"],
                data["location_detail"],
                data.get("part"),
                data["assigned_member"]
            ))
    else:
        cursor.execute("""
            UPDATE dbo.court_assignments
            SET assigned_member = ?
            WHERE assignment_date = ?
              AND courthouse = ?
              AND assignment_type = ?
              AND (
                    ISNULL(location_group, '') = ISNULL(?, '')
                    OR ISNULL(location_detail, '') = ISNULL(?, '')
                  )
              AND ISNULL(part, '') = ISNULL(?, '')
        """, (
            data["assigned_member"],
            data["assignment_date"],
            data["courthouse"],
            data["assignment_type"],
            data["location_detail"],
            data["location_detail"],
            data.get("part")
        ))

        if cursor.rowcount == 0:
            cursor.execute("""
                UPDATE dbo.court_assignments
                SET assigned_member = ?
                WHERE assignment_date = ?
                  AND courthouse = ?
                  AND assignment_type = ?
                  AND (
                        ISNULL(location_group, '') = ISNULL(?, '')
                        OR ISNULL(location_detail, '') = ISNULL(?, '')
                      )
            """, (
                data["assigned_member"],
                data["assignment_date"],
                data["courthouse"],
                data["assignment_type"],
                data["location_detail"],
                data["location_detail"]
            ))

        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO dbo.court_assignments (
                    assignment_date,
                    courthouse,
                    assignment_type,
                    location_group,
                    location_detail,
                    part,
                    judge_name,
                    shift_time,
                    assigned_member,
                    assignment_notes,
                    created_at
                )
                VALUES (?, ?, ?, NULL, ?, ?, NULL, NULL, ?, NULL, GETDATE())
            """, (
                data["assignment_date"],
                data["courthouse"],
                data["assignment_type"],
                data["location_detail"],
                data.get("part"),
                data["assigned_member"]
            ))

    conn.commit()
    conn.close()

    return {"status": "success", "updated": cursor.rowcount}


@app.route("/api/get-courtroom-meta")
def get_courtroom_meta():
    date = request.args.get("date")
    conn = get_conn()
    cursor = conn.cursor()
    _ensure_courtroom_meta_table(cursor)

    cursor.execute("""
        SELECT assignment_date, courthouse, location_detail, part, start_time, restart_time, adjourned_time, is_down, is_high_profile
        FROM dbo.courtroom_meta
        WHERE assignment_date = ?
    """, (date,))

    rows = [
        {
            "assignment_date": str(row[0]),
            "courthouse": row[1],
            "location_detail": row[2],
            "part": row[3] or "",
            "start_time": row[4] or "",
            "restart_time": row[5] or "",
            "adjourned_time": row[6] or "",
            "is_down": bool(row[7]),
            "is_high_profile": bool(row[8])
        }
        for row in cursor.fetchall()
    ]

    conn.close()
    return jsonify(rows)


def _parse_assigned_member_names(value):
    text = (value or "").strip()
    if not text:
        return []

    if "||" in text:
        return [name.strip() for name in re.split(r"\s*\|\|\s*", text) if name.strip()]

    if "\n" in text:
        return [name.strip() for name in re.split(r"\n+", text) if name.strip()]

    return [text]


def _required_deputies_for_courtroom_label(label):
    normalized = (label or "").strip().upper()
    if not normalized:
        return 0

    if normalized in {"CLOSED", "NO DEPUTY", "NO DEPUTIES", "CIVIL - NO DEPUTIES"}:
        return 0

    if normalized == "NEED 2 DEPUTIES":
        return 2

    if normalized in {"NEED 1 DEPUTY", "CIVIL - 1 DEPUTY", "JUVENILE", "FAMILY", "WAITING TO RECEIVE CASE", "OPEN"} or normalized.startswith("OPEN-"):
        return 1

    return 1


def _fixed_post_requirement_group(row):
    courthouse = (row.get("courthouse") or "").strip().lower()
    post = (row.get("location_group") or "").strip().lower()
    part = (row.get("part") or "").strip().lower()

    # Transportation is informational only and should never count toward staffing totals.
    if post == "transportation":
        return None

    # Mitchell Calvert includes an optional third slot.
    if courthouse == "mitchell" and post == "calvert" and part == "0800-3":
        return None

    # Armed requirement rules:
    # - Jury Room + St. Paul are treated as one combined armed post.
    if courthouse == "mitchell" and post in {"jury room", "st. paul"}:
        return f"{courthouse}|jury-stpaul-combined"

    # - Cummings has one armed requirement at 8:00; 8:30 slots are optional for armed coverage.
    if courthouse == "cummings" and post == "cummings":
        if part == "0800":
            return f"{courthouse}|cummings-0800"
        if part.startswith("0830"):
            return None

    # Default: each fixed post slot counts independently.
    return "|".join([
        courthouse,
        post,
        (row.get("location_detail") or "").strip().lower(),
        part,
    ])


@app.route("/api/assignment-totals")
def assignment_totals():
    date = request.args.get("date")
    if not date:
        return jsonify({"vacant": 0, "filled": 0})

    # Reuse the exact same query + dedupe behavior as /api/search
    query = """
        SELECT TOP 200
            assignment_date,
            courthouse,
            assignment_type,
            location_group,
            location_detail,
            judge_name,
            part,
            assigned_member,
            assignment_notes
        FROM dbo.court_assignments
        WHERE assignment_date = ?
        ORDER BY assignment_date DESC
    """

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(query, (date,))
    columns = [c[0] for c in cursor.description]
    raw_results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()

    # SAME dedupe as /api/search (prefer row that has assigned_member)
    deduped = {}
    for row in raw_results:
        key = (
            row.get("assignment_date"),
            (row.get("courthouse") or "").strip(),
            (row.get("assignment_type") or "").strip(),
            (row.get("location_group") or "").strip(),
            (row.get("location_detail") or "").strip(),
            (row.get("part") or "").strip(),
        )
        existing = deduped.get(key)
        if not existing:
            deduped[key] = row
            continue

        if not (existing.get("assigned_member") or "").strip() and (row.get("assigned_member") or "").strip():
            deduped[key] = row

    rows = list(deduped.values())

    # COUNT using staffing requirement rules
    vacant = filled = 0
    fixed_post_groups = {}

    for r in rows:
        typ = (r.get("assignment_type") or "").strip()
        if typ not in ("Courtroom", "Fixed Post"):
            continue

        assigned_names = _parse_assigned_member_names(r.get("assigned_member"))
        assigned_count = len(assigned_names)
        label = (r.get("assignment_notes") or "").strip().upper()

        if typ == "Fixed Post":
            group_key = _fixed_post_requirement_group(r)
            if not group_key:
                continue

            existing_assigned = fixed_post_groups.get(group_key, 0)
            if assigned_count > existing_assigned:
                fixed_post_groups[group_key] = assigned_count
            continue

        # Courtroom:
        required_deputies = _required_deputies_for_courtroom_label(label)
        if required_deputies == 0:
            continue

        if label == "OPEN" or label.startswith("OPEN-"):
            continue

        filled += min(assigned_count, required_deputies)
        vacant += max(required_deputies - assigned_count, 0)

    for assigned_count in fixed_post_groups.values():
        if assigned_count > 0:
            filled += 1
        else:
            vacant += 1

    return jsonify({"vacant": vacant, "filled": filled})

@app.route("/api/update-courtroom-meta", methods=["POST"])
def update_courtroom_meta():
    data = request.json or {}

    assignment_date = (data.get("assignment_date") or "").strip()
    courthouse = (data.get("courthouse") or "").strip()
    location_detail = (data.get("location_detail") or "").strip()

    if not assignment_date or not courthouse or not location_detail:
        return jsonify({"status": "error", "message": "assignment_date, courthouse, and location_detail are required"}), 400

    conn = get_conn()
    cursor = conn.cursor()
    _ensure_courtroom_meta_table(cursor)

    cursor.execute("""
        MERGE dbo.courtroom_meta AS target
        USING (
            SELECT ? AS assignment_date, ? AS courthouse, ? AS location_detail, ? AS part
        ) AS source
        ON target.assignment_date = source.assignment_date
           AND target.courthouse = source.courthouse
           AND target.location_detail = source.location_detail
           AND ISNULL(target.part, '') = ISNULL(source.part, '')
        WHEN MATCHED THEN
            UPDATE SET
                start_time = ?,
                restart_time = ?,
                adjourned_time = ?,
                is_down = ?,
                is_high_profile = ?,
                updated_at = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (assignment_date, courthouse, location_detail, part, start_time, restart_time, adjourned_time, is_down, is_high_profile, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
    """, (
        assignment_date,
        courthouse,
        location_detail,
        data.get("part") or "",
        data.get("start_time") or None,
        data.get("restart_time") or None,
        data.get("adjourned_time") or None,
        1 if data.get("is_down") else 0,
        1 if data.get("is_high_profile") else 0,
        assignment_date,
        courthouse,
        location_detail,
        data.get("part") or "",
        data.get("start_time") or None,
        data.get("restart_time") or None,
        data.get("adjourned_time") or None,
        1 if data.get("is_down") else 0,
        1 if data.get("is_high_profile") else 0,
    ))

    conn.commit()
    conn.close()
    return {"status": "success"}


@app.route("/api/clear-daily-assignments", methods=["POST"])
def clear_daily_assignments():
    data = request.json or {}
    assignment_date = (data.get("assignment_date") or "").strip()

    if not assignment_date:
        return jsonify({"status": "error", "message": "assignment_date is required"}), 400

    conn = get_conn()
    cursor = conn.cursor()
    _ensure_courtroom_meta_table(cursor)

    cursor.execute("""
        UPDATE dbo.court_assignments
        SET assigned_member = NULL
        WHERE assignment_date = ?
    """, (assignment_date,))

    cursor.execute("""
        UPDATE dbo.court_assignments
        SET assignment_notes = NULL
        WHERE assignment_date = ?
          AND assignment_type = 'Courtroom'
    """, (assignment_date,))

    cursor.execute("""
        UPDATE dbo.courtroom_meta
        SET is_high_profile = 0,
            updated_at = GETDATE()
        WHERE assignment_date = ?
    """, (assignment_date,))

    conn.commit()
    conn.close()

    return jsonify({"status": "success"})


@app.route("/api/clear-section-assignments", methods=["POST"])
def clear_section_assignments():
    data = request.json or {}
    assignment_date = (data.get("assignment_date") or "").strip()
    section_type = (data.get("section_type") or "").strip().lower()
    courthouse = (data.get("courthouse") or "").strip()

    if not assignment_date:
        return jsonify({"status": "error", "message": "assignment_date is required"}), 400

    if section_type not in {"fixed_post", "courtroom"}:
        return jsonify({"status": "error", "message": "section_type must be fixed_post or courtroom"}), 400

    conn = get_conn()
    cursor = conn.cursor()
    if section_type == "courtroom":
        _ensure_courtroom_meta_table(cursor)

    if section_type == "fixed_post":
        if not courthouse:
            return jsonify({"status": "error", "message": "courthouse is required for fixed_post clear"}), 400

        cursor.execute("""
            UPDATE dbo.court_assignments
            SET assigned_member = NULL
            WHERE assignment_date = ?
              AND assignment_type = 'Fixed Post'
              AND courthouse = ?
        """, (assignment_date, courthouse))

    if section_type == "courtroom":
        params = [assignment_date]
        courthouse_clause = ""
        if courthouse:
            courthouse_clause = " AND courthouse = ?"
            params.append(courthouse)

        cursor.execute(f"""
            UPDATE dbo.court_assignments
            SET assigned_member = NULL,
                assignment_notes = NULL
            WHERE assignment_date = ?
              AND assignment_type = 'Courtroom'{courthouse_clause}
        """, params)

        cursor.execute(f"""
            UPDATE dbo.courtroom_meta
            SET is_high_profile = 0,
                updated_at = GETDATE()
            WHERE assignment_date = ?{courthouse_clause}
        """, params)

    conn.commit()
    conn.close()

    return jsonify({"status": "success"})


@app.route("/api/search")
def search():
    name = request.args.get("name")
    date = request.args.get("date")
    if date:
        cursor = get_conn().cursor()
        cursor.execute("""
            INSERT INTO dbo.court_assignments (
                assignment_date,
                courthouse,
                assignment_type,
                location_group,
                location_detail,
                part,
                judge_name,
                shift_time,
                assigned_member,
                assignment_notes,
                created_at
            )
            SELECT
                ?,
                t.courthouse,
                t.assignment_type,
                t.location_group,
                t.location_detail,
                t.part,
                t.judge_name,
                t.shift_time,
                NULL,
                t.assignment_notes,
                GETDATE()
            FROM dbo.court_assignment_template t
            WHERE NOT EXISTS (
                SELECT 1
                FROM dbo.court_assignments a
                WHERE a.assignment_date = ?
                AND a.courthouse = t.courthouse
                AND a.assignment_type = t.assignment_type
                AND ISNULL(a.location_group,'') = ISNULL(t.location_group,'')
                AND ISNULL(a.location_detail,'') = ISNULL(t.location_detail,'')
                AND ISNULL(a.part,'') = ISNULL(t.part,'')
            )
        """, (date, date))
        cursor.connection.commit()
        cursor.connection.close()
    courthouse = request.args.get("courthouse")

    query = """
        SELECT TOP 200
            a.assignment_date,
            a.courthouse,
            a.assignment_type,
            a.location_group,
            a.location_detail,
            a.judge_name,
            a.part,
            a.shift_time,
            a.assigned_member,
            a.assignment_notes,
            ISNULL(m.is_high_profile, 0) AS is_high_profile
        FROM dbo.court_assignments a
        LEFT JOIN dbo.courtroom_meta m
            ON m.assignment_date = a.assignment_date
            AND m.courthouse = a.courthouse
            AND ISNULL(m.location_detail, '') = ISNULL(a.location_detail, '')
            AND ISNULL(m.part, '') = ISNULL(a.part, '')
        WHERE 1=1
    """

    params = []

    if name:
        query += " AND a.assigned_member LIKE ?"
        params.append(f"%{name}%")

    if date:
        query += " AND a.assignment_date = ?"
        params.append(date)

    if courthouse:
        query += " AND a.courthouse = ?"
        params.append(courthouse)

    query += " ORDER BY a.assignment_date DESC"

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(query, params)

    columns = [column[0] for column in cursor.description]
    raw_results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Some historical data contains duplicate assignment rows for the same slot.
    # Collapse those rows at read time so the UI shows each assignment once.
    deduped_results = {}
    for row in raw_results:
        dedupe_key = (
            row.get("assignment_date"),
            (row.get("courthouse") or "").strip(),
            (row.get("assignment_type") or "").strip(),
            (row.get("location_group") or "").strip(),
            (row.get("location_detail") or "").strip(),
            (row.get("part") or "").strip(),
        )

        existing = deduped_results.get(dedupe_key)
        if not existing:
            deduped_results[dedupe_key] = row
            continue

        existing_assigned = (existing.get("assigned_member") or "").strip()
        incoming_assigned = (row.get("assigned_member") or "").strip()

        # Prefer the row that has an assigned member populated.
        if not existing_assigned and incoming_assigned:
            deduped_results[dedupe_key] = row

    results = list(deduped_results.values())

    conn.close()

    return jsonify(results)


@app.route("/api/import-previous-weekday", methods=["POST"])
def import_previous_weekday():
    data = request.json or {}
    target_date_raw = data.get("target_date")
    target_date = _parse_date_value(target_date_raw)

    if not target_date:
        return jsonify({"status": "error", "message": "target_date is required (YYYY-MM-DD)"}), 400

    source_date = _previous_weekday(target_date)
    source_date_str = source_date.isoformat()
    target_date_str = target_date.isoformat()

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT full_name, current_status
        FROM dbo.deputies
    """)
    unavailable_names = {
        (row[0] or "").strip().lower()
        for row in cursor.fetchall()
        if row[0] and _is_off_for_assignment(row[1], target_date_str)
    }

    def _slot_key(row):
        return (
            (row["courthouse"] or "").strip(),
            (row["assignment_type"] or "").strip(),
            (row["location_group"] or "").strip(),
            (row["location_detail"] or "").strip(),
            (row["part"] or "").strip(),
        )

    cursor.execute("""
        SELECT courthouse, assignment_type, location_group, location_detail, part, assigned_member
        FROM dbo.court_assignments
        WHERE assignment_date = ?
    """, (target_date_str,))
    target_rows = [
        {
            "courthouse": row[0],
            "assignment_type": row[1],
            "location_group": row[2],
            "location_detail": row[3],
            "part": row[4],
            "assigned_member": row[5],
        }
        for row in cursor.fetchall()
    ]

    target_slot_keys = {_slot_key(row) for row in target_rows}
    populated_slot_keys = {
        _slot_key(row)
        for row in target_rows
        if (row.get("assigned_member") or "").strip()
    }

    cursor.execute("""
        SELECT courthouse, assignment_type, location_group, location_detail, part,
               judge_name, shift_time, assigned_member, assignment_notes
        FROM dbo.court_assignments
        WHERE assignment_date = ?
          AND ISNULL(assigned_member, '') <> ''
    """, (source_date_str,))
    source_rows = [
        {
            "courthouse": row[0],
            "assignment_type": row[1],
            "location_group": row[2],
            "location_detail": row[3],
            "part": row[4],
            "judge_name": row[5],
            "shift_time": row[6],
            "assigned_member": row[7],
            "assignment_notes": row[8],
        }
        for row in cursor.fetchall()
    ]

    updated_count = 0
    inserted_count = 0

    for source_row in source_rows:
        assigned_member = (source_row.get("assigned_member") or "").strip()
        if assigned_member.lower() in unavailable_names:
            continue

        slot_key = _slot_key(source_row)

        if slot_key in target_slot_keys:
            if slot_key in populated_slot_keys:
                continue

            if source_row.get("assignment_type") == "Fixed Post":
                cursor.execute("""
                    UPDATE dbo.court_assignments
                    SET assigned_member = ?,
                        assignment_notes = ?
                    WHERE assignment_date = ?
                      AND courthouse = ?
                      AND assignment_type = ?
                      AND ISNULL(location_group,'') = ?
                      AND ISNULL(location_detail,'') = ?
                      AND ISNULL(part,'') = ?
                      AND ISNULL(assigned_member,'') = ''
                """, (
                    assigned_member,
                    source_row.get("assignment_notes"),
                    target_date_str,
                    source_row.get("courthouse"),
                    source_row.get("assignment_type"),
                    (source_row.get("location_group") or ""),
                    (source_row.get("location_detail") or ""),
                    (source_row.get("part") or ""),
                ))
            else:
                cursor.execute("""
                    UPDATE dbo.court_assignments
                    SET assigned_member = ?,
                        assignment_notes = ?,
                        shift_time = COALESCE(NULLIF(shift_time, ''), ?)
                    WHERE assignment_date = ?
                      AND courthouse = ?
                      AND assignment_type = ?
                      AND ISNULL(location_group,'') = ?
                      AND ISNULL(location_detail,'') = ?
                      AND ISNULL(part,'') = ?
                      AND ISNULL(assigned_member,'') = ''
                """, (
                    assigned_member,
                    source_row.get("assignment_notes"),
                    source_row.get("shift_time"),
                    target_date_str,
                    source_row.get("courthouse"),
                    source_row.get("assignment_type"),
                    (source_row.get("location_group") or ""),
                    (source_row.get("location_detail") or ""),
                    (source_row.get("part") or ""),
                ))
            if cursor.rowcount and cursor.rowcount > 0:
                updated_count += cursor.rowcount
                populated_slot_keys.add(slot_key)
            continue

        shift_time_to_insert = None if source_row.get("assignment_type") == "Fixed Post" else source_row.get("shift_time")

        cursor.execute("""
            INSERT INTO dbo.court_assignments (
                assignment_date,
                courthouse,
                assignment_type,
                location_group,
                location_detail,
                part,
                judge_name,
                shift_time,
                assigned_member,
                assignment_notes,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """, (
            target_date_str,
            source_row.get("courthouse"),
            source_row.get("assignment_type"),
            source_row.get("location_group"),
            source_row.get("location_detail"),
            source_row.get("part"),
            source_row.get("judge_name"),
            shift_time_to_insert,
            assigned_member,
            source_row.get("assignment_notes"),
        ))
        inserted_count += 1
        target_slot_keys.add(slot_key)
        populated_slot_keys.add(slot_key)
    imported_count = updated_count + inserted_count
    conn.commit()
    conn.close()

    return jsonify({
        "status": "success",
        "target_date": target_date_str,
        "source_date": source_date_str,
        "updated_count": updated_count,
        "inserted_count": inserted_count,
        "imported_count": imported_count,
    })

if __name__ == "__main__":
    app.run(debug=True)
