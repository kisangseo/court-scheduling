from flask import Flask, request, jsonify, render_template
from db_connect import get_conn
import pyodbc
import os
import json
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
    if normalized in ["sick", "leave", "light duty"]:
        return normalized.title() if normalized != "light duty" else "Light Duty"
    if normalized in ["unscheduled", "unscheduled leave", "callout sick", "unscheduled leave or callout sick"]:
        return "Unscheduled"
    return status_type


def _parse_date_value(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


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


@app.route("/api/transfers")
def get_transfers():
    assignment_date = request.args.get("date")
    if not assignment_date:
        return jsonify([])

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT full_name, transfer_out_time, transfer_in_time
        FROM dbo.deputy_transfers
        WHERE assignment_date = ?
    """, (assignment_date,))
    rows = cursor.fetchall()
    conn.close()

    def fmt(dt):
        if not dt:
            return None
        # "9:05 AM" formatting
        return dt.strftime("%I:%M %p").lstrip("0")

    return jsonify([
        {
            "full_name": r[0],
            "out_time": fmt(r[1]),
            "in_time": fmt(r[2]),
        }
        for r in rows
    ])


@app.route("/api/transfer-out", methods=["POST"])
def transfer_out():
    data = request.json or {}
    assignment_date = data.get("assignment_date")
    full_name = data.get("full_name")
    if not assignment_date or not full_name:
        return jsonify({"status": "error", "message": "missing assignment_date/full_name"}), 400

    conn = get_conn()
    cursor = conn.cursor()

    # Upsert: set OUT if not set (or overwrite if you want; I’m doing "set/overwrite" for simplicity)
    cursor.execute("""
        MERGE dbo.deputy_transfers AS t
        USING (SELECT ? AS assignment_date, ? AS full_name) AS s
        ON t.assignment_date = s.assignment_date AND t.full_name = s.full_name
        WHEN MATCHED THEN
            UPDATE SET transfer_out_time = CAST(SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS datetime)
        WHEN NOT MATCHED THEN
            INSERT (assignment_date, full_name, transfer_out_time, transfer_in_time)
            VALUES (?, ?, CAST(SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS datetime), NULL);
    """, (assignment_date, full_name, assignment_date, full_name))

    conn.commit()
    conn.close()
    return jsonify({"status": "success"})


@app.route("/api/transfer-in", methods=["POST"])
def transfer_in():
    data = request.json or {}
    assignment_date = data.get("assignment_date")
    full_name = data.get("full_name")
    if not assignment_date or not full_name:
        return jsonify({"status": "error", "message": "missing assignment_date/full_name"}), 400

    conn = get_conn()
    cursor = conn.cursor()

    # Only set IN if the row exists + has an OUT time (so “in” only happens after a real transfer-out)
    cursor.execute("""
        UPDATE dbo.deputy_transfers
        SET transfer_in_time = CAST(SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS datetime)
        WHERE assignment_date = ?
          AND full_name = ?
          AND transfer_out_time IS NOT NULL
    """, (assignment_date, full_name))

    conn.commit()
    conn.close()
    return jsonify({"status": "success"})
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

    if status in ["Sick", "Leave", "Light Duty", "Unscheduled"] and start_date and end_date:

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
    data = request.json
    original_full_name = data.get("original_full_name")
    full_name = data.get("full_name")

    email = data.get("email") or _build_baltimore_email(full_name)
    if not email:
        return jsonify({"status": "error", "message": "full_name must be in 'Last name, First name' format"}), 400

    conn = get_conn()
    cursor = conn.cursor()

    if original_full_name:
        update_queries = [
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, email = ?, division = ?, rank = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, email, data.get("division"), data.get("rank"), data.get("capacity_tag"), original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, email = ?, division = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, email, data.get("division"), data.get("capacity_tag"), original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, division = ?, rank = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, data.get("division"), data.get("rank"), data.get("capacity_tag"), original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, division = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, data.get("division"), data.get("capacity_tag"), original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, email = ?, rank = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, email, data.get("rank"), data.get("capacity_tag"), original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, rank = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, data.get("rank"), data.get("capacity_tag"), original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, email = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, email, data.get("capacity_tag"), original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (full_name, data.get("capacity_tag"), original_full_name)),
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
            """, (full_name, email, data.get("division"), data.get("rank"), data.get("capacity_tag"))),
            ("""
                INSERT INTO dbo.deputies (full_name, email, division, capacity_tag, current_status)
                VALUES (?, ?, ?, ?, NULL)
            """, (full_name, email, data.get("division"), data.get("capacity_tag"))),
            ("""
                INSERT INTO dbo.deputies (full_name, email, rank, capacity_tag, current_status)
                VALUES (?, ?, ?, ?, NULL)
            """, (full_name, email, data.get("rank"), data.get("capacity_tag"))),
            ("""
                INSERT INTO dbo.deputies (full_name, email, capacity_tag, current_status)
                VALUES (?, ?, ?, NULL)
            """, (full_name, email, data.get("capacity_tag"))),
            ("""
                INSERT INTO dbo.deputies (full_name, division, rank, capacity_tag, current_status)
                VALUES (?, ?, ?, ?, NULL)
            """, (full_name, data.get("division"), data.get("rank"), data.get("capacity_tag"))),
            ("""
                INSERT INTO dbo.deputies (full_name, division, capacity_tag, current_status)
                VALUES (?, ?, ?, NULL)
            """, (full_name, data.get("division"), data.get("capacity_tag"))),
            ("""
                INSERT INTO dbo.deputies (full_name, rank, capacity_tag, current_status)
                VALUES (?, ?, ?, NULL)
            """, (full_name, data.get("rank"), data.get("capacity_tag"))),
            ("""
                INSERT INTO dbo.deputies (full_name, capacity_tag, current_status)
                VALUES (?, ?, NULL)
            """, (full_name, data.get("capacity_tag"))),
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
    data = request.json

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE dbo.court_assignments
        SET assignment_notes = ?
        WHERE assignment_date = ?
          AND courthouse = ?
          AND assignment_type = ?
          AND location_detail = ?
          AND part = ?
    """, (
        data["assignment_notes"],
        data["assignment_date"],
        data["courthouse"],
        data["assignment_type"],
        data["location_detail"],
        data["part"]
    ))

    conn.commit()
    conn.close()

    return {"status": "success"}

@app.route("/api/update-judge-name", methods=["POST"])
def update_judge_name():
    data = request.json

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE dbo.court_assignments
        SET judge_name = ?
        WHERE assignment_date = ?
          AND courthouse = ?
          AND assignment_type = ?
          AND location_detail = ?
          AND part = ?
    """, (
        data["judge_name"],
        data["assignment_date"],
        data["courthouse"],
        data["assignment_type"],
        data["location_detail"],
        data["part"]
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
        """, (
            data["assigned_member"],
            data["assignment_date"],
            data["courthouse"],
            data["assignment_type"],
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
        WHERE 1=1
    """

    params = []

    if name:
        query += " AND assigned_member LIKE ?"
        params.append(f"%{name}%")

    if date:
        query += " AND assignment_date = ?"
        params.append(date)

    if courthouse:
        query += " AND courthouse = ?"
        params.append(courthouse)

    query += " ORDER BY assignment_date DESC"

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

if __name__ == "__main__":
    app.run(debug=True)
