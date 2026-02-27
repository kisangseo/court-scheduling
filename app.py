from flask import Flask, request, jsonify, render_template
from db_connect import get_conn
import pyodbc
import os
import json
from datetime import datetime, timedelta



app = Flask(__name__)


def _normalize_status_type(status_type):
    if not status_type:
        return None
    normalized = status_type.strip().lower()
    if normalized in ["sick", "leave", "light duty"]:
        return normalized.title() if normalized != "light duty" else "Light Duty"
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
        return {"legacy": None, "ranges": []}

    try:
        parsed = json.loads(status_text)
        if isinstance(parsed, dict) and "ranges" in parsed:
            return {
                "legacy": parsed.get("legacy"),
                "ranges": parsed.get("ranges") or []
            }
    except (json.JSONDecodeError, TypeError):
        pass

    return {"legacy": status_text, "ranges": []}


def _effective_status_for_date(status_text, target_date):
    payload = _parse_status_payload(status_text)

    if target_date:
        target = _parse_date_value(target_date)
        for status_range in payload.get("ranges", []):
            start = _parse_date_value(status_range.get("start_date"))
            end = _parse_date_value(status_range.get("end_date"))
            if start and end and start <= target <= end:
                return status_range.get("status")

    return payload.get("legacy")


def _serialize_status_payload(payload):
    if not payload.get("ranges") and not payload.get("legacy"):
        return None
    return json.dumps(payload)

@app.route("/")
def index():
    return render_template("search.html")
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

    cursor.execute("""
        SELECT current_status
        FROM dbo.deputies
        WHERE full_name = ?
    """, (full_name,))
    row = cursor.fetchone()
    current_status = row[0] if row else None
    payload = _parse_status_payload(current_status)

    ranges = payload.get("ranges", [])
    ranges = [r for r in ranges if not (r.get("status") == status and r.get("start_date") == start_date and r.get("end_date") == end_date)]
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

    conn.commit()
    conn.close()

    return {"status": "success"}


@app.route("/api/upsert-deputy", methods=["POST"])
def upsert_deputy():
    data = request.json
    original_full_name = data.get("original_full_name")

    conn = get_conn()
    cursor = conn.cursor()

    if original_full_name:
        update_queries = [
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, division = ?, rank = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (data.get("full_name"), data.get("division"), data.get("rank"), data.get("capacity_tag"), original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, division = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (data.get("full_name"), data.get("division"), data.get("capacity_tag"), original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, rank = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (data.get("full_name"), data.get("rank"), data.get("capacity_tag"), original_full_name)),
            ("""
                UPDATE dbo.deputies
                SET full_name = ?, capacity_tag = ?
                WHERE full_name = ?
            """, (data.get("full_name"), data.get("capacity_tag"), original_full_name)),
        ]

        for query, params in update_queries:
            try:
                cursor.execute(query, params)
                break
            except pyodbc.ProgrammingError:
                continue
    else:
        insert_queries = [
            ("""
                INSERT INTO dbo.deputies (full_name, division, rank, capacity_tag, current_status)
                VALUES (?, ?, ?, ?, NULL)
            """, (data.get("full_name"), data.get("division"), data.get("rank"), data.get("capacity_tag"))),
            ("""
                INSERT INTO dbo.deputies (full_name, division, capacity_tag, current_status)
                VALUES (?, ?, ?, NULL)
            """, (data.get("full_name"), data.get("division"), data.get("capacity_tag"))),
            ("""
                INSERT INTO dbo.deputies (full_name, rank, capacity_tag, current_status)
                VALUES (?, ?, ?, NULL)
            """, (data.get("full_name"), data.get("rank"), data.get("capacity_tag"))),
            ("""
                INSERT INTO dbo.deputies (full_name, capacity_tag, current_status)
                VALUES (?, ?, NULL)
            """, (data.get("full_name"), data.get("capacity_tag"))),
        ]

        for query, params in insert_queries:
            try:
                cursor.execute(query, params)
                break
            except pyodbc.ProgrammingError:
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

    cursor.execute("""
        UPDATE dbo.court_assignments
        SET assigned_member = ?
        WHERE assignment_date = ?
          AND courthouse = ?
          AND assignment_type = ?
          AND (
                (assignment_type = 'Fixed Post' AND ISNULL(location_group, '') = ISNULL(?, ''))
                OR
                (assignment_type <> 'Fixed Post' AND ISNULL(location_detail, '') = ISNULL(?, ''))
              )
          AND ISNULL(part, '') = ISNULL(?, '')
    """, (
        data["assigned_member"],
        data["assignment_date"],
        data["courthouse"],
        data["assignment_type"],
        data["location_detail"],
        data["location_detail"],
        data["part"]
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
                AND a.location_detail = t.location_detail
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
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    conn.close()

    return jsonify(results)

if __name__ == "__main__":
    app.run(debug=True)
