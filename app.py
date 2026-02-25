from flask import Flask, request, jsonify, render_template
from db_connect import get_conn
import pyodbc
import os



app = Flask(__name__)

@app.route("/")
def index():
    return render_template("search.html")


@app.route("/api/search")
def search():
    name = request.args.get("name")
    date = request.args.get("date")
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