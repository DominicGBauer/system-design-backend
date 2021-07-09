import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin


@app.route("/api/highlights")
@cross_origin()
def getDatesForHighlight():
    query_parameters = request.args
    share = query_parameters.get("share")
    limit = query_parameters.get("daysInQuarter")

    query = """
    SELECT `date` AS name, price AS value
    FROM equity_data
    WHERE instrument='{0}'
    ORDER BY `date` DESC
    LIMIT {1}
    """.format(
        share, limit
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)

    resp.status_code = 200

    return resp
