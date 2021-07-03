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

    query = "select `date` as name, price as value from equity_data"

    if share:
        query += " where instrument='{0}' order by `date` desc".format(share)
        query += " limit {0}".format(limit)

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)

    resp.status_code = 200

    return resp
