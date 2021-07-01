import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin
import pandas as pd


@app.route("/api/index")
@cross_origin()
def getIndex():
    query_parameters = request.args
    date = query_parameters.get("date")
    indexName = query_parameters.get("indexName")

    query = """SELECT
            instrument AS name,
            `Gross Market Capitalisation` AS value
            FROM `index_constituents`
            WHERE `{0} New` = "{0}"
            """.format(
        indexName
    )

    if date:
        query += " AND date='{0}'".format(date)

    query += " order by `Gross Market Capitalisation`"

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    resp.status_code = 200

    return resp
