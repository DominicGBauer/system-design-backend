import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin
import pandas as pd


@app.route("/api/shares")
@cross_origin()
def getAShare():
    query_parameters = request.args
    share = query_parameters.get("share")

    query = "select `date` as name, price as value from equity_data"

    if share:
        query += " where instrument='{0}';".format(share)

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)

    resp.status_code = 200

    return resp
