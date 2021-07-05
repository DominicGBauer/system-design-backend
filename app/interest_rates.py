import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin


@app.route("/api/interest_rates")
@cross_origin()
def getInterestRates():
    query_parameters = request.args
    curve = query_parameters.get("curve")
    tenor = query_parameters.get("tenor")

    query = """SELECT
            `Date` as name,
            ROUND(Rate * 100,2) as value
            FROM `interest_rate_data`
            WHERE `Tenor` = {0}
            AND `Curve` = {1}
            """.format(
        tenor, curve
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    resp.status_code = 200

    return resp
