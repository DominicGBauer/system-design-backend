import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin


@app.route("/api/interest-rates")
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


@app.route("/api/interest-rates/available-dates")
@cross_origin()
def getInterestRateAvailableDates():
    query = """
    SELECT DISTINCT `date`
    FROM `interest_rate_data`
    ORDER BY `Date` DESC
    """

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    resp.status_code = 200

    return resp


@app.route("/api/interest-rates/yield-curve")
@cross_origin()
def getYieldCurve():
    query_parameters = request.args
    curve = query_parameters.get("curve")
    date = query_parameters.get("date")

    query = """
    SELECT Tenor AS name,
    Rate AS value
    FROM `interest_rate_data`
    WHERE `Date` = '{0}'
    AND Curve = '{1}'
    ORDER BY `Tenor`
    """.format(
        date, curve
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    resp.status_code = 200

    return resp
