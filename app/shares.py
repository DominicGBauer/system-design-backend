import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin
from rest_utils import createEndDate


@app.route("/api/shares/<string:share>")
@cross_origin()
def getShare(share):
    query = """
    SELECT `date` AS name,
    price/100 AS value
    FROM equity_data
    WHERE instrument='{0}'
    """.format(
        share
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)

    resp.status_code = 200

    return resp


@app.route("/api/shares/available-shares")
@cross_origin()
def getAvailableShares():
    query = """
    SELECT DISTINCT instrument,
    alpha
    FROM `index_constituents`
    """

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)

    resp.status_code = 200

    return resp


@app.route("/api/shares/<string:share>/info")
@cross_origin()
def getShareInformation(share):
    query_parameters = request.args
    date = query_parameters.get("date")
    endDate = createEndDate(str(date))

    query = """
    SELECT DISTINCT
    ic.instrument AS share,
    ic.alpha AS code,
    bo.`Index` AS `index`,
    bo.beta AS beta,
    bo.`p-Value Beta` AS p_value,
    round(bo.`R2` * 100, 2) AS r_squared,
    iclass.`Super Sector` AS super_sector ,
    iclass.`Sub-Sector` AS sub_sector,
    iclass.`Sector` AS sector
    FROM ba_beta_output AS bo
    LEFT JOIN `index_constituents` AS ic
    ON ic.`Alpha` = bo.`Instrument`
    LEFT JOIN `industry_classification` AS iclass
    ON iclass.`Sub-Sector Code` = ic.`ICB Sub-Sector`
    WHERE bo.instrument = '{0}'
    AND bo.`Date` BETWEEN '{1}' AND '{2}'
    """.format(
        share, date, endDate
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)

    resp.status_code = 200

    return resp


@app.route("/api/shares/<string:share>/available-dates")
@cross_origin()
def getShareDates(share):
    query = """
    SELECT DISTINCT
    `Date` AS quarter
    FROM `ba_beta_output`
    WHERE instrument = '{0}'
    ORDER BY `Date` DESC
    """.format(
        share
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)

    resp.status_code = 200

    return resp
