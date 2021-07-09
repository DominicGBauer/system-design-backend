import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin
from rest_utils import createEndDate


@app.route("/api/index")
@cross_origin()
def getIndex():
    query_parameters = request.args
    indexName = query_parameters.get("indexName")
    date = query_parameters.get("date")
    endDate = createEndDate(str(date))

    query = """SELECT
            instrument AS name,
            `Gross Market Capitalisation` AS value
            FROM `index_constituents`
            WHERE `{0} New` = "{0}"
            AND date BETWEEN'{1}' AND '{2}'
            ORDER BY `Gross Market Capitalisation`
            """.format(
        indexName, date, endDate
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    resp.status_code = 200

    return resp


@app.route("/api/index/super-sector")
@cross_origin()
def getIndexSubSector():
    query_parameters = request.args
    indexName = query_parameters.get("indexName")
    date = query_parameters.get("date")
    endDate = createEndDate(str(date))

    query = """SELECT
            iclass.`Super Sector` AS name,
            SUM(ic.`Gross Market Capitalisation`) AS value
            FROM `index_constituents` AS ic
            left join `industry_classification` AS iclass
            ON ic.`ICB Sub-Sector`= iclass.`Sub-Sector Code`
            WHERE `{0} New` = "{0}"
            AND date BETWEEN'{1}' AND '{2}'
            GROUP BY iclass.`Super Sector`
            """.format(
        indexName, date, endDate
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    resp.status_code = 200

    return resp
