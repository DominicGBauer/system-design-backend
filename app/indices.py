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
            `Gross Market Capitalisation`/100 AS value
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


@app.route("/api/index/available-index-types")
@cross_origin()
def getAvailableIndices():
    query = """SELECT DISTINCT `index type` as index_type
            FROM jse_index
            ORDER BY `index type`
            """

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
            SUM(ic.`Gross Market Capitalisation`)/100 AS value
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


@app.route("/api/index/<string:indexType>")
@cross_origin()
def getInd(indexType):
    query_parameters = request.args
    date = query_parameters.get("date")
    endDate = createEndDate(str(date))

    query = """SELECT DISTINCT
    j203.data_points,
    j203.instrument,
    j203_beta,
    j203_p_value,
    j200_beta,
    j200_p_value,
    j250_beta,
    j250_p_value,
    j257_beta,
    j257_p_value,
    j258_beta,
    j258_p_value,
    jIndex.`index name` AS index_name
    FROM
    (
        SELECT instrument,
        `Data Points` as data_points,
        Beta AS j203_beta,
        `p-Value Beta` AS j203_p_value
        FROM ba_beta_output
        WHERE `Date` BETWEEN '{0}' AND '{1}'
        AND `Index` = 'J203'
        AND instrument IN (
            SELECT `Index Code`
            FROM jse_index
            WHERE `Index Type` = '{2}'
        )
    ) AS j203
    LEFT JOIN
    (
        SELECT instrument,
        Beta AS j200_beta,
        `p-Value Beta` AS j200_p_value
        FROM ba_beta_output
        WHERE `Date` BETWEEN '{0}' AND '{1}'
        AND `Index` = 'J200'
        AND instrument IN (
            SELECT `Index Code`
            FROM jse_index
            WHERE `Index Type` = '{2}'
        )
    ) AS j200
    ON j203.instrument = j200.instrument
    LEFT JOIN
    (
        SELECT instrument,
        Beta AS j250_beta,
        `p-Value Beta` AS j250_p_value
        FROM ba_beta_output
        WHERE `Date` BETWEEN '{0}' AND '{1}'
        AND `Index` = 'J250'
        AND instrument IN (
            SELECT `Index Code`
            FROM jse_index
            WHERE `Index Type` = '{2}'
        )
    ) AS j250
    ON j203.instrument = j250.instrument
    LEFT JOIN
    (
        SELECT instrument,
        Beta AS j257_beta,
        `p-Value Beta` AS j257_p_value
        FROM ba_beta_output
        WHERE `Date` BETWEEN '{0}' AND '{1}'
        AND `Index` = 'J257'
        AND instrument IN (
            SELECT `Index Code`
            FROM jse_index
            WHERE `Index Type` = '{2}'
        )
    ) AS j257
    ON j203.instrument = j257.instrument
    LEFT JOIN
    (
        SELECT instrument,
        Beta AS j258_beta,
        `p-Value Beta` AS j258_p_value
        FROM ba_beta_output
        WHERE `Date` BETWEEN '{0}' AND '{1}'
        AND `Index` = 'J258'
        AND instrument IN (
            SELECT `Index Code`
            FROM jse_index
            WHERE `Index Type` = '{2}'
        )
    ) AS j258
    ON j203.instrument = j258.instrument
    LEFT JOIN `jse_index` AS jIndex
    ON jIndex.`Index Code` = j203.`Instrument`
    """.format(
        date, endDate, indexType
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    resp.status_code = 200

    return resp
