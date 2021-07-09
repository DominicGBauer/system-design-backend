import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin
from rest_utils import createEndDate


@app.route("/api/sectors/<string:sector>")
@cross_origin()
def getSector(sector):
    query_parameters = request.args
    date = query_parameters.get("date")
    endDate = createEndDate(str(date))

    query = """SELECT DISTINCT
    j203.data_points,
    j203.percentage_days_traded,
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
    iclass.`Super Sector` AS sector
    FROM
    (
        SELECT instrument,
        `Data Points` as data_points,
        round(`% Days Traded`,2)*100 as percentage_days_traded,
        Beta AS j203_beta,
        `p-Value Beta` AS j203_p_value
        FROM ba_beta_output
        WHERE `Date` BETWEEN '{0}' AND '{1}'
        AND `Index` = 'J203'
    ) AS j203
    LEFT JOIN
    (
        SELECT instrument,
        Beta AS j200_beta,
        `p-Value Beta` AS j200_p_value
        FROM ba_beta_output
        WHERE `Date` BETWEEN '{0}' AND '{1}'
        AND `Index` = 'J200'
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
    ) AS j258
    ON j203.instrument = j258.instrument
    LEFT JOIN `index_constituents` AS ic
    ON ic.`Alpha` = j203.`Instrument`
    LEFT JOIN `industry_classification` AS iclass
    ON iclass.`Sub-Sector Code` = ic.`ICB Sub-Sector`
    HAVING iclass.`Super Sector` = '{2}'
    """.format(
        date, endDate, sector
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    resp.status_code = 200

    return resp


@app.route("/api/sectors/available-sectors")
@cross_origin()
def getAvailableSectors():
    query = """
    SELECT DISTINCT `Super Sector` as sector
    FROM industry_classification
    ORDER BY `Super Sector`
    """

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)

    resp.status_code = 200

    return resp
