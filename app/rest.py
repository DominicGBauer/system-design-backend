import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin
import rest_utils
import pandas as pd


@app.route("/api")
@cross_origin()
def users():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM jse_index")

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.status_code = 200

    return resp


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


@app.route("/api/synthethic-table")
@cross_origin()
def syntheticTable():
    query_parameters = request.args
    date = query_parameters.get("date")
    endDate = rest_utils.createEndDate(str(date))

    indexName = str(query_parameters.get("indexName"))
    indexCode = str(query_parameters.get("indexCode"))

    query = """SELECT ic.instrument,
    ic.date,
    beta.beta,
    beta.`Total Risk` AS market_volatility,
    beta.`Unique Risk` AS specific_volatility,
    ic.`Gross Market Capitalisation`/ (SELECT SUM(`Gross Market Capitalisation`) FROM `index_constituents`
    WHERE date BETWEEN "{0}" AND "{1}"
    AND `{2} New` = "{2}"
    ) AS weights
    FROM `index_constituents` AS ic
    LEFT JOIN `ba_beta_output` AS beta
    ON beta.instrument = ic.alpha
    WHERE beta.date BETWEEN "{0}" AND "{1}"
    AND ic.date BETWEEN "{0}" AND "{1}"
    AND ic.`{2} New` = "{2}"
    AND beta.index = "{3}"
    UNION
    SELECT instrument,
    date,
    beta,
    `Total Risk` AS market_volatility,
    `Unique Risk` AS specific_volatility,
    alpha
    FROM `ba_beta_output`
    WHERE date BETWEEN "{0}" AND "{1}"
    AND `index` = "{3}"
    AND `instrument` = "{3}"
    """.format(
        date, endDate, indexName, indexCode
    )

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    df_initial = pd.read_sql_query(query, conn)
    marketVolatility = df_initial.iloc[[-1]]["market_volatility"].item()
    df = df_initial[:-1]

    portfolioBeta = rest_utils.calculatePortfolioBeta(df["weights"], df["beta"])
    systematicCovarianceMatrix = rest_utils.calculateSystematicCovarianceMatrix(
        df["beta"], marketVolatility
    )
    specificCovarianceMatrix = rest_utils.calculateSpecificCovarianceMatrix(
        df["specific_volatility"]
    )
    portfolioSpecificVariance = rest_utils.calculatePortfolioSpecificVariance(
        df["weights"], df["specific_volatility"]
    )
    portfolioSystematicVariance = rest_utils.calculatePortfolioSystematicVariance(
        df["weights"], df["beta"], marketVolatility
    )
    portfolioVariance = rest_utils.calculatePortfolioVariance(
        portfolioSystematicVariance, portfolioSpecificVariance
    )
    totalCovarianceMatrix = rest_utils.calculateTotalCovarianceMatrix(
        systematicCovarianceMatrix, specificCovarianceMatrix
    )
    correlationMatrix = rest_utils.calculateCorrelationMatrix(totalCovarianceMatrix)

    # print("portfolio beta: ")
    # print(portfolioBeta)
    # print("systematic covariance: ")
    # print(systematicCovarianceMatrix)
    # print("specific covariance: ")
    # print(specificCovarianceMatrix.shape)
    # print("portfolio specific variance: ")
    # print(portfolioSpecificVariance)
    # print("portfolio systematic variance: ")
    # print(portfolioSystematicVariance)
    # print("portfolio variance: ")
    # print(portfolioVariance)
    # print("totalCovariance ")
    # print(totalCovarianceMatrix)
    print("correlation ")
    print(correlationMatrix)
    resp = jsonify(results)

    resp.status_code = 200

    return resp


def index():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM index_constituents")

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.status_code = 200

    return resp


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
