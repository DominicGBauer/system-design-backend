import pymysql
from app import app
from db import mysql
from flask import jsonify, request
from flask_cors import cross_origin
from rest_utils import createEndDate, calculateStatistics
import pandas as pd


@app.route("/api/synthethic-table")
@cross_origin()
def syntheticTable():
    query_parameters = request.args
    date = query_parameters.get("date")
    endDate = createEndDate(str(date))

    indexName = str(query_parameters.get("indexName"))
    indexCode = str(query_parameters.get("indexCode"))

    query = """SELECT beta.alpha,
    ic.date,
    beta.beta,
    beta.instrument as code,
    beta.`p-Value Beta` as p_value,
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
    AND beta.`Total Risk` != 0
    AND beta.beta IS NOT NULL
    UNION
    SELECT alpha,
    date,
    beta,
    `Data Points`,
    `p-Value Beta`,
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

    (
        portfolioBeta,
        specificCovarianceMatrix,
        systematicCovarianceMatrix,
        portfolioSpecificVariance,
        portfolioSystematicVariance,
        totalCovarianceMatrix,
        portfolioVariance,
        correlationMatrix,
    ) = calculateStatistics(
        df["beta"], df["weights"], df["specific_volatility"], marketVolatility
    )

    calculations = {
        "portfolio_beta": portfolioBeta,
        "portfolio_specific_variance": portfolioSpecificVariance,
        "portfolio_systematic_variance": portfolioSystematicVariance,
        "portfolio_variance": portfolioVariance,
        "systematic_covariance_matrix": systematicCovarianceMatrix.tolist(),
        "specific_covariance_matrix": specificCovarianceMatrix.tolist(),
        "total_covariance_matrix": totalCovarianceMatrix.tolist(),
        "correlation_matrix": correlationMatrix.tolist(),
    }

    results.append(calculations)

    resp = jsonify(results)

    resp.status_code = 200

    return resp
