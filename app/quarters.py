import pymysql
from app import app
from db import mysql
from flask import jsonify
from flask_cors import cross_origin
import pandas as pd
from flask.json import JSONEncoder
from datetime import date


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


app.json_encoder = CustomJSONEncoder


@app.route("/api/quarters")
@cross_origin()
def getDates():
    query = """SELECT
            DISTINCT(`date`) AS quarter
            FROM`index_constituents`
            ORDER BY `date` DESC
            """

    conn = mysql.connect()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()

    resp = jsonify(results)
    print(results)
    resp.status_code = 200

    return resp
