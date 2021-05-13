import pymysql
from app import app
from db import mysql
from flask import jsonify
from flask_cors import cross_origin


@app.route("/api")
@cross_origin()
def users():
    conn = mysql.connect()

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT * FROM user")

    rows = cursor.fetchall()

    resp = jsonify(rows)
    resp.headers.add("Access-Control-Allow-Origin", "*")
    resp.status_code = 200

    return resp


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
