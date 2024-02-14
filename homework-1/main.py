"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2
import os
from config import ROOT_DIR

DIR = "homework-1"
DIR_DIR = "north_data"
PASSWORD_POSTGRES = os.getenv("PASSWORD_POSTGRES")


def get_data(csvfile):
    data_list = []
    with open(os.path.join(ROOT_DIR, DIR, DIR_DIR, csvfile), encoding="utf-8", newline='') as file:
        reader = csv.reader(file, delimiter=",")
        for row in reader:
            data_list.append(tuple(row))
    return data_list[1:]


conn = psycopg2.connect(host="localhost", database="north", user="postgres", password=PASSWORD_POSTGRES)
try:
    with conn:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO customers VALUES (%s, %s, %s)", get_data("customers_data.csv"))
            cur.execute("SELECT * FROM customers")
            # rows = cur.fetchall()
            # print(rows)
            cur.executemany("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", get_data("employees_data.csv"))
            cur.execute("SELECT * FROM employees")
            # rows = cur.fetchall()
            # print(rows)
            cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", get_data("orders_data.csv"))
            cur.execute("SELECT * FROM orders")
            # rows = cur.fetchall()
            # print(rows)

finally:
    conn.close()
