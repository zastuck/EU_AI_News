import sqlite3
import pandas as pd

DB_PATH = "eu_ai_news.db"

con = sqlite3.connect(DB_PATH)

cur = con.cursor()

cur.execute(
    """
    SELECT COUNT(id) FROM articles
    """
)

rows = cur.fetchall()
