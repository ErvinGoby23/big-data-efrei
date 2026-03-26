import json
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="05062003",
    database="bigdata",
    port=3307
)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS review (
        review_id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(50),
        business_id VARCHAR(50),
        stars FLOAT,
        useful INT,
        funny INT,
        cool INT,
        text TEXT,
        date DATETIME
    )
""")

batch = []
BATCH_SIZE = 1000

with open("yelp_academic_dataset_review.json", "r", encoding="utf-8") as f:
    for line in f:
        row = json.loads(line)
        batch.append((
            row["review_id"], row["user_id"], row["business_id"],
            row["stars"], row["useful"], row["funny"],
            row["cool"], row["text"], row["date"]
        ))

        if len(batch) == BATCH_SIZE:
            cursor.executemany("""
                INSERT IGNORE INTO review VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, batch)
            conn.commit()
            batch = []

# insert le reste
if batch:
    cursor.executemany("""
        INSERT IGNORE INTO review VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, batch)
    conn.commit()

cursor.close()
conn.close()
print("Done — table review chargée dans MariaDB")