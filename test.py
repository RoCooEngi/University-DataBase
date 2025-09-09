import sqlite3

# Connect to a database (creates it if it doesn’t exist)
conn = sqlite3.connect("my_database.db")

# Create a cursor to execute SQL commands
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    age INTEGER,
    email TEXT UNIQUE
)
""")
cursor.execute("INSERT INTO users (name, age, email) VALUES (?, ?, ?)",
               ("Alice", 25, "alice@example.com"))

cursor.execute("INSERT INTO users (name, age, email) VALUES (?, ?, ?)",
               ("Bob", 30, "bob@example.com"))
conn.commit()
cursor.execute("SELECT * FROM users")
rows = cursor.fetchall()

for row in rows:
    print(row)
conn.close()
