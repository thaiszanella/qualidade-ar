from conexao import get_db, get_redis

db = get_db()
print("MongoDB:", db.client.admin.command("ping"))

r = get_redis()
print("Redis:", r.ping())