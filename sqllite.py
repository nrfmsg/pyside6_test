import atexit
import os
import sqlite3
import tempfile

_DB_PATH = None


def _cleanup_db():
    global _DB_PATH
    if _DB_PATH and os.path.exists(_DB_PATH):
        try:
            os.remove(_DB_PATH)
        except OSError:
            pass
        _DB_PATH = None



def main():
    global _DB_PATH
    tmp = tempfile.NamedTemporaryFile(prefix="example_", suffix=".db", delete=False)
    _DB_PATH = tmp.name
    tmp.close()

    atexit.register(_cleanup_db)
    # 1) 接続（ファイルがなければ作成）
    conn = sqlite3.connect(_DB_PATH)
    cur = conn.cursor()

    # 2) テーブル作成
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            age  INTEGER NOT NULL
        )
    """)

    # 3) データ挿入
    cur.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Alice", 30))
    cur.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Bob", 25))

    # 4) 変更を保存
    conn.commit()

    # 5) 取得
    cur.execute("SELECT id, name, age FROM users ORDER BY id")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    # 6) 終了
    conn.close()
    _cleanup_db()

if __name__ == "__main__":
    main()
