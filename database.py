import random
import sqlite3
import string
from pathlib import Path

from ..config import BATCH_SIZE, DB_PATH, MAX_LEN, MIN_LEN, TARGET_ROWS


def random_text(min_len: int = MIN_LEN, max_len: int = MAX_LEN) -> str:
    """
    テストデータ向けに、英数字のみのランダム文字列を1件生成する。

    - 可変長(7〜30文字)にすることで、固定長より実運用に近い負荷を再現
    - 文字種を英数字へ絞り、文字コード依存のトラブルを避ける

    Args:
        min_len: 生成文字列の最小長。
        max_len: 生成文字列の最大長。

    Returns:
        ランダム生成された英数字文字列。
    """
    length = random.randint(min_len, max_len)
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


def ensure_database(db_path: Path = DB_PATH, target_rows: int = TARGET_ROWS) -> None:
    """
    SQLiteファイルとtextsテーブルを用意し、target_rows件まで不足分を補充する。

    この関数は冪等に設計されているため、毎回起動時に呼んでも安全。

    Args:
        db_path: 準備対象のSQLiteファイルパス。
        target_rows: 最終的に確保したいレコード件数。
    """
    # with文で接続のcloseを自動化し、例外時のリークを防ぐ。
    with sqlite3.connect(db_path) as conn:
        # 大量insert向けの設定:
        # WALは読み書き競合に比較的強く、NORMAL同期は性能と安全性のバランスが良い。
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        # idは表示順の安定化にも使う主キー。
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS texts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value TEXT NOT NULL
            )
            """
        )
        # 今後の検索拡張を見据えたインデックス（一覧表示のみなら必須ではない）。
        conn.execute("CREATE INDEX IF NOT EXISTS idx_texts_value ON texts(value)")

        # 既存件数が目標以上なら何もしない。
        current_rows = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
        if current_rows >= target_rows:
            return

        # 不足分のみをBATCH_SIZE単位でinsertしてコミットする。
        # 巨大トランザクションを避けて、メモリ使用量と待ち時間を抑える。
        rows_to_insert = target_rows - current_rows
        while rows_to_insert > 0:
            chunk = min(BATCH_SIZE, rows_to_insert)
            values = [(random_text(),) for _ in range(chunk)]
            conn.executemany("INSERT INTO texts(value) VALUES (?)", values)
            rows_to_insert -= chunk
            conn.commit()
