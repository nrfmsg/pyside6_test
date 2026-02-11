import sqlite3
from pathlib import Path

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt


class SqliteTableModel(QAbstractTableModel):
    """
    textsテーブルをQTableViewへ供給するModel層。

    300万件のような大規模データを全件メモリに持たず、
    表示位置の近傍だけをキャッシュして返す。
    """

    def __init__(self, db_path: Path, parent=None) -> None:
        """
        SQLite接続と表示キャッシュを初期化する。

        Args:
            db_path: 読み込み対象SQLiteファイルのパス。
            parent: Qt親オブジェクト。
        """
        super().__init__(parent)
        # Model専用接続。UIスレッド内で利用する想定。
        self.conn = sqlite3.connect(db_path)
        # rowCountはViewが頻繁に参照するため初期化時に確定。
        self._row_count = self.conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]

        # keyset取得の基準となる最小id。空テーブル時は1をフォールバック。
        first_id_row = self.conn.execute("SELECT MIN(id) FROM texts").fetchone()
        self._first_id = int(first_id_row[0]) if first_id_row and first_id_row[0] is not None else 1

        self._headers = ["id", "value"]
        # _cache_start: モデル全体での先頭行番号
        # _cache_rows : [(id, value), ...] の実データ
        self._cache_start = -1
        self._cache_rows: list[tuple[int, str]] = []
        # 1回のSQL取得件数。増やすとSQL回数減、メモリ消費増。
        self._cache_size = 1000

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """
        Viewへ総行数を返す。

        Args:
            parent: 親index（テーブルモデルでは常に無効想定）。
        """
        # フラットテーブルのため子要素は持たない。
        if parent.isValid():
            return 0
        return self._row_count

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """
        表示列数を返す（id/valueの2列）。

        Args:
            parent: 親index（テーブルモデルでは常に無効想定）。
        """
        # id/valueの2列固定。
        if parent.isValid():
            return 0
        return len(self._headers)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        """
        ヘッダ表示文字列を返す。

        Args:
            section: 列または行の番号。
            orientation: 水平/垂直の向き。
            role: 取得対象ロール。
        """
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return self._headers[section]
        # 垂直ヘッダは1始まり表示。
        return section + 1

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        """
        指定セルの表示値を返す。

        必要な行がキャッシュ外の場合のみDBから近傍を再取得する。

        Args:
            index: 対象セルのindex。
            role: 取得対象ロール。
        """
        # 非表示ロールや不正indexはNoneで返す。
        if not index.isValid() or role != Qt.DisplayRole:
            return None

        row = index.row()
        # 要求行がキャッシュ外なら近傍を再取得。
        self._ensure_cache(row)
        # 絶対行番号をキャッシュ相対位置へ変換。
        offset = row - self._cache_start
        if offset < 0 or offset >= len(self._cache_rows):
            return None

        record = self._cache_rows[offset]
        return record[index.column()]

    def _ensure_cache(self, row: int) -> None:
        """
        指定行を含むようにキャッシュを更新する内部メソッド。

        Args:
            row: モデル全体での絶対行番号。
        """
        # 既にキャッシュ内なら再クエリしない。
        if self._cache_start <= row < self._cache_start + len(self._cache_rows):
            return

        # 要求行がキャッシュ中央付近に来るよう開始位置を調整。
        # 上下スクロール時の再読込回数を抑える。
        start = max(0, row - (self._cache_size // 2))
        # OFFSETを使わずid条件で取得するkeyset方式。
        # 大きな行番号でもOFFSET走査コストを避けやすい。
        start_id = self._first_id + start
        rows = self.conn.execute(
            "SELECT id, value FROM texts WHERE id >= ? ORDER BY id LIMIT ?",
            (start_id, self._cache_size),
        ).fetchall()

        self._cache_start = start
        self._cache_rows = rows

    def close(self) -> None:
        """Modelが保持するSQLite接続を明示的に閉じる。"""
        # 明示的クローズで終了処理を明確化。
        self.conn.close()
