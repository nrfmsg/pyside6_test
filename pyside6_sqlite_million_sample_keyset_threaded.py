import random
import sqlite3
import string
import sys
from pathlib import Path

from PySide6.QtCore import QAbstractTableModel, QModelIndex, QObject, QThread, Qt, Signal, Slot
from PySide6.QtWidgets import (
    QApplication,
    QLabel,
    QMainWindow,
    QPlainTextEdit,
    QSplitter,
    QTableView,
    QVBoxLayout,
    QWidget,
)


# ------------------------------------------------------------
# このサンプルの目的
# 1) SQLite に 300万件の文字列データを作る
# 2) QTableView + QAbstractTableModel で一覧表示する
# 3) 上の一覧で選んだ行の詳細を下ペインへ表示する
# ------------------------------------------------------------

# SQLite ファイル名（実行ディレクトリ直下に作成される）
DB_PATH = Path("million_strings.db")
# 最終的に用意したいレコード件数
TARGET_ROWS = 3_000_000
# 文字列長の下限 / 上限
MIN_LEN = 7
MAX_LEN = 30
# INSERT 時の1バッチ件数（大きいほど速いがメモリ使用量は増える）
BATCH_SIZE = 10_000


def random_text(min_len: int = MIN_LEN, max_len: int = MAX_LEN) -> str:
    """
    7〜30文字（デフォルト）のランダムな英数字文字列を1件生成する。

    SQLite に大量投入するテストデータとして使うため、
    読みやすさより生成コストの軽さを優先している。
    """
    # レコードごとに長さを変えることで、実運用に近い可変長データにする。
    length = random.randint(min_len, max_len)
    # 英数字のみを採用し、文字コード依存の問題を避ける。
    chars = string.ascii_letters + string.digits
    # k=length 個をランダムに選び、1本の文字列として結合して返す。
    return "".join(random.choices(chars, k=length))


def ensure_database(db_path: Path = DB_PATH, target_rows: int = TARGET_ROWS) -> None:
    """
    SQLite DB とテーブルを準備し、不足分を target_rows まで補充する。

    - すでに件数が足りていれば何もしない（再実行可能）
    - 不足している場合だけ差分をバッチ INSERT する
    """
    # with を使うことで、例外時にも接続が自動でクローズされる。
    with sqlite3.connect(db_path) as conn:
        # 大量書き込み向けの設定:
        # WAL: 読み書き競合に強く、書き込み性能も比較的良い
        # synchronous=NORMAL: FULL より高速で、サンプル用途として十分
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")

        # 表示対象テーブル。id は表示時の安定した並び順にも使う。
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS texts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value TEXT NOT NULL
            )
            """
        )

        # value に対する検索や拡張時を想定し、例としてインデックスを作成。
        # （今回の一覧表示だけなら必須ではない）
        conn.execute("CREATE INDEX IF NOT EXISTS idx_texts_value ON texts(value)")
        # すでに存在する件数を確認する。
        current_rows = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
        # 目標件数を満たしていれば再投入しない（冪等動作）。
        if current_rows >= target_rows:
            return

        # 既存件数との差分だけ追加する。
        rows_to_insert = target_rows - current_rows
        while rows_to_insert > 0:
            # 最終バッチだけ BATCH_SIZE 未満になる可能性があるため min を使う。
            chunk = min(BATCH_SIZE, rows_to_insert)
            # executemany 用に [(value,), ...] の形へ整形する。
            values = [(random_text(),) for _ in range(chunk)]
            conn.executemany("INSERT INTO texts(value) VALUES (?)", values)
            rows_to_insert -= chunk
            # 長い処理になるため、一定単位で確定して進める。
            conn.commit()


class ChunkLoaderWorker(QObject):
    """
    DBチャンク読み込み専用ワーカー。

    モデルから要求された行範囲をワーカースレッドで取得し、
    chunk_loaded シグナルでメインスレッドへ返す。
    """

    chunk_loaded = Signal(int, object)

    def __init__(self, db_path: Path, first_id: int) -> None:
        super().__init__()
        self._db_path = db_path
        self._first_id = first_id
        self._conn: sqlite3.Connection | None = None

    def _ensure_connection(self) -> sqlite3.Connection:
        # sqlite3.Connection は作成したスレッドでのみ使う。
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path)
        return self._conn

    @Slot(int, int)
    def load_chunk(self, start_row: int, chunk_size: int) -> None:
        conn = self._ensure_connection()
        start_id = self._first_id + start_row
        rows = conn.execute(
            "SELECT id, value FROM texts WHERE id >= ? ORDER BY id LIMIT ?",
            (start_id, chunk_size),
        ).fetchall()
        self.chunk_loaded.emit(start_row, rows)

    @Slot()
    def close_connection(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class SqliteTableModel(QAbstractTableModel):
    """
    SQLite の texts テーブルを QTableView に表示するためのモデル。

    Qt の Model/View では、表示に必要なセルだけ `data()` が頻繁に呼ばれる。
    300万件を毎回 SQL で単発取得すると遅いため、
    表示位置付近をまとめて読んでキャッシュする。

    この版は OFFSET を使わず、id を起点にするキーセット方式で取得する。
    （WHERE id >= ? ORDER BY id LIMIT ?）

    さらに DB読み込みをワーカースレッドへ移し、
    スクロール中のUI停止を避ける。
    """

    request_chunk = Signal(int, int)
    request_worker_close = Signal()

    def __init__(self, db_path: Path, parent=None) -> None:
        super().__init__(parent)
        # 総件数と先頭idは初期化時に同期取得し、その後は非同期読み込みへ移る。
        with sqlite3.connect(db_path) as conn:
            self._row_count = conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
            first_id_row = conn.execute("SELECT MIN(id) FROM texts").fetchone()
        self._first_id = int(first_id_row[0]) if first_id_row and first_id_row[0] is not None else 1
        # 表示列の見出し
        self._headers = ["id", "value"]

        # チャンクキャッシュ管理:
        # key=start_row, value=[(id, value), ...]
        self._chunk_cache: dict[int, list[tuple[int, str]]] = {}
        self._pending_chunks: set[int] = set()
        self._cache_size = 1000
        self._max_cached_chunks = 8

        # DB読み込みワーカーを起動。
        self._worker_thread = QThread(self)
        self._worker = ChunkLoaderWorker(db_path, self._first_id)
        self._worker.moveToThread(self._worker_thread)
        self.request_chunk.connect(self._worker.load_chunk, Qt.ConnectionType.QueuedConnection)
        self.request_worker_close.connect(
            self._worker.close_connection, Qt.ConnectionType.QueuedConnection
        )
        self._worker.chunk_loaded.connect(self._on_chunk_loaded, Qt.ConnectionType.QueuedConnection)
        self._worker_thread.start()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        # フラットな表なので親を持つ行は存在しない。
        if parent.isValid():
            return 0
        return self._row_count

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        # 2列固定（id / value）
        if parent.isValid():
            return 0
        return len(self._headers)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        # 表示用ロール以外（編集・装飾など）はこのサンプルでは未対応。
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            # 水平ヘッダ: ["id", "value"]
            return self._headers[section]
        # 垂直ヘッダには人間が読みやすい 1 始まりの行番号を返す。
        return section + 1

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        # 不正インデックスや非表示ロールには値を返さない。
        if not index.isValid() or role != Qt.DisplayRole:
            return None

        # 要求された行がキャッシュ外なら、非同期で読み込みを要求する。
        row = index.row()
        chunk_start = (row // self._cache_size) * self._cache_size
        rows = self._chunk_cache.get(chunk_start)
        if rows is None:
            self._request_chunk(chunk_start)
            return None

        offset = row - chunk_start
        if offset < 0 or offset >= len(rows):
            return None
        record = rows[offset]
        return record[index.column()]

    def _request_chunk(self, chunk_start: int) -> None:
        # すでに要求中・取得済みチャンクは再要求しない。
        if chunk_start in self._pending_chunks or chunk_start in self._chunk_cache:
            return
        if chunk_start < 0 or chunk_start >= self._row_count:
            return
        self._pending_chunks.add(chunk_start)
        self.request_chunk.emit(chunk_start, self._cache_size)

    @Slot(int, object)
    def _on_chunk_loaded(self, chunk_start: int, rows: object) -> None:
        loaded_rows = list(rows)
        self._pending_chunks.discard(chunk_start)
        self._chunk_cache[chunk_start] = loaded_rows

        # キャッシュを増やしすぎないよう、古いチャンクから削除する。
        while len(self._chunk_cache) > self._max_cached_chunks:
            oldest_start = next(iter(self._chunk_cache))
            if oldest_start == chunk_start:
                break
            del self._chunk_cache[oldest_start]

        # 読み込み完了範囲を View に通知して再描画させる。
        if loaded_rows:
            start = chunk_start
            end = min(chunk_start + len(loaded_rows) - 1, self._row_count - 1)
            top_left = self.index(start, 0)
            bottom_right = self.index(end, len(self._headers) - 1)
            self.dataChanged.emit(top_left, bottom_right, [Qt.DisplayRole])

    def close(self) -> None:
        # ワーカー側のDB接続を閉じ、スレッドを停止する。
        self.request_worker_close.emit()
        self._worker_thread.quit()
        self._worker_thread.wait()


class MainWindow(QMainWindow):
    """上下2ペイン構成のメイン画面。上:一覧、下:選択行の詳細表示。"""

    def __init__(self, db_path: Path) -> None:
        super().__init__()
        # ウィンドウ全体設定
        self.setWindowTitle("SQLite 3,000,000 rows viewer")
        self.resize(900, 600)

        # 上ペイン: 一覧テーブル（現状仕様を維持）
        self.table = QTableView(self)
        self.model = SqliteTableModel(db_path, self)
        self.table.setModel(self.model)
        # 表示は value のみとし、id 列（列0）は非表示にする。
        self.table.setColumnHidden(0, True)

        # 見やすさ優先の最低限設定。
        self.table.setAlternatingRowColors(True)
        # 今回は ORDER BY id 固定で読み込むため、ソートは無効化。
        self.table.setSortingEnabled(False)
        # 行全体選択にし、詳細ペインが「1行単位」で連動しやすいようにする。
        self.table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        # 複数選択時の仕様を考えなくて済むよう単一選択に限定。
        self.table.setSelectionMode(QTableView.SelectionMode.SingleSelection)
        self.table.verticalHeader().setDefaultSectionSize(22)

        # 下ペイン: 上で選択した行の value 内容を表示
        self.detail_value_label = QLabel("value:", self)
        self.detail_value = QPlainTextEdit(self)
        self.detail_value.setReadOnly(True)
        self.detail_value.setPlaceholderText("上の一覧で行を選択すると内容を表示します。")

        # 詳細ペインはラベル + テキスト領域で構成する。
        detail_widget = QWidget(self)
        detail_layout = QVBoxLayout(detail_widget)
        # 余白はデフォルトを使い、シンプルさを優先。
        detail_layout.addWidget(self.detail_value_label)
        detail_layout.addWidget(self.detail_value)

        # 上下分割のコンテナ。ユーザーが境界線をドラッグして比率調整できる。
        splitter = QSplitter(Qt.Orientation.Vertical, self)
        splitter.addWidget(self.table)
        splitter.addWidget(detail_widget)
        # 初期比率: 上を広め（一覧確認を主操作にするため）
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        self.setCentralWidget(splitter)

        # 一覧の選択変更に合わせて下ペインを更新
        self.table.selectionModel().currentRowChanged.connect(self._on_current_row_changed)
        # 選択中行のデータが非同期で届いた時に下ペインを再更新する。
        self.model.dataChanged.connect(self._on_model_data_changed)
        # 初期表示として先頭行を選択
        if self.model.rowCount() > 0:
            self.table.selectRow(0)

    def closeEvent(self, event) -> None:
        # ウィンドウを閉じる時にモデルの DB 接続を確実に閉じる。
        self.model.close()
        super().closeEvent(event)

    def transform_detail_data(self, row_id: int, row_value: str) -> str:
        """
        下画面に表示する文字列を加工するための拡張ポイント。

        必要に応じて、このメソッド内で row_id / row_value を使って
        表示用テキストへ変換する実装を追加する。
        """
        raise NotImplementedError

    def _on_current_row_changed(self, current: QModelIndex, previous: QModelIndex) -> None:
        # 選択行の value を下ペインへ反映する。
        del previous  # 未使用
        if not current.isValid():
            # 選択が外れた場合はプレースホルダ状態に戻す。
            self.detail_value.setPlainText("")
            return

        # current は「列」も持つため、同じ行の列0(id) / 列1(value)を明示取得する。
        id_index = self.model.index(current.row(), 0)
        value_index = self.model.index(current.row(), 1)
        row_id = self.model.data(id_index, Qt.DisplayRole)
        row_value = self.model.data(value_index, Qt.DisplayRole)
        if row_id is None or row_value is None:
            self.detail_value.setPlainText("Loading...")
            return

        # 表示前に加工フックを呼び出す（実装は transform_detail_data 側で行う）。
        try:
            display_value = self.transform_detail_data(int(row_id), str(row_value))
        except NotImplementedError:
            # まだ未実装の場合は元データをそのまま表示する。
            display_value = str(row_value)

        # 下ペインへ反映。
        self.detail_value.setPlainText(display_value)

    def _on_model_data_changed(self, top_left: QModelIndex, bottom_right: QModelIndex, roles) -> None:
        del roles  # 未使用
        current = self.table.currentIndex()
        if not current.isValid():
            return
        row = current.row()
        if top_left.row() <= row <= bottom_right.row():
            self._on_current_row_changed(current, QModelIndex())


def main() -> None:
    """
    エントリポイント。

    1. DB を準備（不足件数があれば補充）
    2. Qt アプリを起動
    3. QTableView で texts テーブルを表示
    """
    # 初回起動時のみ大量投入に時間がかかる可能性があるため、進行メッセージを出す。
    print("Preparing database (if needed)...")
    ensure_database()
    print(f"Database ready: {DB_PATH.resolve()}")
    # ここから Qt イベントループを開始。
    app = QApplication(sys.argv)
    window = MainWindow(DB_PATH)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
