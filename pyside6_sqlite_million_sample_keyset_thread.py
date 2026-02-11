import random
import sqlite3
import string
import sys
from pathlib import Path

from PySide6.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QObject,
    QRunnable,
    QThreadPool,
    Qt,
    Signal,
    Slot,
)
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


class SqliteTableModel(QAbstractTableModel):
    """
    SQLite の texts テーブルを QTableView に表示するためのモデル。

    Qt の Model/View では、表示に必要なセルだけ `data()` が頻繁に呼ばれる。
    300万件を毎回 SQL で単発取得すると遅いため、
    表示位置付近をまとめて読んでキャッシュする。

    この版は OFFSET を使わず、id を起点にするキーセット方式で取得する。
    （WHERE id >= ? ORDER BY id LIMIT ?）
    """

    def __init__(self, db_path: Path, parent=None) -> None:
        super().__init__(parent)
        # モデル専用の DB 接続。
        # UI からのアクセスはメインスレッドだけなので簡素な構成にしている。
        self.conn = sqlite3.connect(db_path)
        # QTableView が必要とする総行数を先に取得。
        self._row_count = self.conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
        # id の最小値。row -> id の変換に使う。
        first_id_row = self.conn.execute("SELECT MIN(id) FROM texts").fetchone()
        self._first_id = int(first_id_row[0]) if first_id_row and first_id_row[0] is not None else 1
        # 表示列の見出し
        self._headers = ["id", "value"]

        # キャッシュ管理:
        # _cache_start: キャッシュ先頭の絶対行番号
        # _cache_rows : [(id, value), ...] の実データ
        self._cache_start = -1
        self._cache_rows: list[tuple[int, str]] = []
        # 一度に保持する件数。大きいほど SQL 回数は減る一方で、
        # 一回の取得コストとメモリ使用量は増える。
        self._cache_size = 1000

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

        # 要求された行がキャッシュ外なら、その近傍を DB から再読込する。
        # index.row() はモデル全体での絶対行番号。
        row = index.row()
        self._ensure_cache(row)
        # キャッシュ先頭との差分を計算し、キャッシュ内インデックスに変換。
        offset = row - self._cache_start
        if offset < 0 or offset >= len(self._cache_rows):
            # 取得失敗時は None を返し、ビュー側に空表示させる。
            return None

        # record は (id, value) のタプル。列番号で取り出して返す。
        record = self._cache_rows[offset]
        return record[index.column()]

    def _ensure_cache(self, row: int) -> None:
        # すでに必要行がキャッシュ内なら何もしない。
        if self._cache_start <= row < self._cache_start + len(self._cache_rows):
            return

        # 要求行がキャッシュ中央付近に来るように開始位置を決める。
        # これにより上下スクロール時の再読み込み頻度を抑える。
        start = max(0, row - (self._cache_size // 2))

        # OFFSET を使わず、id を起点に必要範囲のみを取得する。
        # 前提: このサンプルでは INSERT のみで id が連番で増える。
        start_id = self._first_id + start
        rows = self.conn.execute(
            "SELECT id, value FROM texts WHERE id >= ? ORDER BY id LIMIT ?",
            (start_id, self._cache_size),
        ).fetchall()
        # 新しいキャッシュへ置き換える。
        self._cache_start = start
        self._cache_rows = rows

    def close(self) -> None:
        # 明示的に DB 接続を閉じて終了時の後始末を行う。
        self.conn.close()


class DetailTransformSignals(QObject):
    """詳細データ変換タスクの完了通知用シグナル。"""

    finished = Signal(int, str)
    failed = Signal(int, str)


class DetailTransformTask(QRunnable):
    """選択中行の詳細文字列をワーカースレッドで生成する。"""

    def __init__(self, request_id: int, row_id: int, row_value: str, transform_fn) -> None:
        super().__init__()
        self._request_id = request_id
        self._row_id = row_id
        self._row_value = row_value
        self._transform_fn = transform_fn
        self.signals = DetailTransformSignals()

    @Slot()
    def run(self) -> None:
        try:
            display_value = self._transform_fn(self._row_id, self._row_value)
        except NotImplementedError:
            display_value = self._row_value
        except Exception as exc:
            self.signals.failed.emit(self._request_id, str(exc))
            return
        self.signals.finished.emit(self._request_id, str(display_value))


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
        self._detail_request_id = 0
        self._detail_thread_pool = QThreadPool(self)
        self._detail_thread_pool.setMaxThreadCount(1)

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
        # 初期表示として先頭行を選択
        if self.model.rowCount() > 0:
            self.table.selectRow(0)

    def closeEvent(self, event) -> None:
        # ウィンドウを閉じる時にモデルの DB 接続を確実に閉じる。
        self._detail_thread_pool.clear()
        self._detail_thread_pool.waitForDone(2000)
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
        self._detail_request_id += 1
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
            return

        request_id = self._detail_request_id
        # 未開始タスクを破棄し、最新選択のみを優先して処理する。
        self._detail_thread_pool.clear()
        task = DetailTransformTask(request_id, int(row_id), str(row_value), self.transform_detail_data)
        task.signals.finished.connect(self._on_detail_ready)
        task.signals.failed.connect(self._on_detail_failed)
        self._detail_thread_pool.start(task)

    @Slot(int, str)
    def _on_detail_ready(self, request_id: int, display_value: str) -> None:
        # すでに新しい選択へ切り替わっている場合は古い結果を捨てる。
        if request_id != self._detail_request_id:
            return
        self.detail_value.setPlainText(display_value)

    @Slot(int, str)
    def _on_detail_failed(self, request_id: int, error_message: str) -> None:
        # すでに新しい選択へ切り替わっている場合は古い結果を捨てる。
        if request_id != self._detail_request_id:
            return
        self.detail_value.setPlainText(f"詳細表示の処理でエラーが発生しました: {error_message}")


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
