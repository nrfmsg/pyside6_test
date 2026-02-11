from collections.abc import Callable

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import (
    QLabel,
    QMainWindow,
    QPlainTextEdit,
    QSplitter,
    QTableView,
    QVBoxLayout,
    QWidget,
)


class MainWindow(QMainWindow):
    """
    画面構築に専念するView層。

    データ取得や表示内容の変換ロジックは持たず、
    Controllerから渡されたModel/コールバックを配線する。
    """

    def __init__(self) -> None:
        """メインウィンドウのUI部品を生成し、上下2ペインを構築する。"""
        super().__init__()
        self.setWindowTitle("SQLite 3,000,000 rows viewer")
        self.resize(900, 600)

        # 上ペイン: 一覧テーブル
        self.table = QTableView(self)
        # 下ペイン: 選択行の詳細表示
        self.detail_value_label = QLabel("value:", self)
        self.detail_value = QPlainTextEdit(self)
        self.detail_value.setReadOnly(True)
        self.detail_value.setPlaceholderText("上の一覧で行を選択すると内容を表示します。")

        # 下ペインを1つのWidgetにまとめ、Splitterへ載せる。
        detail_widget = QWidget(self)
        detail_layout = QVBoxLayout(detail_widget)
        detail_layout.addWidget(self.detail_value_label)
        detail_layout.addWidget(self.detail_value)

        # 上下レイアウトはSplitterでユーザーが比率調整可能にする。
        splitter = QSplitter(Qt.Orientation.Vertical, self)
        splitter.addWidget(self.table)
        splitter.addWidget(detail_widget)
        # 初期比率は上(一覧)を広めに設定。
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        self.setCentralWidget(splitter)

        # closeEvent時にController/Modelの終了処理を呼ぶためのフック。
        self._on_close: Callable[[], None] | None = None

    def set_table_model(self, model) -> None:
        """
        View側の表示設定をまとめて適用する。
        Modelの実データ仕様に依存する判断（id列非表示など）もここで統一。
        """
        self.table.setModel(model)
        # idは内部参照用とし、画面表示はvalue中心にする。
        self.table.setColumnHidden(0, True)
        self.table.setAlternatingRowColors(True)
        # keyset順固定表示のため、ユーザーソートは無効。
        self.table.setSortingEnabled(False)
        self.table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableView.SelectionMode.SingleSelection)
        self.table.verticalHeader().setDefaultSectionSize(22)

    def bind_current_row_changed(self, callback) -> None:
        """
        一覧の選択行変更シグナルへコールバックを接続する。

        Args:
            callback: currentRowChanged(current, previous) を受け取る関数。
        """
        # 行選択変更イベントをControllerへ委譲する。
        self.table.selectionModel().currentRowChanged.connect(callback)

    def select_first_row_if_available(self) -> None:
        """モデルにデータがある場合、先頭行を選択する。"""
        # 初期表示時に最初の行を選択し、下ペインへ内容を出しやすくする。
        model = self.table.model()
        if model is not None and model.rowCount() > 0:
            self.table.selectRow(0)

    def set_detail_text(self, text: str) -> None:
        """
        詳細ペインへテキストを表示する。

        Args:
            text: 表示対象の文字列。
        """
        # 下ペインへ表示文字列を反映する。
        self.detail_value.setPlainText(text)

    def clear_detail_text(self) -> None:
        """詳細ペインを空文字でクリアする。"""
        # 選択解除時の表示クリア。
        self.detail_value.setPlainText("")

    def get_row_data_indexes(self, row: int) -> tuple[QModelIndex, QModelIndex]:
        """
        指定行のid/value列indexを返す。

        Args:
            row: モデル全体での行番号。

        Returns:
            (id列index, value列index) のタプル。
        """
        # 同一行のid列(0)とvalue列(1)のindexを返す。
        model = self.table.model()
        return model.index(row, 0), model.index(row, 1)

    def set_on_close(self, callback: Callable[[], None]) -> None:
        """
        ウィンドウ終了時に実行するクリーンアップ処理を登録する。

        Args:
            callback: 引数なしのコールバック。
        """
        # Window close時に実行するクリーンアップ処理を登録。
        self._on_close = callback

    def closeEvent(self, event) -> None:
        """
        Qtのcloseイベントを処理し、登録済み終了処理を実行する。

        Args:
            event: Qtが渡すcloseイベントオブジェクト。
        """
        # DB接続closeなどをController側で登録したコールバックへ委譲。
        if self._on_close is not None:
            self._on_close()
        super().closeEvent(event)
