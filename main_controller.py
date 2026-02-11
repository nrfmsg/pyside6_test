from pathlib import Path

from PySide6.QtCore import QModelIndex, Qt

from ..model.sqlite_table_model import SqliteTableModel
from ..view.main_window import MainWindow


class MainController:
    """
    ModelとViewを接続し、UIイベントに対する振る舞いを定義するController層。
    """

    def __init__(self, db_path: Path) -> None:
        """
        Controllerを初期化してModel/Viewを接続する。

        Args:
            db_path: 表示対象SQLiteファイルのパス。
        """
        # Controllerが依存オブジェクトの生成・配線を一元管理する。
        self.model = SqliteTableModel(db_path)
        self.view = MainWindow()

        # Viewの表示対象Modelを設定し、選択イベントをハンドラへ接続する。
        self.view.set_table_model(self.model)
        self.view.bind_current_row_changed(self.on_current_row_changed)
        # Window close時にModelのDB接続を確実に閉じる。
        self.view.set_on_close(self.model.close)
        # 起動直後に先頭行を選択し、詳細ペインに内容を出す。
        self.view.select_first_row_if_available()

    def transform_detail_data(self, row_id: int, row_value: str) -> str:
        """
        詳細表示の変換拡張ポイント。

        必要になった時点で派生クラスや直接実装で利用する。

        Args:
            row_id: 選択行の主キーid。
            row_value: 選択行の文字列値。

        Returns:
            下ペインに表示する文字列。
        """
        raise NotImplementedError

    def on_current_row_changed(self, current: QModelIndex, previous: QModelIndex) -> None:
        """
        一覧の選択行変更イベントを処理し、下ペイン表示を更新する。

        Args:
            current: 新しく選択された行index。
            previous: 直前に選択されていた行index。
        """
        # previousは現状不要だが、Qtシグネチャ互換のため受け取る。
        del previous
        if not current.isValid():
            # 選択解除時は詳細ペインを空に戻す。
            self.view.clear_detail_text()
            return

        # 同じ行のid/valueを取得し、必要なら表示用へ変換する。
        id_index, value_index = self.view.get_row_data_indexes(current.row())
        row_id = self.model.data(id_index, Qt.DisplayRole)
        row_value = self.model.data(value_index, Qt.DisplayRole)

        try:
            display_value = self.transform_detail_data(int(row_id), str(row_value))
        except NotImplementedError:
            # 変換処理未実装時は生データをそのまま表示。
            display_value = str(row_value)

        self.view.set_detail_text(display_value)
