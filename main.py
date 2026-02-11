import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication

# このモジュールは2通りの実行方法に対応する:
# 1) python -m mvc_keyset_app.main   (パッケージ実行)
# 2) python mvc_keyset_app/main.py   (直接実行)
# 直接実行では相対importが使えないため、プロジェクトルートをsys.pathへ追加して
# 絶対importへ切り替える。
if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from mvc_keyset_app.config import DB_PATH
    from mvc_keyset_app.controller.main_controller import MainController
    from mvc_keyset_app.model.database import ensure_database
else:
    from .config import DB_PATH
    from .controller.main_controller import MainController
    from .model.database import ensure_database


def main() -> None:
    """
    アプリ全体の起動シーケンスを実行する。

    1) SQLiteデータベースの準備
    2) Qtアプリケーション作成
    3) MVCのController生成と画面表示
    4) イベントループ開始
    """
    # 起動前にDBの存在と件数を保証する。
    # 既に十分な件数がある場合は即returnするため、再実行時コストは小さい。
    print("Preparing database (if needed)...")
    ensure_database()
    print(f"Database ready: {DB_PATH.resolve()}")

    # QApplicationはQt GUIアプリの必須エントリ。
    # sys.argvを渡すことで、Qt標準オプションを受け取れる。
    app = QApplication(sys.argv)
    # ControllerがModel/Viewの接続をまとめて担当する。
    controller = MainController(DB_PATH)
    controller.view.show()
    # Qtイベントループを開始し、終了コードをOSへ返す。
    sys.exit(app.exec())


if __name__ == "__main__":
    # スクリプト直実行時のエントリポイント。
    main()
