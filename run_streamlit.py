#!/usr/bin/env python3
"""
Streamlitアプリケーション実行スクリプト
"""

import subprocess
import sys
import os

def main():
    """Streamlitアプリケーションを実行"""
    # 現在のディレクトリを取得
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # srcディレクトリに移動
    src_dir = os.path.join(current_dir, "src")
    
    # Streamlitアプリケーションを実行
    cmd = [
        sys.executable, "-m", "streamlit", "run", 
        os.path.join(src_dir, "streamlit_app.py"),
        "--server.port", "8501",
        "--server.address", "localhost"
    ]
    
    print("🌤️ Streamlitアプリケーションを起動中...")
    print(f"URL: http://localhost:8501")
    print("Ctrl+C で停止できます")
    
    try:
        subprocess.run(cmd, cwd=current_dir)
    except KeyboardInterrupt:
        print("\nアプリケーションを停止しました。")
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main() 