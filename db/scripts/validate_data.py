#!/usr/bin/env python3
"""
生成されたデータの検証スクリプト
"""
import pandas as pd
import os

def validate_file(filepath, expected_columns=None):
    """CSVファイルを検証"""
    if not os.path.exists(filepath):
        print(f"❌ ファイルが見つかりません: {filepath}")
        return False
    
    try:
        df = pd.read_csv(filepath)
        print(f"✅ {filepath}: {len(df)} 行, {len(df.columns)} 列")
        
        # 先頭5行を表示
        print("   先頭5行:")
        print(df.head().to_string(index=False))
        
        # カラムの確認
        print(f"   カラム: {list(df.columns)}")
        
        return True
    
    except Exception as e:
        print(f"❌ {filepath} の読み込みでエラー: {e}")
        return False

def main():
    """メイン処理"""
    print("🔍 === データ検証開始 ===")
    
    # データディレクトリのパス（現在 /db/scripts にいるので ../data）
    data_dir = '../data'
    
    # 各ファイルを検証
    validate_file(f'{data_dir}/users.csv')
    print()
    validate_file(f'{data_dir}/web_events.csv')
    print()
    validate_file(f'{data_dir}/sales.csv')
    
    print("✅ === 検証完了 ===")

if __name__ == "__main__":
    main()
