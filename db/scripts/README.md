# サンプルデータ生成スクリプト

## 概要
このスクリプトは、マーケティング分析・AI・クラウド活用のための高品質ダミーデータを自動生成します。
「ユーザー」「イベント」「売上」の3種類のデータを、リアルな分布と相互関連性を持って生成します。

## 特徴
- 💯 100万件規模の大量データも高速生成
- 🔄 データの一貫性を自動的に保証（ユーザーID参照など）
- 📊 現実的な分布のデータ生成（年齢・性別・地域など）
- 📂 CSV/Parquet両形式で出力可能

## 使い方

### 基本的な使い方
```bash
# 設定ファイルを使ってデータ生成
python scripts/generate_sample_data.py --config scripts/config.yaml

# データ量を指定して生成
python scripts/generate_sample_data.py --n_users 50000 --n_events 500000 --n_sales 100000

# 生成後にPostgreSQLにインポート
python scripts/generate_sample_data.py --db_import
