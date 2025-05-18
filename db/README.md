# マーケティングデータベース設計

## 概要
このデータベースはマーケティング分析のための「ユーザー／イベント／売上」データを管理します。

## 設計原則
- 第1〜第3正規形を適用し、データの重複を排除
- 外部キー制約で参照整合性を確保
- よく使うクエリ向けにインデックスを設計

## テーブル構成

### マスターテーブル
- `prefectures`: 都道府県と地域情報
- `membership_levels`: 会員ランク情報
- `products`: 商品情報

### トランザクションテーブル
- `users`: ユーザー基本情報
- `user_hobbies`: ユーザーの趣味（1対多）
- `web_events`: Webサイト上の行動ログ
- `sales`: 売上データ

## ER図
[ER図はこちら](er.mmd)

## セットアップ方法
```bash
# PostgreSQLコンテナの起動
docker run --name marketing-db -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=password -e POSTGRES_DB=marketing -p 5432:5432 -d postgres:15

# スキーマの適用
psql -h localhost -p 5432 -U admin -d marketing -f schema.sql
