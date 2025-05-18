#!/usr/bin/env python3
"""
シンプルなダミーデータ生成スクリプト
"""
import pandas as pd
import random
from datetime import datetime, timedelta
import os

# 設定
random.seed(42)

def generate_users(count=1000):
    """ユーザーデータを生成"""
    print(f"🧑‍🤝‍🧑 ユーザーデータを {count} 件生成中...")
    
    users = []
    for i in range(1, count + 1):
        user = {
            'user_id': i,
            'username': f'user_{i:04d}',
            'age': random.randint(18, 80),
            'gender': random.choice(['male', 'female', 'other']),
            'region': random.choice(['東京', '大阪', '愛知', '福岡', '北海道']),
            'member_status': random.choice(['regular', 'premium', 'gold']),
            'registration_date': (datetime.now() - timedelta(days=random.randint(1, 1000))).strftime('%Y-%m-%d')
        }
        users.append(user)
    
    return pd.DataFrame(users)

def generate_events(users_df, count=5000):
    """イベントデータを生成"""
    print(f"🖱️ イベントデータを {count} 件生成中...")
    
    events = []
    user_ids = users_df['user_id'].tolist()
    
    for i in range(1, count + 1):
        event = {
            'event_id': i,
            'user_id': random.choice(user_ids),
            'event_type': random.choice(['page_view', 'click', 'search', 'purchase']),
            'event_date': (datetime.now() - timedelta(hours=random.randint(1, 720))).strftime('%Y-%m-%d %H:%M:%S'),
            'page_url': f'https://example.com/page{random.randint(1, 100)}'
        }
        events.append(event)
    
    return pd.DataFrame(events)

def generate_sales(users_df, count=2000):
    """売上データを生成"""
    print(f"💰 売上データを {count} 件生成中...")
    
    sales = []
    user_ids = users_df['user_id'].tolist()
    
    for i in range(1, count + 1):
        sale = {
            'sale_id': i,
            'user_id': random.choice(user_ids),
            'total_amount': random.randint(500, 50000),
            'category': random.choice(['electronics', 'clothing', 'books', 'food']),
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal']),
            'sale_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d')
        }
        sales.append(sale)
    
    return pd.DataFrame(sales)

def main():
    """メイン処理"""
    print("🚀 === ダミーデータ生成開始 ===")
    
    # データ生成
    users_df = generate_users(1000)
    events_df = generate_events(users_df, 5000)
    sales_df = generate_sales(users_df, 2000)
    
    # 保存ディレクトリを作成（../data にする）
    data_dir = '../data'
    os.makedirs(data_dir, exist_ok=True)
    
    # CSVファイルとして保存
    users_df.to_csv(f'{data_dir}/users.csv', index=False, encoding='utf-8')
    events_df.to_csv(f'{data_dir}/web_events.csv', index=False, encoding='utf-8')
    sales_df.to_csv(f'{data_dir}/sales.csv', index=False, encoding='utf-8')
    
    print("✅ === 生成完了 ===")
    print(f"📊 ユーザー: {len(users_df)} 件")
    print(f"🖱️ イベント: {len(events_df)} 件")
    print(f"💰 売上: {len(sales_df)} 件")
    print(f"💾 データは {data_dir}/ フォルダに保存されました")

if __name__ == "__main__":
    main()
