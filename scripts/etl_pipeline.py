# scripts/etl_pipeline.py
"""
Polarsを使用した高速ETLパイプライン
- 大規模データの高速処理
- 欠損値補完・型変換・集計
- CSV/Parquet双方向対応
"""

import polars as pl
import pandas as pd
from pathlib import Path
import sys
import argparse
from typing import Optional
import logging
from datetime import datetime

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def read_data(file_path: str) -> pl.DataFrame:
    """データ読み込み（CSV/Parquet対応）"""
    logger.info(f"📂 データを読み込み中: {file_path}")
    
    if not Path(file_path).exists():
        raise FileNotFoundError(f"ファイルが見つかりません: {file_path}")
    
    if file_path.endswith('.csv'):
        df = pl.read_csv(file_path)
    elif file_path.endswith('.parquet'):
        df = pl.read_parquet(file_path)
    else:
        raise ValueError("サポートされていないファイル形式です（CSV/Parquetのみ）")
    
    logger.info(f"✅ 読み込み完了: {df.height:,}行 × {df.width}列")
    return df

def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    """データクリーニング"""
    logger.info("🧹 データクリーニングを実行中...")
    
    initial_rows = df.height
    
    # 欠損値処理
    df_cleaned = df.drop_nulls()
    
    # 重複除去
    df_cleaned = df_cleaned.unique()
    
    # 基本的な型変換とバリデーション
    if 'amount' in df.columns:
        # 負の金額を0に修正
        df_cleaned = df_cleaned.with_columns([
            pl.when(pl.col('amount') < 0)
            .then(0)
            .otherwise(pl.col('amount'))
            .alias('amount')
        ])
    
    # 日付列がある場合の処理
    date_cols = [col for col in df.columns if 'date' in col.lower() or col in ['created_at', 'updated_at']]
    for col in date_cols:
        try:
            df_cleaned = df_cleaned.with_columns([
                pl.col(col).str.to_datetime().alias(col)
            ])
        except:
            logger.warning(f"⚠️  日付変換に失敗: {col}")
    
    removed_rows = initial_rows - df_cleaned.height
    logger.info(f"✅ クリーニング完了: {removed_rows:,}行を除去")
    
    return df_cleaned

def transform_data(df: pl.DataFrame) -> pl.DataFrame:
    """データ変換・集計"""
    logger.info("⚡ データ変換を実行中...")
    
    df_transformed = df
    
    # 金額関連の変換
    if 'amount' in df.columns:
        df_transformed = df_transformed.with_columns([
            # 金額のログ変換
            (pl.col('amount') + 1).log().alias('amount_log'),
            # 金額の標準化
            ((pl.col('amount') - pl.col('amount').mean()) / pl.col('amount').std()).alias('amount_normalized')
        ])
    
    # ユーザー別集計（user_idがある場合）
    if 'user_id' in df.columns and 'amount' in df.columns:
        user_agg = df.group_by('user_id').agg([
            pl.col('amount').sum().alias('user_total_amount'),
            pl.col('amount').mean().alias('user_avg_amount'),
            pl.col('amount').count().alias('user_transaction_count'),
            pl.col('amount').max().alias('user_max_amount')
        ])
        
        # 元データと結合
        df_transformed = df_transformed.join(user_agg, on='user_id', how='left')
    
    # 日付関連の特徴量生成
    if 'created_at' in df.columns:
        df_transformed = df_transformed.with_columns([
            pl.col('created_at').dt.year().alias('year'),
            pl.col('created_at').dt.month().alias('month'),
            pl.col('created_at').dt.weekday().alias('weekday'),
            pl.col('created_at').dt.hour().alias('hour')
        ])
    
    new_cols = df_transformed.width - df.width
    logger.info(f"✅ 変換完了: {new_cols}列を追加")
    
    return df_transformed

def save_data(df: pl.DataFrame, file_path: str) -> None:
    """データ保存（CSV/Parquet対応）"""
    logger.info(f"💾 データを保存中: {file_path}")
    
    # 出力ディレクトリが存在しない場合は作成
    output_dir = Path(file_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if file_path.endswith('.csv'):
        df.write_csv(file_path)
    elif file_path.endswith('.parquet'):
        df.write_parquet(file_path)
    else:
        raise ValueError("サポートされていない出力形式です（CSV/Parquetのみ）")
    
    # ファイルサイズを取得
    file_size = Path(file_path).stat().st_size / (1024 * 1024)  # MB
    logger.info(f"✅ 保存完了: {file_path} ({file_size:.2f}MB)")

def run_etl_pipeline(input_path: str, output_path: str) -> dict:
    """ETLパイプライン実行"""
    start_time = datetime.now()
    logger.info("🚀 ETLパイプラインを開始します")
    
    try:
        # Extract: データ読み込み
        df = read_data(input_path)
        
        # Transform: データ変換
        df_cleaned = clean_data(df)
        df_transformed = transform_data(df_cleaned)
        
        # Load: データ保存
        save_data(df_transformed, output_path)
        
        # 実行結果のサマリー
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        summary = {
            'input_file': input_path,
            'output_file': output_path,
            'input_rows': df.height,
            'output_rows': df_transformed.height,
            'input_columns': df.width,
            'output_columns': df_transformed.width,
            'processing_time_seconds': processing_time,
            'rows_removed': df.height - df_transformed.height,
            'columns_added': df_transformed.width - df.width
        }
        
        logger.info(f"🎊 ETLパイプライン完了!")
        logger.info(f"📊 処理時間: {processing_time:.2f}秒")
        logger.info(f"📈 {summary['input_rows']:,}行 → {summary['output_rows']:,}行")
        logger.info(f"📋 {summary['input_columns']}列 → {summary['output_columns']}列")
        
        return summary
        
    except Exception as e:
        logger.error(f"❌ ETLパイプライン実行中にエラーが発生: {e}")
        raise

def compare_with_pandas(file_path: str) -> None:
    """Pandas vs Polars性能比較"""
    if not Path(file_path).exists():
        logger.warning("⚠️  比較用ファイルが見つかりません")
        return
    
    logger.info("🏃‍♂️ Pandas vs Polars 性能比較を実行中...")
    
    import time
    
    # Pandas処理時間測定
    start_time = time.time()
    df_pandas = pd.read_csv(file_path)
    df_pandas = df_pandas.dropna().drop_duplicates()
    if 'amount' in df_pandas.columns:
        df_pandas['amount_log'] = (df_pandas['amount'] + 1).apply(lambda x: x.__log__())
    pandas_time = time.time() - start_time
    
    # Polars処理時間測定
    start_time = time.time()
    df_polars = (pl.read_csv(file_path)
                .drop_nulls()
                .unique())
    if 'amount' in df_polars.columns:
        df_polars = df_polars.with_columns([
            (pl.col('amount') + 1).log().alias('amount_log')
        ])
    polars_time = time.time() - start_time
    
    speed_improvement = pandas_time / polars_time if polars_time > 0 else float('inf')
    
    logger.info(f"⚡ 性能比較結果:")
    logger.info(f"   Pandas: {pandas_time:.3f}秒")
    logger.info(f"   Polars: {polars_time:.3f}秒")
    logger.info(f"   高速化: {speed_improvement:.1f}倍")

if __name__ == "__main__":
    # コマンドライン引数の解析
    parser = argparse.ArgumentParser(
        description='高速ETLパイプライン（Polars使用）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  python scripts/etl_pipeline.py --input data/raw/sales.csv --output data/processed/sales.parquet
  python scripts/etl_pipeline.py -i data/raw/users.csv -o data/processed/users.parquet --compare
        """
    )
    
    parser.add_argument(
        '--input', '-i', 
        required=True, 
        help='入力ファイルパス (.csv または .parquet)'
    )
    parser.add_argument(
        '--output', '-o', 
        required=True, 
        help='出力ファイルパス (.csv または .parquet)'
    )
    parser.add_argument(
        '--compare', 
        action='store_true',
        help='Pandas vs Polars性能比較を実行'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='詳細ログを表示'
    )
    
    args = parser.parse_args()
    
    # ログレベル設定
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # ETLパイプライン実行
        summary = run_etl_pipeline(args.input, args.output)
        
        # 性能比較（オプション）
        if args.compare:
            compare_with_pandas(args.input)
        
        # 結果表示
        print("\n" + "="*50)
        print("🎊 ETLパイプライン実行完了！")
        print("="*50)
        print(f"入力: {summary['input_file']}")
        print(f"出力: {summary['output_file']}")
        print(f"処理時間: {summary['processing_time_seconds']:.2f}秒")
        print(f"データ変換: {summary['input_rows']:,}行 → {summary['output_rows']:,}行")
        print(f"列追加: +{summary['columns_added']}列")
        print("="*50)
        
    except Exception as e:
        logger.error(f"❌ 実行エラー: {e}")
        sys.exit(1)
