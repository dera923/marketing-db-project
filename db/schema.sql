-- usersテーブル（お客さん情報）
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY, -- お客さんごとの番号（自動的に増える）
    email VARCHAR(255) UNIQUE NOT NULL, -- メールアドレス（重複禁止）
    first_name VARCHAR(100), -- 名前
    last_name VARCHAR(100), -- 苗字
    age INT CHECK (age >= 0), -- 年齢（マイナスにはならない）
    gender VARCHAR(20), -- 性別
    region VARCHAR(100), -- 住んでいる地域
    membership_status VARCHAR(50), -- 会員レベル（ゴールド、シルバーなど）
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 登録した日時
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- 情報を更新した日時
);

-- web_eventsテーブル（Webでの行動記録）
CREATE TABLE web_events (
    event_id SERIAL PRIMARY KEY, -- イベントの番号
    user_id INT REFERENCES users(user_id), -- どのお客さんの行動か（usersテーブルと連携）
    session_id VARCHAR(255), -- セッションID（一連の行動をまとめるID）
    event_type VARCHAR(50), -- 何をしたか（ページ閲覧、クリックなど）
    page_url VARCHAR(255), -- どのページを見たか
    referrer_url VARCHAR(255), -- どこから来たか
    user_agent TEXT, -- 使っているブラウザや端末の情報
    ip_address VARCHAR(45), -- IPアドレス
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- いつ行動したか
);

-- salesテーブル（売上情報）
CREATE TABLE sales (
    sale_id SERIAL PRIMARY KEY, -- 売上の番号
    user_id INT REFERENCES users(user_id), -- どのお客さんが買ったか
    product_id INT, -- 何を買ったか
    product_category VARCHAR(100), -- 商品のカテゴリ
    amount DECIMAL(10, 2), -- いくらで買ったか
    quantity INT DEFAULT 1, -- 何個買ったか
    sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- いつ買ったか
);

-- web_eventsテーブル用のインデックス
CREATE INDEX idx_web_events_user_id ON web_events(user_id);
CREATE INDEX idx_web_events_event_time ON web_events(event_time);

-- salesテーブル用のインデックス
CREATE INDEX idx_sales_user_id ON sales(user_id);
CREATE INDEX idx_sales_product_id ON sales(product_id);
CREATE INDEX idx_sales_sale_date ON sales(sale_date);
