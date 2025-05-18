-- マスターデータの挿入
INSERT INTO products (product_id, product_name, product_category, price) VALUES 
(101, 'Tシャツ', '衣類', 2500.00),
(102, 'スニーカー', '靴', 8000.00),
(103, 'スマートフォン', '電子機器', 15000.00),
(104, 'チョコレートセット', '食品', 1200.00),
(105, '化粧水', '化粧品', 3500.00),
(106, 'テニスラケット', 'スポーツ用品', 6000.00),
(107, 'プログラミング入門書', '本', 1500.00);

-- シーケンス値を調整
SELECT setval(pg_get_serial_sequence('products', 'product_id'), 
              (SELECT MAX(product_id) FROM products));

-- ユーザーデータの挿入
INSERT INTO users (user_id, email, first_name, last_name, age, gender) VALUES 
(1, 'tanaka@example.com', '太郎', '田中', 35, '男性'),
(2, 'yamada@example.com', '花子', '山田', 28, '女性'),
(3, 'suzuki@example.com', '一郎', '鈴木', 42, '男性'),
(4, 'sato@example.com', '美咲', '佐藤', 25, '女性'),
(5, 'watanabe@example.com', '健太', '渡辺', 31, '男性');

-- シーケンス値を調整
SELECT setval(pg_get_serial_sequence('users', 'user_id'), 
              (SELECT MAX(user_id) FROM users));

-- Web行動データの挿入
INSERT INTO web_events (
    user_id, 
    session_id, 
    event_type, 
    page_url, 
    referrer_url, 
    user_agent, 
    ip_address,
    event_time
) VALUES 
(1, 'sess_abc123', 'pageview', 'https://example.com/home', 'https://google.com', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', '192.168.1.1', '2024-05-01 08:30:00'),
(1, 'sess_abc123', 'click', 'https://example.com/products', 'https://example.com/home', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', '192.168.1.1', '2024-05-01 08:32:15'),
(2, 'sess_def456', 'pageview', 'https://example.com/products/clothing', 'https://example.com/home', 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0)', '192.168.1.2', '2024-05-01 09:45:00'),
(3, 'sess_ghi789', 'pageview', 'https://example.com/cart', 'https://example.com/products', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', '192.168.1.3', '2024-05-01 10:15:30'),
(3, 'sess_ghi789', 'purchase', 'https://example.com/checkout', 'https://example.com/cart', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', '192.168.1.3', '2024-05-01 10:20:45'),
(4, 'sess_jkl012', 'pageview', 'https://example.com/blog', 'https://facebook.com', 'Mozilla/5.0 (iPad; CPU OS 14_0)', '192.168.1.4', '2024-05-01 11:05:00'),
(5, 'sess_mno345', 'search', 'https://example.com/search?q=shoes', 'https://example.com/home', 'Mozilla/5.0 (Android 11; Mobile)', '192.168.1.5', '2024-05-01 13:40:20');

-- 売上データの挿入
INSERT INTO sales (
    user_id, 
    product_id, 
    amount, -- またはunit_price（実際のカラム名に合わせる）
    quantity, 
    sale_date
) VALUES 
(1, 101, 2500.00, 1, '2024-05-01 08:45:00'),
(2, 102, 8000.00, 1, '2024-05-01 10:30:00'),
(3, 103, 15000.00, 1, '2024-05-01 12:15:00'),
(3, 104, 1200.00, 2, '2024-05-02 14:20:00'),
(4, 105, 3500.00, 1, '2024-05-02 16:45:00'),
(5, 106, 6000.00, 1, '2024-05-03 09:10:00'),
(1, 107, 1500.00, 3, '2024-05-03 11:30:00');
