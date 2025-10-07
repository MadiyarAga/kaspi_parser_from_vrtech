import json
from db.utils import connect_db

# ========== Работа с PostgreSQL ==========


def create_tables():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name TEXT,
            category TEXT,
            min_price NUMERIC,
            max_price NUMERIC,
            rating NUMERIC,
            review_count INTEGER,
            seller_count INTEGER,
            images JSONB,
            specs JSONB
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS offers (
            id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
            merchant_name TEXT,
            price NUMERIC,
            merchant_rating NUMERIC,
            merchant_reviews INTEGER,
            kaspi_delivery BOOLEAN
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Таблицы products и offers созданы в БД.")


def save_product_to_db(product_data):
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM products WHERE name = %s;", (product_data["name"],))
    existing = cursor.fetchone()
    if existing:
        product_id = existing[0]
        cursor.execute("""
            UPDATE products
            SET category = %s,
                min_price = %s,
                max_price = %s,
                rating = %s,
                review_count = %s,
                seller_count = %s,
                images = %s,
                specs = %s
            WHERE id = %s;
        """, (
            product_data["category"],
            product_data["min_price"],
            product_data["max_price"],
            product_data["rating"],
            product_data["review_count"],
            product_data["seller_count"],
            json.dumps(product_data.get("images")),
            json.dumps(product_data.get("specs")),
            product_id
        ))
    else:
        cursor.execute("""
            INSERT INTO products (name, category, min_price, max_price, rating, review_count, seller_count, images, specs)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """, (
            product_data["name"],
            product_data["category"],
            product_data["min_price"],
            product_data["max_price"],
            product_data["rating"],
            product_data["review_count"],
            product_data["seller_count"],
            json.dumps(product_data.get("images")),
            json.dumps(product_data.get("specs"))
        ))
        product_id = cursor.fetchone()[0]

    conn.commit()
    cursor.close()
    conn.close()
    return product_id


def save_offers_to_db(offers, product_id):
    conn = connect_db()
    cursor = conn.cursor()
    for offer in offers:
        cursor.execute("""
            SELECT id FROM offers WHERE product_id=%s AND merchant_name=%s;
        """, (product_id, offer.get("merchantName")))
        if cursor.fetchone():
            continue
        cursor.execute("""
            INSERT INTO offers (product_id, merchant_name, price, merchant_rating, merchant_reviews, kaspi_delivery)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            product_id,
            offer.get("merchantName"),
            offer.get("price"),
            offer.get("merchantRating"),
            offer.get("merchantReviewsQuantity"),
            offer.get("kaspiDelivery")
        ))
    conn.commit()
    cursor.close()
    conn.close()
