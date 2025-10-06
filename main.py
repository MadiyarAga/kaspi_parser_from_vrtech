import json
import psycopg2


def save_to_postgres(product_data):
    conn = psycopg2.connect(
        dbname="kaspi_parser",
        user="postgres",
        password="12345",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO products (name, category, min_price, max_price, rating, review_count)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        product_data["name"],
        product_data["category"],
        product_data["min_price"],
        product_data["max_price"],
        product_data["rating"],
        product_data["review_count"]
    ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Данные успешно сохранены в PostgreSQL.")


def main():
    # читаем product.json, который ты уже создавал
    with open("export/product.json", "r", encoding="utf-8") as f:
        product_data = json.load(f)

    save_to_postgres(product_data)


if __name__ == "__main__":
    main()
