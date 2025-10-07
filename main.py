import requests
import json
import os
import re
from statistics import mean
from pathlib import Path
from db.models import create_tables, save_product_to_db, save_offers_to_db
from logger import log_action



# ========== Функции для получения данных с Kaspi ==========

def extract_product_id(url: str) -> str:
    """Извлекаем product_id из ссылки Kaspi"""

    match = re.search(r'/p/[^/]+-(\d+)/', url)
    if not match:
        raise ValueError("Не удалось извлечь product_id из URL")
    return match.group(1)


def get_offers_data(product_id: str) -> dict:
    """Получаем список офферов (продавцов)"""

    url = f"https://kaspi.kz/yml/offer-view/offers/{product_id}?cityId=750000000"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://kaspi.kz/shop/p/{product_id}/",
        "Content-Type": "application/json;charset=UTF-8",
        "Cookie": "_ga=GA1.1.868895152.1728392994; ssaid=20b43360-8abc-11ef-877f-432ba1616ece; "
                  "test.user.group_exp=93; test.user.group_exp2=10; ks.tg=69; kaspi.storefront.cookie.city=750000000; "
                  "_ga_0R30CM934D=GS1.1.1732526586.3.0.1732526587.59.0.0; "
                  "_ga_6273EB2NKQ=GS1.1.1733489296.1.1.1733489351.0.0.0; locale=ru-RU; "
                  "k_stat=eb2e4138-eb4d-4f4b-9391-02531bafe1d4; "
                  "_hjSessionUser_283363"
                  "=eyJpZCI6IjFiZmIwYmRkLTExZWQtNWViOC1hNzI3LTAzNDUxYWIwYjY0NiIsImNyZWF0ZWQiOjE3MzA3MTU3MTgxNTAsImV4aXN0aW5nIjp0cnVlfQ=="
    }

    payload = {
        "cityId": 750000000,
        "offersList": [str(product_id)]
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()


# ========== Сохранение файлов ==========

def save_product_to_json(product_data: dict, export_dir="export"):
    Path(export_dir).mkdir(parents=True, exist_ok=True)
    filepath = Path(export_dir) / "product.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(product_data, f, ensure_ascii=False, indent=2)
    print(f"Файл {filepath} сохранён.")


def save_offers_to_jsonl(offers: list, export_dir="export"):
    Path(export_dir).mkdir(parents=True, exist_ok=True)
    filepath = Path(export_dir) / "offers.jsonl"
    with open(filepath, "w", encoding="utf-8") as f:
        for offer in offers:
            offer_data = {
                "merchant_name": offer.get("merchantName"),
                "price": offer.get("price"),
                "merchant_rating": offer.get("merchantRating"),
                "merchant_reviews": offer.get("merchantReviewsQuantity"),
                "kaspi_delivery": offer.get("kaspiDelivery"),
            }
            f.write(json.dumps(offer_data, ensure_ascii=False) + "\n")
    print(f"Офферы сохранены в {filepath} ({len(offers)} записей).")


# ========== Основной процесс ==========

def main():
    os.makedirs("export", exist_ok=True)
    create_tables()

    # Читаем ссылку из seed.json
    with open("seed.json", "r", encoding="utf-8") as f:
        url = json.load(f)["product_url"]

    # Извлекаем product_id
    product_id = extract_product_id(url)
    print(f"\nID товара: {product_id}")

    # Получаем офферы и детали товара
    offers_data = get_offers_data(product_id)
    offers = offers_data.get("offers", [])
    if not offers:
        raise ValueError("Не удалось получить офферы от API Kaspi")

    # Извлекаем основную информацию
    product_title = offers[0].get("title")
    category = offers[0].get("masterCategory")

    prices = [o.get("price") for o in offers if o.get("price")]
    min_price = min(prices)
    max_price = max(prices)
    avg_rating = round(mean([o.get("merchantRating") for o in offers if o.get("merchantRating")]), 1)
    total_reviews = sum([o.get("merchantReviewsQuantity") for o in offers if o.get("merchantReviewsQuantity")])
    seller_count = len(offers)

    # Извлекаем product_id
    product_id = extract_product_id(url)
    log_action("extract_product_id", "success", {"url": url, "product_id": product_id})

    # Получаем офферы и детали товара
    offers_data = get_offers_data(product_id)
    offers = offers_data.get("offers", [])
    log_action("fetch_offers", "success", {"offers_count": len(offers)})

    # Формируем результат
    product_data = {
        "name": product_title,
        "category": category,
        "min_price": min_price,
        "max_price": max_price,
        "rating": avg_rating,
        "review_count": total_reviews,
        "seller_count": seller_count,
        "images": [],
        "specs": {}
    }

    # Сохраняем в файлы
    save_product_to_json(product_data)
    save_offers_to_jsonl(offers)
    log_action("export_json", "success", {"product": product_data["name"], "offers_count": len(offers)})

    # Сохраняем в БД
    product_db_id = save_product_to_db(product_data)
    save_offers_to_db(offers, product_db_id)
    log_action("save_db", "success", {"product_db_id": product_db_id, "offers_count": len(offers)})

    print("\nРасширенный сбор данных успешно завершён!")


if __name__ == "__main__":
    main()
