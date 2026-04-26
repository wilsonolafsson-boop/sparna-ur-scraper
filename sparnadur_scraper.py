"""
Sparnaður - Verðskráarskrapar fyrir íslenskar verslanir
========================================================
Keyrir á þínum eigin server (Railway, Render, VPS)
Uppfærir verðgögn á 12-24 klst fresti inn í Supabase

Uppsetning:
    pip install requests beautifulsoup4 playwright supabase python-dotenv schedule
    playwright install chromium

Umhverfisbreytur (.env skrá):
    SUPABASE_URL=https://xxx.supabase.co
    SUPABASE_KEY=your-service-role-key
"""

import os
import json
import time
import logging
import schedule
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ─── Supabase tengill ───────────────────────────────────────────────────────
try:
    from supabase import create_client
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    USE_SUPABASE = True
except Exception:
    log.warning("Supabase ekki tengt — vistar í JSON skrár í staðinn")
    USE_SUPABASE = False

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "is-IS,is;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}


# ═══════════════════════════════════════════════════════════════════════════════
# KRÓNAN SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════
def scrape_kronan():
    """
    Krónan notar JavaScript-rendered síður.
    Við notum Playwright til að fá fullt HTML.
    """
    log.info("🔴 Byrja scraping á Krónan...")
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(extra_http_headers=HEADERS)

        # Krónan hefur leitarsíðu — sækjum algengar flokkar
        categories = [
            ("mjolk-og-rjomi", "Mjólk og rjómi"),
            ("kjot", "Kjöt"),
            ("fiskur", "Fiskur"),
            ("braud-og-kex", "Brauð og kex"),
            ("graenaeti-og-avaxtir", "Grænæti og ávextir"),
            ("mjolkurvara", "Mjólkurvörur"),
        ]

        for slug, category_name in categories:
            try:
                url = f"https://www.kronan.is/islenska/{slug}"
                page.goto(url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(2000)  # bíða eftir JS

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")

                # Finna vörukort — aðlaga selector að raunverulegu HTML
                product_cards = soup.select(".product-card, .product-item, [data-product]")

                for card in product_cards:
                    try:
                        name_el = card.select_one(".product-name, .product-title, h3, h4")
                        price_el = card.select_one(".product-price, .price, [class*='price']")
                        img_el = card.select_one("img")
                        unit_el = card.select_one(".product-unit, .unit-price")

                        if not name_el or not price_el:
                            continue

                        name = name_el.get_text(strip=True)
                        price_text = price_el.get_text(strip=True)
                        price = parse_price(price_text)

                        if price is None or price <= 0:
                            continue

                        products.append({
                            "store": "kronan",
                            "store_name": "Krónan",
                            "name": name,
                            "price": price,
                            "price_text": price_text,
                            "category": category_name,
                            "unit": unit_el.get_text(strip=True) if unit_el else None,
                            "image": img_el.get("src") if img_el else None,
                            "url": url,
                            "scraped_at": datetime.utcnow().isoformat(),
                        })

                    except Exception as e:
                        log.debug(f"Villa við vörukort: {e}")
                        continue

                log.info(f"  Krónan {category_name}: {len([p for p in products if p['category'] == category_name])} vörur")
                time.sleep(1.5)  # vera kurteiss

            except Exception as e:
                log.error(f"Villa við Krónan {slug}: {e}")

        browser.close()

    log.info(f"✅ Krónan: {len(products)} vörur scrapaðar")
    return products


# ═══════════════════════════════════════════════════════════════════════════════
# BÓNUS SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════
def scrape_bonus():
    """
    Bónus — sækjum tilboðasíðuna og vörulista.
    """
    log.info("🟢 Byrja scraping á Bónus...")
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(extra_http_headers=HEADERS)

        # Tilboð síðan — alltaf frítt aðgengileg
        try:
            page.goto("https://www.bonus.is/tilbod", wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)

            html = page.content()
            soup = BeautifulSoup(html, "html.parser")

            # Bónus tilboðakort
            offer_cards = soup.select(".offer-card, .product-card, .offer-item, article")

            for card in offer_cards:
                try:
                    name_el = card.select_one("h2, h3, h4, .title, .name")
                    price_el = card.select_one(".price, .offer-price, [class*='price']")
                    orig_price_el = card.select_one(".original-price, .was-price, [class*='original']")
                    img_el = card.select_one("img")

                    if not name_el or not price_el:
                        continue

                    name = name_el.get_text(strip=True)
                    price_text = price_el.get_text(strip=True)
                    price = parse_price(price_text)

                    orig_price = None
                    if orig_price_el:
                        orig_price = parse_price(orig_price_el.get_text(strip=True))

                    if price is None or price <= 0:
                        continue

                    discount_pct = None
                    if orig_price and orig_price > price:
                        discount_pct = round((1 - price / orig_price) * 100)

                    products.append({
                        "store": "bonus",
                        "store_name": "Bónus",
                        "name": name,
                        "price": price,
                        "price_text": price_text,
                        "original_price": orig_price,
                        "discount_pct": discount_pct,
                        "is_offer": True,
                        "category": "Tilboð",
                        "image": img_el.get("src") if img_el else None,
                        "url": "https://www.bonus.is/tilbod",
                        "scraped_at": datetime.utcnow().isoformat(),
                    })

                except Exception as e:
                    log.debug(f"Villa við Bónus kort: {e}")
                    continue

            log.info(f"  Bónus tilboð: {len(products)} vörur")

        except Exception as e:
            log.error(f"Villa við Bónus: {e}")

        browser.close()

    log.info(f"✅ Bónus: {len(products)} vörur scrapaðar")
    return products


# ═══════════════════════════════════════════════════════════════════════════════
# HAGKAUP SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════
def scrape_hagkaup():
    """
    Hagkaup — sækjum vörulista með Playwright.
    """
    log.info("🔵 Byrja scraping á Hagkaup...")
    products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(extra_http_headers=HEADERS)

        categories = [
            ("matvara", "Matvara"),
            ("drykkir", "Drykkir"),
            ("hreinlaetisvarur", "Hreinlætisvörur"),
        ]

        for slug, category_name in categories:
            try:
                url = f"https://www.hagkaup.is/{slug}"
                page.goto(url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(2000)

                # Scrolla niður til að hlaða fleiri vörur (lazy loading)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1500)

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")

                product_cards = soup.select(".product, .product-card, [class*='product']")

                for card in product_cards:
                    try:
                        name_el = card.select_one(".product-name, h3, h4, .name")
                        price_el = card.select_one(".price, .product-price")
                        img_el = card.select_one("img")

                        if not name_el or not price_el:
                            continue

                        name = name_el.get_text(strip=True)
                        price_text = price_el.get_text(strip=True)
                        price = parse_price(price_text)

                        if price is None or price <= 0:
                            continue

                        products.append({
                            "store": "hagkaup",
                            "store_name": "Hagkaup",
                            "name": name,
                            "price": price,
                            "price_text": price_text,
                            "category": category_name,
                            "image": img_el.get("src") if img_el else None,
                            "url": url,
                            "scraped_at": datetime.utcnow().isoformat(),
                        })

                    except Exception as e:
                        log.debug(f"Villa við Hagkaup kort: {e}")
                        continue

                log.info(f"  Hagkaup {category_name}: {len([p for p in products if p['category'] == category_name])} vörur")
                time.sleep(1.5)

            except Exception as e:
                log.error(f"Villa við Hagkaup {slug}: {e}")

        browser.close()

    log.info(f"✅ Hagkaup: {len(products)} vörur scrapaðar")
    return products


# ═══════════════════════════════════════════════════════════════════════════════
# BENSÍNVERÐ — Gasvaktin / Orkustofnun
# ═══════════════════════════════════════════════════════════════════════════════
def scrape_fuel_prices():
    """
    Sækir bensínverð frá Orkustofnun API (opinbert, opið).
    """
    log.info("⛽ Sækja bensínverð...")
    try:
        # Orkustofnun hefur opinbert gagnasett
        r = requests.get(
            "https://www.os.is/gasvaktin/gasvaktin.json",
            headers=HEADERS,
            timeout=15
        )

        if r.status_code == 200:
            data = r.json()
            prices = []

            for station in data:
                prices.append({
                    "station_id": station.get("key"),
                    "name": station.get("name"),
                    "company": station.get("company"),
                    "petrol": station.get("bensin95"),
                    "diesel": station.get("diesel"),
                    "lat": station.get("geo", {}).get("lat"),
                    "lon": station.get("geo", {}).get("lon"),
                    "updated_at": datetime.utcnow().isoformat(),
                })

            log.info(f"✅ Bensínverð: {len(prices)} stöðvar")
            return prices
        else:
            log.warning(f"Gasvaktin skilaði {r.status_code}")
            return []

    except Exception as e:
        log.error(f"Villa við bensínverð: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# HJÁLPARFÖLL
# ═══════════════════════════════════════════════════════════════════════════════
def parse_price(price_text: str) -> float | None:
    """
    Breytir íslenskt verðtexta í float.
    Dømi: "1.290 kr.", "1290kr", "1,290" → 1290.0
    """
    if not price_text:
        return None
    try:
        # Fjarlægja allt nema tölur og kommu/punkt
        cleaned = price_text.replace(" ", "").replace("\xa0", "")
        cleaned = "".join(c for c in cleaned if c.isdigit() or c in ".,")
        # Íslenskt: punkt = þúsundatákn, komma = decimal
        # Dæmi: "1.290,50" → 1290.50
        if "," in cleaned and "." in cleaned:
            cleaned = cleaned.replace(".", "").replace(",", ".")
        elif "," in cleaned:
            # Ef aðeins komma — gæti verið decimal eða þúsundatákn
            parts = cleaned.split(",")
            if len(parts) == 2 and len(parts[1]) <= 2:
                cleaned = cleaned.replace(",", ".")
            else:
                cleaned = cleaned.replace(",", "")
        elif "." in cleaned:
            parts = cleaned.split(".")
            if len(parts) == 2 and len(parts[1]) <= 2:
                pass  # decimal punkt
            else:
                cleaned = cleaned.replace(".", "")  # þúsundatákn

        return float(cleaned) if cleaned else None
    except (ValueError, AttributeError):
        return None


def normalize_product_name(name: str) -> str:
    """
    Staðlar vöruheiti til að auðvelda samanburð á milli verslana.
    """
    import re
    name = name.lower().strip()
    name = re.sub(r'\s+', ' ', name)
    # Fjarlægja algengar endingar
    name = name.replace(" gr.", "g").replace(" ml.", "ml").replace(" stk.", "")
    return name


def save_to_supabase(products: list, table: str):
    """Vista vörur í Supabase."""
    if not USE_SUPABASE or not products:
        return

    try:
        # Nota upsert til að uppfæra ef vara er til
        batch_size = 100
        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]
            supabase.table(table).upsert(batch).execute()
        log.info(f"  💾 {len(products)} vörur vistaðar í Supabase ({table})")
    except Exception as e:
        log.error(f"Supabase villa: {e}")


def save_to_json(products: list, filename: str):
    """Vista í JSON skrá (ef Supabase er ekki tengt)."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
    log.info(f"  💾 {len(products)} vörur vistaðar í {filename}")


def match_products_across_stores(all_products: list) -> list:
    """
    Finnur sömu vöru hjá mismunandi verslunum og tengir þær saman.
    Skilar lista af samanburðum.
    """
    from collections import defaultdict
    import difflib

    # Hópa vörur eftir staðlað nafn
    by_name = defaultdict(list)
    for p in all_products:
        key = normalize_product_name(p["name"])
        by_name[key].append(p)

    comparisons = []
    for name, items in by_name.items():
        stores = {i["store"]: i["price"] for i in items}
        if len(stores) < 2:
            continue  # þarf að vera í minnst 2 verslunum

        prices = list(stores.values())
        cheapest_store = min(stores, key=stores.get)
        cheapest_price = stores[cheapest_store]
        most_expensive = max(prices)
        savings = round(most_expensive - cheapest_price)

        comparisons.append({
            "product_name": items[0]["name"],
            "normalized_name": name,
            "prices": stores,
            "cheapest_store": cheapest_store,
            "cheapest_price": cheapest_price,
            "max_price": most_expensive,
            "max_savings": savings,
            "store_count": len(stores),
        })

    # Raða eftir mestum sparnaði
    comparisons.sort(key=lambda x: x["max_savings"], reverse=True)
    return comparisons


# ═══════════════════════════════════════════════════════════════════════════════
# AÐALFALL — Keyrir allar scraping
# ═══════════════════════════════════════════════════════════════════════════════
def run_all_scrapers():
    log.info("=" * 60)
    log.info("🚀 Sparnaður scraper byrjar...")
    log.info(f"   Tími: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    all_products = []

    # 1. Krónan
    try:
        kronan = scrape_kronan()
        all_products.extend(kronan)
        if USE_SUPABASE:
            save_to_supabase(kronan, "products")
        else:
            save_to_json(kronan, "kronan_products.json")
    except Exception as e:
        log.error(f"Krónan scraper misheppnaðist: {e}")

    # 2. Bónus
    try:
        bonus = scrape_bonus()
        all_products.extend(bonus)
        if USE_SUPABASE:
            save_to_supabase(bonus, "products")
        else:
            save_to_json(bonus, "bonus_products.json")
    except Exception as e:
        log.error(f"Bónus scraper misheppnaðist: {e}")

    # 3. Hagkaup
    try:
        hagkaup = scrape_hagkaup()
        all_products.extend(hagkaup)
        if USE_SUPABASE:
            save_to_supabase(hagkaup, "products")
        else:
            save_to_json(hagkaup, "hagkaup_products.json")
    except Exception as e:
        log.error(f"Hagkaup scraper misheppnaðist: {e}")

    # 4. Bensínverð
    try:
        fuel = scrape_fuel_prices()
        if USE_SUPABASE:
            save_to_supabase(fuel, "fuel_prices")
        else:
            save_to_json(fuel, "fuel_prices.json")
    except Exception as e:
        log.error(f"Bensínverð scraper misheppnaðist: {e}")

    # 5. Verðsamanburður
    if all_products:
        comparisons = match_products_across_stores(all_products)
        log.info(f"\n📊 Verðsamanburður — Top 10 sparnaður:")
        for c in comparisons[:10]:
            log.info(
                f"  {c['product_name'][:40]:<40} "
                f"Ódýrast: {c['cheapest_store']:<10} "
                f"{c['cheapest_price']:>8.0f} kr  "
                f"(sparar {c['max_savings']:>5.0f} kr)"
            )

        if USE_SUPABASE:
            save_to_supabase(comparisons, "price_comparisons")
        else:
            save_to_json(comparisons, "price_comparisons.json")

    log.info("=" * 60)
    log.info(f"✅ Lokið! {len(all_products)} vörur scrapaðar samtals")
    log.info("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════════
# TÍMASETNING — Keyrir á 12 klst fresti
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    if "--once" in sys.argv:
        # Keyra einu sinni (til að prófa)
        run_all_scrapers()
    else:
        # Keyra á 12 klst fresti (production)
        log.info("⏰ Scraper stilltur á 12 klst fresti (06:00 og 18:00)")
        schedule.every().day.at("06:00").do(run_all_scrapers)
        schedule.every().day.at("18:00").do(run_all_scrapers)

        # Keyra strax við ræsingu
        run_all_scrapers()

        while True:
            schedule.run_pending()
            time.sleep(60)
