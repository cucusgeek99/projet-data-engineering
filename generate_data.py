#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Générateur des jeux de données du Projet Final Spark & Scala (version groupe de 3).

Produit dans le répertoire de sortie :
  - merchants.csv     (CSV)
  - users.json        (JSON Lines)
  - products.parquet  (répertoire Parquet partitionné, style Spark)
  - transactions.csv  (CSV)

Des anomalies sont volontairement injectées (valeurs invalides, valeurs nulles,
clés étrangères orphelines) afin que les questions de validation et de qualité
des données produisent des résultats non triviaux.

Usage : python generate_data.py <repertoire_de_sortie> [--seed 2026]
"""

import argparse
import csv
import json
import os
import random
import shutil
import uuid
from datetime import date, datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------- paramètres

N_MERCHANTS = 600
N_PRODUCTS = 6000
N_USERS = 12000
N_PARQUET_PARTS = 12

TX_START = date(2024, 1, 1)
TX_END = date(2025, 12, 31)

CATEGORIES = [
    "Electronics", "Books", "Food", "Clothing", "Automotive",
    "Home & Garden", "Toys", "Health", "Beauty", "Sports",
]

REGIONS = [
    "Ile-de-France", "Auvergne-Rhône-Alpes", "Nouvelle-Aquitaine", "Occitanie",
    "Hauts-de-France", "Grand Est", "Provence-Alpes-Côte d'Azur",
    "Pays de la Loire", "Normandie", "Bretagne",
]

CITIES = [
    "Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Montpellier",
    "Strasbourg", "Bordeaux", "Lille", "Rennes", "Reims", "Saint-Etienne",
    "Toulon", "Le Havre", "Grenoble", "Dijon", "Angers", "Nîmes", "Villeurbanne",
]

PAYMENT_METHODS = ["CARD", "CASH", "PAYPAL", "BANK_TRANSFER", "CRYPTO"]
PAYMENT_WEIGHTS = [0.34, 0.16, 0.22, 0.20, 0.08]

SEGMENTS = ["Budget", "Standard", "Premium", "VIP"]
SEGMENT_WEIGHTS = [0.30, 0.36, 0.24, 0.10]

MERCHANT_PREFIXES = {
    "Electronics": ["DigitalHub", "TechStore", "GadgetShop", "ElectroWorld"],
    "Books": ["LibraryShop", "BookCorner", "BookHaven", "ReadMore"],
    "Food": ["FoodMart", "FoodHub", "FreshMarket", "GourmetShop"],
    "Clothing": ["TrendyWear", "ClothingHub", "StyleShop", "FashionStore"],
    "Automotive": ["CarParts", "AutoShop", "MotorHub", "AutoCenter"],
    "Home & Garden": ["GardenWorld", "DecorShop", "HomeCenter", "HomeStyle"],
    "Toys": ["KidsWorld", "ToyLand", "ToyHub", "PlayShop"],
    "Health": ["HealthShop", "WellnessStore", "HealthHub", "VitalShop"],
    "Beauty": ["BeautyWorld", "CosmeticHub", "GlowShop", "BeautyCorner"],
    "Sports": ["SportZone", "ActiveWear", "SportHub", "FitnessShop"],
}

PRODUCT_ITEMS = {
    "Electronics": ["Smartphone", "Tablet", "Laptop", "Smart TV", "Headphones", "Camera"],
    "Books": ["Novel", "Cookbook", "Biography", "Comic Book", "Textbook", "Atlas"],
    "Food": ["Coffee Pack", "Olive Oil", "Chocolate Box", "Cheese Platter", "Tea Set", "Pasta Box"],
    "Clothing": ["Sneakers", "Jacket", "Jeans", "T-Shirt", "Dress", "Scarf"],
    "Automotive": ["Car Parts", "Tyre Set", "Car Battery", "Dash Cam", "Roof Rack", "Oil Filter"],
    "Home & Garden": ["Sofa", "Lamp", "Garden Chair", "Cookware Set", "Rug", "Plant Pot"],
    "Toys": ["Board Game", "Puzzle", "Action Figure", "Building Blocks", "Toy Car", "Doll"],
    "Health": ["Vitamins", "Blood Monitor", "First Aid Kit", "Massage Gun", "Scale", "Thermometer"],
    "Beauty": ["Face Cream", "Perfume", "Lipstick", "Shampoo", "Nail Kit", "Serum"],
    "Sports": ["Running Shoes", "Yoga Mat", "Dumbbells", "Bicycle Helmet", "Tennis Racket", "Backpack"],
}

PRODUCT_TIERS = ["Classic", "Pro", "Elite", "Standard", "Premium", "Essential"]

PRICE_RANGES = {
    "Electronics": (49.0, 2200.0),
    "Books": (4.5, 65.0),
    "Food": (2.0, 90.0),
    "Clothing": (12.0, 320.0),
    "Automotive": (18.0, 1600.0),
    "Home & Garden": (9.0, 850.0),
    "Toys": (5.0, 160.0),
    "Health": (4.0, 220.0),
    "Beauty": (5.0, 165.0),
    "Sports": (9.0, 540.0),
}

# taux d'anomalies injectées
ANOMALIES = {
    "tx_amount_invalide": 1200,
    "tx_timestamp_invalide": 700,
    "tx_user_orphelin": 400,
    "tx_produit_orphelin": 300,
    "tx_marchand_orphelin": 250,
    "tx_location_nulle": 500,
    "tx_payment_nul": 300,
    "user_age_invalide": 220,
    "user_revenu_invalide": 130,
    "user_ville_nulle": 160,
    "product_prix_invalide": 85,
    "product_rating_invalide": 95,
    "product_nom_nul": 55,
    "merchant_commission_invalide": 14,
    "merchant_region_nulle": 9,
}


def month_key(d):
    return d.year * 12 + d.month


def add_months(d, n):
    total = month_key(d) - 1 + n
    return date(total // 12, total % 12 + 1, 1)


def random_day_in_month(rng, first_of_month, lo, hi):
    """Un jour aléatoire dans le mois, borné à [lo, hi]."""
    nxt = add_months(first_of_month, 1)
    span = (nxt - first_of_month).days
    d = first_of_month + timedelta(days=rng.randrange(span))
    if d < lo:
        d = lo
    if d > hi:
        d = hi
    return d


def random_time(rng, night_bias=False):
    """Heure de la journée, distribution réaliste (creux la nuit)."""
    if night_bias:
        hour = rng.choice([22, 23, 0, 1, 2, 3, 4])
    else:
        hours = list(range(24))
        weights = [
            1, 1, 1, 1, 1, 2,      # 0h-5h
            4, 7, 9, 11, 12, 12,   # 6h-11h
            11, 10, 10, 11, 12, 13,  # 12h-17h
            14, 13, 11, 8, 5, 3,   # 18h-23h
        ]
        hour = rng.choices(hours, weights=weights)[0]
    return hour, rng.randrange(60), rng.randrange(60)


def ts_str(d, hour, minute, second):
    return "%04d%02d%02d%02d%02d%02d" % (d.year, d.month, d.day, hour, minute, second)


# ---------------------------------------------------------------- marchands

def build_merchants(rng):
    rows = []
    for i in range(1, N_MERCHANTS + 1):
        cat = rng.choice(CATEGORIES)
        prefix = rng.choice(MERCHANT_PREFIXES[cat])
        est = date(2017, 1, 1) + timedelta(days=rng.randrange(2400))
        rows.append({
            "merchant_id": "M%05d" % i,
            "name": "%s %d" % (prefix, i),
            "category": cat,
            "region": rng.choices(REGIONS, weights=[18, 14, 11, 10, 9, 8, 9, 8, 7, 6])[0],
            "commission_rate": round(rng.uniform(0.01, 0.10), 4),
            "establishment_date": est.strftime("%Y%m%d"),
        })

    idx = rng.sample(range(len(rows)), ANOMALIES["merchant_commission_invalide"])
    for k in idx:
        rows[k]["commission_rate"] = rng.choice([-0.05, 1.25, 1.5, -0.01, 2.0])
    for k in rng.sample(range(len(rows)), ANOMALIES["merchant_region_nulle"]):
        rows[k]["region"] = ""
    return rows


# ---------------------------------------------------------------- produits

def build_products(rng, merchants):
    by_cat = {}
    for m in merchants:
        by_cat.setdefault(m["category"], []).append(m["merchant_id"])

    rows = []
    for i in range(1, N_PRODUCTS + 1):
        cat = rng.choice(CATEGORIES)
        lo, hi = PRICE_RANGES[cat]
        price = round(rng.uniform(lo, hi), 2)
        # note corrélée au prix : les produits chers sont un peu mieux notés
        base = 2.6 + 1.6 * ((price - lo) / (hi - lo))
        rating = round(min(5.0, max(1.0, rng.gauss(base, 0.8))), 1)
        rows.append({
            "product_id": "P%05d" % i,
            "name": "%s %s %d" % (rng.choice(PRODUCT_TIERS), rng.choice(PRODUCT_ITEMS[cat]), i),
            "category": cat,
            "price": price,
            "merchant_id": rng.choice(by_cat[cat]),
            "rating": rating,
            "stock": rng.randrange(0, 1200),
        })

    for k in rng.sample(range(len(rows)), ANOMALIES["product_prix_invalide"]):
        rows[k]["price"] = rng.choice([0.0, -12.5, -1.0, -99.99])
    for k in rng.sample(range(len(rows)), ANOMALIES["product_rating_invalide"]):
        rows[k]["rating"] = rng.choice([0.0, 0.5, 5.8, 6.4, -1.0])
    for k in rng.sample(range(len(rows)), ANOMALIES["product_nom_nul"]):
        rows[k]["name"] = None
    return rows


# ---------------------------------------------------------------- utilisateurs

def build_users(rng):
    rows = []
    for i in range(1, N_USERS + 1):
        segment = rng.choices(SEGMENTS, weights=SEGMENT_WEIGHTS)[0]
        age = int(min(95, max(16, rng.gauss(41, 15))))
        income_base = {"Budget": 22000, "Standard": 38000, "Premium": 62000, "VIP": 105000}[segment]
        income = round(max(9000.0, rng.gauss(income_base, income_base * 0.28)), 2)
        n_pref = rng.choices([1, 2, 3, 4], weights=[0.30, 0.34, 0.24, 0.12])[0]
        reg = date(2023, 1, 1) + timedelta(days=rng.randrange(1000))
        rows.append({
            "user_id": "U%05d" % i,
            "age": age,
            "annual_income": income,
            "city": rng.choices(CITIES, weights=[22, 10, 10, 8, 6, 6, 5, 5, 5, 5, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2])[0],
            "customer_segment": segment,
            "preferred_categories": rng.sample(CATEGORIES, n_pref),
            "registration_date": reg.strftime("%Y%m%d"),
        })

    for k in rng.sample(range(len(rows)), ANOMALIES["user_age_invalide"]):
        rows[k]["age"] = rng.choice([0, 8, 12, 14, 103, 117, 140, -3])
    for k in rng.sample(range(len(rows)), ANOMALIES["user_revenu_invalide"]):
        rows[k]["annual_income"] = rng.choice([0.0, -1500.0, -230.75, 0.0])
    for k in rng.sample(range(len(rows)), ANOMALIES["user_ville_nulle"]):
        rows[k]["city"] = None
    return rows


# ---------------------------------------------------------------- transactions

def build_transactions(rng, users, products):
    """Génère les transactions avec des cohortes mensuelles et une rétention décroissante."""
    prod_by_id = {p["product_id"]: p for p in products}
    prod_ids = [p["product_id"] for p in products]
    prod_by_cat = {}
    for p in products:
        prod_by_cat.setdefault(p["category"], []).append(p["product_id"])

    n_months = month_key(TX_END) - month_key(TX_START) + 1
    first_month_weights = [max(1.0, 12.0 - 0.32 * i) for i in range(n_months)]

    intensity = {"Budget": 0.7, "Standard": 1.0, "Premium": 1.5, "VIP": 2.4}
    retention0 = {"Budget": 0.30, "Standard": 0.40, "Premium": 0.52, "VIP": 0.68}

    rows = []
    counter = 0

    for u in users:
        # 12 % des inscrits n'achètent jamais : utile pour l'analyse de conversion
        if rng.random() < 0.12:
            continue

        seg = u["customer_segment"]
        start_idx = rng.choices(range(n_months), weights=first_month_weights)[0]
        prefs = u["preferred_categories"]
        city = u["city"] or rng.choice(CITIES)
        fav_payment = rng.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]

        user_tx = []
        for k in range(n_months - start_idx):
            if k == 0:
                active = True
            else:
                p_active = retention0[seg] * (0.86 ** (k - 1))
                active = rng.random() < p_active
            if not active:
                continue

            m_first = add_months(date(TX_START.year, TX_START.month, 1), start_idx + k)
            n_tx = max(1, int(rng.gauss(3.4 * intensity[seg], 1.4)))
            for _ in range(min(n_tx, 8)):
                d = random_day_in_month(rng, m_first, TX_START, TX_END)
                # 62 % des achats dans une catégorie préférée
                cat = rng.choice(prefs) if (prefs and rng.random() < 0.62) else rng.choice(CATEGORIES)
                pid = rng.choice(prod_by_cat[cat])
                user_tx.append((d, pid))

        # semaine intensive pour 3 % des utilisateurs (règle « utilisateur actif »)
        if user_tx and rng.random() < 0.03:
            base_day = min(rng.choice(user_tx)[0], TX_END - timedelta(days=8))
            for off in range(rng.randrange(5, 8)):
                d = base_day + timedelta(days=off)
                cat = rng.choice(prefs) if prefs else rng.choice(CATEGORIES)
                user_tx.append((d, rng.choice(prod_by_cat[cat])))

        for d, pid in user_tx:
            prod = prod_by_id[pid]
            night = rng.random() < 0.04
            hour, minute, second = random_time(rng, night_bias=night)
            unit = prod["price"] if prod["price"] > 0 else round(rng.uniform(10, 100), 2)
            qty = rng.choices([1, 2, 3], weights=[0.82, 0.14, 0.04])[0]
            amount = round(unit * qty * rng.uniform(0.9, 1.12), 2)
            counter += 1
            rows.append({
                "transaction_id": "TX%07d" % counter,
                "user_id": u["user_id"],
                "product_id": pid,
                "merchant_id": prod["merchant_id"],
                "amount": amount,
                "timestamp": ts_str(d, hour, minute, second),
                "location": city if rng.random() < 0.88 else rng.choice(CITIES),
                "payment_method": fav_payment if rng.random() < 0.55
                else rng.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
                "category": prod["category"],
            })

    # --- transactions atypiques : montants aberrants, nuit, CRYPTO, rafales ---
    n_suspicious = int(len(rows) * 0.006)
    for _ in range(n_suspicious):
        src = rng.choice(rows)
        counter += 1
        d = datetime.strptime(src["timestamp"], "%Y%m%d%H%M%S").date()
        hour, minute, second = random_time(rng, night_bias=True)
        rows.append({
            "transaction_id": "TX%07d" % counter,
            "user_id": src["user_id"],
            "product_id": src["product_id"],
            "merchant_id": src["merchant_id"],
            "amount": round(src["amount"] * rng.uniform(4.5, 12.0), 2),
            "timestamp": ts_str(d, hour, minute, second),
            "location": rng.choice(CITIES),
            "payment_method": "CRYPTO",
            "category": src["category"],
        })

    # rafales : deux achats du même utilisateur à moins de 5 minutes d'intervalle
    n_bursts = int(len(rows) * 0.004)
    for _ in range(n_bursts):
        src = rng.choice(rows)
        t = datetime.strptime(src["timestamp"], "%Y%m%d%H%M%S") + timedelta(seconds=rng.randrange(45, 290))
        counter += 1
        rows.append({
            "transaction_id": "TX%07d" % counter,
            "user_id": src["user_id"],
            "product_id": rng.choice(prod_ids),
            "merchant_id": src["merchant_id"],
            "amount": round(src["amount"] * rng.uniform(0.8, 3.5), 2),
            "timestamp": t.strftime("%Y%m%d%H%M%S"),
            "location": src["location"],
            "payment_method": rng.choice(PAYMENT_METHODS),
            "category": src["category"],
        })

    rng.shuffle(rows)
    for i, r in enumerate(rows, start=1):
        r["transaction_id"] = "TX%07d" % i

    # ------------------------------- anomalies -------------------------------
    def pick(n):
        return rng.sample(range(len(rows)), n)

    for k in pick(ANOMALIES["tx_amount_invalide"]):
        rows[k]["amount"] = rng.choice([0.0, -15.9, -230.5, 0.0, -1.0])
    for k in pick(ANOMALIES["tx_timestamp_invalide"]):
        ts = rows[k]["timestamp"]
        rows[k]["timestamp"] = rng.choice([ts[:8], ts[:12], ts + "00", "", "NA"])
    for k in pick(ANOMALIES["tx_user_orphelin"]):
        rows[k]["user_id"] = "U%05d" % rng.randrange(90000, 99999)
    for k in pick(ANOMALIES["tx_produit_orphelin"]):
        rows[k]["product_id"] = "P%05d" % rng.randrange(90000, 99999)
    for k in pick(ANOMALIES["tx_marchand_orphelin"]):
        rows[k]["merchant_id"] = "M%05d" % rng.randrange(90000, 99999)
    for k in pick(ANOMALIES["tx_location_nulle"]):
        rows[k]["location"] = ""
    for k in pick(ANOMALIES["tx_payment_nul"]):
        rows[k]["payment_method"] = ""

    return rows


# ---------------------------------------------------------------- écriture

def write_csv(path, rows, fields):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fields})


def write_jsonl(path, rows):
    order = ["age", "annual_income", "city", "customer_segment",
             "preferred_categories", "registration_date", "user_id"]
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({k: r[k] for k in order}, ensure_ascii=False) + "\n")


def write_parquet_dir(path, rows):
    """Écrit un répertoire Parquet façon Spark : plusieurs part-*.snappy.parquet + _SUCCESS."""
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)

    df = pd.DataFrame(rows)
    schema = pa.schema([
        ("product_id", pa.string()),
        ("name", pa.string()),
        ("category", pa.string()),
        ("price", pa.float64()),
        ("merchant_id", pa.string()),
        ("rating", pa.float64()),
        ("stock", pa.int32()),
    ])

    run_id = str(uuid.uuid4())
    chunk = (len(df) + N_PARQUET_PARTS - 1) // N_PARQUET_PARTS
    for i in range(N_PARQUET_PARTS):
        part = df.iloc[i * chunk:(i + 1) * chunk]
        table = pa.Table.from_pandas(part, schema=schema, preserve_index=False)
        name = "part-%05d-%s-c000.snappy.parquet" % (i, run_id)
        pq.write_table(table, os.path.join(path, name), compression="snappy")
    open(os.path.join(path, "_SUCCESS"), "w").close()


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("outdir")
    ap.add_argument("--seed", type=int, default=2026)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    os.makedirs(args.outdir, exist_ok=True)

    merchants = build_merchants(rng)
    products = build_products(rng, merchants)
    users = build_users(rng)
    transactions = build_transactions(rng, users, products)

    write_csv(os.path.join(args.outdir, "merchants.csv"), merchants,
              ["merchant_id", "name", "category", "region", "commission_rate", "establishment_date"])
    write_parquet_dir(os.path.join(args.outdir, "products.parquet"), products)
    write_jsonl(os.path.join(args.outdir, "users.json"), users)
    write_csv(os.path.join(args.outdir, "transactions.csv"), transactions,
              ["transaction_id", "user_id", "product_id", "merchant_id", "amount",
               "timestamp", "location", "payment_method", "category"])

    print("merchants    : %6d lignes" % len(merchants))
    print("products     : %6d lignes" % len(products))
    print("users        : %6d lignes" % len(users))
    print("transactions : %6d lignes" % len(transactions))


if __name__ == "__main__":
    main()
