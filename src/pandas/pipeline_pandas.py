# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyyaml",
#     "pandas",
#     "pyspark",
#     "pytest"
# ]
# ///

import yaml
import os
import pandas as pd
from pathlib import Path
import sqlite3
from datetime import datetime

# ========================================
# CHARGEMENT DE LA CONFIGURATION
# ========================================
# Permet de centraliser les paramètres (chemins, encodage, séparateur)
# dans un fichier settings.yaml plutôt que de les coder "en dur"

def load_settings(path="settings.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

cfg = load_settings()
in_dir = cfg.get("input_dir", "./data/march-input")
out_dir = cfg.get("output_dir", "./data/out")
db_path = cfg.get("db_path", "./data/sales_db.db")
sep = cfg.get("csv_sep", ";")
enc = cfg.get("csv_encoding", "utf-8")
ffmt = cfg.get("csv_float_format", "%.2f")
Path(out_dir).mkdir(parents=True, exist_ok=True)

# ========================================
# CHARGEMENT DES FICHIERS SOURCE
# Énoncé : "customers.csv (clients actifs/inactifs, ville)"
# ========================================

customers_path = os.path.join(in_dir, "customers.csv")
if not os.path.exists(customers_path):
    pass
else:
    customers = pd.read_csv(customers_path)

# ========================================
# CHARGEMENT DES REMBOURSEMENTS
# Énoncé : "refunds.csv (historique des remboursements)"
# ========================================

refunds_path = os.path.join(in_dir, "refunds.csv")
if not os.path.exists(refunds_path):
    pass
else:
    refunds = pd.read_csv(refunds_path)

# ========================================
# EXEMPLE DE CHARGEMENT D'UN FICHIER ORDERS (test)
# Énoncé : "orders_YYYY-MM-DD.json (commandes et lignes d'articles, un fichier par jour)"
# ========================================

order_path = os.path.join(in_dir, "orders_2025-03-01.json")
if not os.path.exists(order_path):
    pass
else:
    order = pd.read_json(order_path)

# ========================================
# CONSOLIDATION DE TOUS LES FICHIERS ORDERS DU MOIS
# Énoncé : "orders_YYYY-MM-DD.json (commandes et lignes d'articles, un fichier par jour)"
# Boucle sur tous les jours du mois de mars (1 à 31)
# ========================================

liste = []
for day in range(1, 32): 
    order_path = os.path.join(in_dir, f"orders_2025-03-{day:02d}.json")
    if not os.path.exists(order_path):
        continue
    else:
        order = pd.read_json(order_path)
    liste.append(order)
orders = pd.concat(liste) 

# ========================================
# FONCTION POUR STANDARDISER LES BOOLÉENS
# Les données peuvent arriver dans différents formats : "true", 1, "yes", etc.
# Cette fonction les transforme tous en booléen Python (True/False)
# ========================================

def controle_bool(v):
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return bool(v)
    if v is None: return False
    s = str(v).strip().lower()
    return s in ("1","true","yes","y","t")

# ========================================
# NETTOYAGE DES DONNÉES CLIENTS
# Standardisation du champ is_active et des types de colonnes
# ========================================

customers["is_active"] = customers["is_active"].apply(controle_bool)
customers = customers.astype({"customer_id":"string","city":"string"})

# ========================================
# NETTOYAGE DES REMBOURSEMENTS
# Énoncé : "Agréger les remboursements par commande, avec des montants négatifs"
# Conversion des montants en numérique et gestion des erreurs
# ========================================

refunds["amount"] = pd.to_numeric(refunds["amount"], errors="coerce").fillna(0.0)
refunds["created_at"] = refunds["created_at"].astype("string")

# ========================================
# FILTRAGE DES COMMANDES PAYÉES
# Énoncé : "Conserver uniquement les commandes payées (payment_status = 'paid')"
# ========================================

ln_initial = len(orders)
orders = orders[orders["payment_status"]=="paid"].copy()
ln_final = len(orders)


# ========================================
# EXPLOSION DES LIGNES D'ARTICLES
# Chaque commande contient plusieurs articles (dans une liste "items")
# On "éclate" cette liste pour avoir une ligne par article
# ========================================

orders2 = orders
orders2 = orders2.explode("items", ignore_index=True)
items = pd.json_normalize(orders2["items"]).add_prefix("item_")
orders2 = pd.concat([orders2.drop(columns=["items"]), items], axis=1)

# ========================================
# REJET DES ARTICLES À PRIX NÉGATIF
# Énoncé : "Écarter toute ligne d'article avec prix unitaire négatif (et consigner ces rejets)"
# ========================================

neg_mask = orders2["item_unit_price"] < 0
n_neg = int(neg_mask.sum())
if n_neg > 0:
    rejects_items = orders2.loc[neg_mask].copy()
    rejects_path = os.path.join(out_dir, "rejects_items.csv")
    rejects_items.to_csv(rejects_path, index=False, encoding=enc)
    orders2 = orders2.loc[~neg_mask].copy()

# ========================================
# DÉDUPLICATION DES COMMANDES
# Énoncé : "Dédupliquer sur order_id (garder la première occurrence)"
# On trie par date de création et on garde la première occurrence par order_id
# ========================================

before = len(orders2)
orders3 = orders2.sort_values(["order_id","created_at"]).drop_duplicates(subset=["order_id"], keep="first")
after = len(orders3)

# ========================================
# CALCUL DU REVENU BRUT PAR COMMANDE
# Calcul : quantité × prix unitaire, puis agrégation par commande
# ========================================

orders3["line_gross"] = orders3["item_qty"] * orders3["item_unit_price"]
per_order = orders3.groupby(["order_id","customer_id","channel","created_at"], as_index=False).agg(
    items_sold=("item_qty","sum"),
    gross_revenue_eur=("line_gross","sum")
)

# ========================================
# EXCLUSION DES CLIENTS INACTIFS
# Énoncé : "Exclure les clients inactifs (is_active = false)"
# On fait une jointure avec la table customers et on filtre sur is_active = True
# ========================================

len_init = len(per_order)
per_order = per_order.merge(customers[["customer_id","city","is_active"]], on="customer_id", how="left")
per_order = per_order[per_order["is_active"]==True].copy() # Important pour respecter le cahier de charge
ln_aft = len(per_order)

# ========================================
# STANDARDISATION DES DATES
# Conversion de différents formats de date en format ISO (YYYY-MM-DD)
# ========================================

def to_date(s):
    s = str(s)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Format de date non reconnu: {s}")

per_order["order_date"] = per_order["created_at"].apply(to_date)

# ========================================
# AGRÉGATION DES REMBOURSEMENTS PAR COMMANDE
# Énoncé : "Agréger les remboursements par commande, avec des montants négatifs"
# ========================================

refunds_sum = refunds.groupby("order_id", as_index=False)["amount"].sum().rename(columns={"amount":"refunds_eur"}) # Somme des remboursements par order_id (commande)
per_order = per_order.merge(refunds_sum, on="order_id", how="left").fillna({"refunds_eur":0.0})

# ========================================
# SAUVEGARDE DANS SQLITE : TABLE orders_clean
# Énoncé : "Une base SQLite sales.db comprenant : orders_clean (détails nettoyés par commande)"
# ========================================

conn = sqlite3.connect(db_path)
per_order_save = per_order[["order_id","customer_id","city","channel","order_date","items_sold","gross_revenue_eur"]].copy()
per_order_save.to_sql("orders_clean", conn, if_exists="replace", index=False)
conn.close()

# ========================================
# AGRÉGATION QUOTIDIENNE PAR VILLE ET CANAL
# Énoncé : "daily_city_sales (agrégats par ville × canal × date)"
# Colonnes : date;city;channel;orders_count;unique_customers;items_sold;gross_revenue_eur;refunds_eur;net_revenue_eur
# ========================================

agg = per_order.groupby(["order_date","city","channel"], as_index=False).agg(
    orders_count=("order_id","nunique"),
    unique_customers=("customer_id","nunique"),
    items_sold=("items_sold","sum"),
    gross_revenue_eur=("gross_revenue_eur","sum"),
    refunds_eur=("refunds_eur","sum")
)
agg["net_revenue_eur"] = agg["gross_revenue_eur"] + agg["refunds_eur"]
agg = agg.rename(columns={"order_date":"date"}).sort_values(["date","city","channel"]).reset_index(drop=True)

# ========================================
# SAUVEGARDE DANS SQLITE : TABLE daily_city_sales
# Énoncé : "Une base SQLite sales.db comprenant : daily_city_sales (agrégats par ville × canal × date)"
# ========================================

conn = sqlite3.connect(db_path)
agg.to_sql("daily_city_sales", conn, if_exists="replace", index=False)
conn.close()

# ========================================
# EXPORTS CSV QUOTIDIENS
# Énoncé : "Un CSV quotidien : daily_summary_YYYYMMDD.csv (séparateur ;, UTF-8, en-tête stable)"
# Énoncé : "Colonnes: date;city;channel;orders_count;unique_customers;items_sold;gross_revenue_eur;refunds_eur;net_revenue_eur"
# ========================================

for d, sub in agg.groupby("date"):
    out_path = os.path.join(out_dir, f"daily_summary_{d.replace('-','')}.csv")
    sub[[
        "date","city","channel","orders_count","unique_customers","items_sold",
        "gross_revenue_eur","refunds_eur","net_revenue_eur"
    ]].to_csv(out_path, index=False, sep=sep, encoding=enc, float_format=ffmt)

# Export d'un fichier consolidé avec toutes les dates
all_path = os.path.join(out_dir, "daily_summary_all.csv")
agg.to_csv(all_path, index=False, sep=sep, encoding=enc, float_format=ffmt)