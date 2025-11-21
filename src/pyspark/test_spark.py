# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyyaml",
#     "pandas",
#     "pyspark",
#     "pytest"
# ]
# ///



from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import yaml
import os
import sqlite3
from pathlib import Path
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import BooleanType, DoubleType, StringType
from pyspark.sql.functions import udf, col, when, explode, row_number, sum, to_date
from pyspark.sql.window import Window
from pathlib import Path
import pprint as pp



spark = SparkSession.builder.appName("Test").getOrCreate()
print("✓ Spark fonctionne")

PROJECT_ROOT = Path("/home/abdeldpro/cours/Esther_brief/migration_pandas_pyspark")

config_file = PROJECT_ROOT / "settings.yaml"

with config_file.open('r') as file:
    config = yaml.safe_load(file)



input_dir = PROJECT_ROOT / config['input_dir'].lstrip('./')
output_dir = PROJECT_ROOT / config['output_dir'].lstrip('./')
output_dir.mkdir(parents=True, exist_ok=True)



customers_path = input_dir / config['input_files']['customers']
df_customers = spark.read.csv(
    str(customers_path),
    header=True,
    sep=config['csv_sep'],
    encoding=config['csv_encoding'],
    inferSchema=True
)



# ========================================
# CONSOLIDATION DE TOUS LES FICHIERS ORDERS DU MOIS
# Énoncé : "orders_YYYY-MM-DD.json (commandes et lignes d'articles, un fichier par jour)"
# Boucle sur tous les jours du mois de mars (1 à 31)
# ========================================

liste_orders = []

for day in range(1, 32): 
    order_path = os.path.join(input_dir, f"orders_2025-03-{day:02d}.json")
    if not os.path.exists(order_path):
        continue
    else:
        order = spark.read.json(order_path, multiLine=True)
        liste_orders.append(order)

if liste_orders:
    from functools import reduce
    orders = reduce(lambda df1, df2: df1.union(df2), liste_orders)
else:
    orders = None



# ========================================
# CHARGEMENT DES REMBOURSEMENTS
# Énoncé : "refunds.csv (historique des remboursements)"
# ========================================

refunds_path = os.path.join(input_dir, "refunds.csv")
try:
    refunds = spark.read.csv(refunds_path, header=True, inferSchema=True)
except AnalysisException:
    refunds = None



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

controle_bool_udf = F.udf(controle_bool, BooleanType())

customers = (
    df_customers
        .withColumn("is_active", controle_bool_udf(df_customers["is_active"]))
        .withColumn("customer_id", df_customers["customer_id"].cast(StringType()))
        .withColumn("city", df_customers["city"].cast(StringType()))
)



# ========================================
# NETTOYAGE DES REMBOURSEMENTS
# Énoncé : "Agréger les remboursements par commande, avec des montants négatifs"
# Conversion des montants en numérique et gestion des erreurs
# ========================================

refunds_clean = (
        refunds.withColumn("amount", F.expr("try_cast(amount AS double)"))
        .na.fill({"amount": 0.0})
        .select("order_id", "amount")
    )



# ========================================
# FILTRAGE DES COMMANDES PAYÉES
# Énoncé : "Conserver uniquement les commandes payées (payment_status = 'paid')"
# ========================================

ln_initial = orders.count()
orders = orders.filter(col("payment_status") == "paid")
ln_final = orders.count()



# ========================================
# EXPLOSION DES LIGNES D'ARTICLES
# Chaque commande contient plusieurs articles (dans une liste "items")
# On "éclate" cette liste pour avoir une ligne par article
# ========================================

orders2 = orders.withColumn("item", explode(col("items")))
orders2 = orders2.select(
    *[col(c) for c in orders2.columns if c != "items"],
    col("item.qty").alias("item_qty"),
    col("item.sku").alias("item_sku"),
    col("item.unit_price").alias("item_unit_price")
).drop("items")



# ========================================
# REJET DES ARTICLES À PRIX NÉGATIF
# Énoncé : "Écarter toute ligne d'article avec prix unitaire négatif (et consigner ces rejets)"
# ========================================

orders2 = orders2.drop("item")

neg_items = orders2.filter(col("item_unit_price") < 0)
n_neg = neg_items.count()
if n_neg > 0:
    rejects_path = os.path.join(output_dir, "rejects_items.csv")

    neg_items.coalesce(1).write.mode("overwrite").csv(
        rejects_path, 
        header=True,
        encoding=config['csv_encoding']
    )
orders2 = orders2.filter(col("item_unit_price") >= 0)



# ========================================
# DÉDUPLICATION DES COMMANDES
# Énoncé : "Dédupliquer sur order_id (garder la première occurrence)"
# On trie par date de création et on garde la première occurrence par order_id
# ========================================

before = orders2.count()
window = Window.partitionBy("order_id").orderBy("created_at")
orders3 = orders2.withColumn("row_num", row_number().over(window))
orders3 = orders3.filter(col("row_num") == 1)
orders3 = orders3.drop("row_num")
after = orders3.count()



# ========================================
# CALCUL DU REVENU BRUT PAR COMMANDE
# Calcul : quantité × prix unitaire, puis agrégation par commande
# ========================================

orders3 = orders3.withColumn(
    "line_gross", 
    col("item_qty") * col("item_unit_price")
)

per_order = orders3.groupBy(
    "order_id", 
    "customer_id", 
    "channel", 
    "created_at"
).agg(
    sum("item_qty").alias("items_sold"),
    sum("line_gross").alias("gross_revenue_eur")
)



# ========================================
# EXCLUSION DES CLIENTS INACTIFS
# Énoncé : "Exclure les clients inactifs (is_active = false)"
# On fait une jointure avec la table customers et on filtre sur is_active = True
# ========================================

len_init = per_order.count()
per_order = per_order.join(
    customers.select("customer_id", "city", "is_active"),
    on="customer_id",
    how="left"
)
per_order = per_order.filter(col("is_active") == True)
len_after = per_order.count()



# ========================================
# STANDARDISATION DES DATES
# Conversion de différents formats de date en format ISO (YYYY-MM-DD)
# ========================================

per_order = per_order.withColumn(
    "order_date",
    to_date(col("created_at"))
)



# ========================================
# AGRÉGATION DES REMBOURSEMENTS PAR COMMANDE
# Énoncé : "Agréger les remboursements par commande, avec des montants négatifs"
# ========================================

# Sécurisation des montants de remboursement + agrégation
refunds_sum = (
    refunds
        .withColumn(
            "amount",
            F.expr("try_cast(amount AS double)")   # -> renvoie null si 'error'
        )
        .fillna({"amount": 0.0})                  # invariant : amount doit être numérique
        .groupBy("order_id")
        .agg(F.sum("amount").alias("refunds_eur"))
)

per_order = (
    per_order
        .join(refunds_sum, on="order_id", how="left")
        .fillna({"refunds_eur": 0.0})
)



# ========================================
# SAUVEGARDE DANS SQLITE : TABLE orders_clean
# Énoncé : "Une base SQLite sales.db comprenant : orders_clean (détails nettoyés par commande)"
# ========================================

db_path = "sales.db"  # ou "data/sales.db" si tu veux un sous-dossier

# Sélection des colonnes PySpark
per_order_save = per_order.select(
    "order_id", "customer_id", "city", "channel",
    "order_date", "items_sold", "gross_revenue_eur"
)

# Conversion en Pandas
per_order_pd = per_order_save.toPandas()

# Sauvegarde dans SQLite
conn = sqlite3.connect(db_path)
per_order_pd.to_sql("orders_clean", conn, if_exists="replace", index=False)
conn.close()