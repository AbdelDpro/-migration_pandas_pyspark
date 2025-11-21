# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyyaml",
#     "pandas",
#     "pyspark",
#     "pytest"
# ]
# ///

# test_equivalence.py
import pandas as pd
import pandas.testing as pdt
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, DoubleType, LongType

# ----------------------------------------------------------------------
# SCHÉMA D'ENTRÉE SIMPLE (Nécessaire pour créer le DF Spark à partir de la liste)
# ----------------------------------------------------------------------
# Pour des tests précis, il faut forcer le schéma d'entrée PySpark
SIMPLE_ORDERS_SCHEMA = StructType([
    StructField("order_id", LongType(), True),
    StructField("created_at", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("items", ArrayType(
        StructType([
            StructField("qty", LongType(), True),
            StructField("sku", StringType(), True),
            StructField("unit_price", DoubleType(), True)
        ])
    ), True)
])

# ----------------------------------------------------------------------
# TEST 1: EXPLOSION, FILTRAGE ET REVENU BRUT
# ----------------------------------------------------------------------

def test_explosion_filtering_and_gross_revenue(spark_session, simple_orders_data, settings):
    """
    Vérifie l'équivalence après les étapes critiques :
    1. Filtrage payment_status='paid'.
    2. Explosion des lignes d'articles.
    3. Rejet des prix négatifs.
    4. Calcul de la colonne line_gross.
    """
    
    # --- PANDAS REFERENCE --- ACT
     
    df_pd = pd.DataFrame(simple_orders_data)
    
    # 1. Filtrage paid
    df_pd = df_pd[df_pd["payment_status"] == settings['business_rules']['payment_status']].copy()
    
    # 2. Explosion et Normalisation
    df_pd = df_pd.explode("items", ignore_index=True)
    items = pd.json_normalize(df_pd["items"]).add_prefix("item_")
    df_pd = pd.concat([df_pd.drop(columns=["items"]), items], axis=1)

    # 3. Rejet des prix négatifs
    df_pd = df_pd[df_pd["item_unit_price"] >= 0].copy()

    # 4. Calcul du revenu brut
    df_pd["line_gross"] = df_pd["item_qty"] * df_pd["item_unit_price"]
    
    # Sélection et tri de référence
    df_pd = df_pd[["order_id", "item_qty", "item_unit_price", "line_gross"]]
    df_pd = df_pd.sort_values(by=["order_id", "item_qty"]).reset_index(drop=True)
    
    # --- PYSPARK IMPLEMENTATION ---
    
    df_spark = spark_session.createDataFrame(simple_orders_data, schema=SIMPLE_ORDERS_SCHEMA)

    # 1. Filtrage paid
    df_spark = df_spark.filter(col("payment_status") == settings['business_rules']['payment_status'])
    
    # 2. Explosion et Normalisation
    orders_spark_exploded = df_spark.withColumn("item", explode(col("items")))
    df_spark = orders_spark_exploded.select(
        col("order_id"),
        col("item.qty").alias("item_qty"),
        col("item.unit_price").alias("item_unit_price")
    ).drop("items")
    
    # 3. Rejet des prix négatifs
    df_spark = df_spark.filter(col("item_unit_price") >= 0)
    
    # 4. Calcul du revenu brut
    df_spark = df_spark.withColumn(
        "line_gross", 
        col("item_qty") * col("item_unit_price")
    )
    
    # Sélection et tri pour la comparaison
    df_spark = df_spark.select("order_id", "item_qty", "item_unit_price", "line_gross")
    df_spark = df_spark.orderBy("order_id", "item_qty")
    
    # --- ASSERTION ---
    
    # Conversion en Pandas pour la comparaison finale
    df_spark_converted = df_spark.toPandas()
    df_spark_converted = df_spark_converted.reset_index(drop=True)
    
    # La comparaison stricte
    pdt.assert_frame_equal(
        df_pd,
        df_spark_converted,
        check_dtype=True,
        check_exact=False, # Utiliser False pour les floats (recommandé pour PySpark/Pandas)
        atol=1e-2 # Tolérance absolue pour les petites erreurs de floating point
    )