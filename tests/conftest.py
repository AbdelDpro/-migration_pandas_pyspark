# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyyaml",
#     "pandas",
#     "pyspark",
#     "pytest"
# ]
# ///

# tests/conftest.py
import pytest
import yaml
from pathlib import Path
from pyspark.sql import SparkSession
import os
import pandas as pd

# Définition du chemin racine du projet (ajustez si nécessaire)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# --- FIXTURE CONFIGURATION (settings) --- ARRANGE 

@pytest.fixture(scope="session")
def settings(request):
    """
    Charge settings.yaml de manière robuste,
    même si pytest est lancé depuis un sous-dossier.
    """
    project_root = request.config.rootpath              # ← racine du projet
    config_file = project_root / "settings.yaml"

    if not config_file.exists():
        raise FileNotFoundError(f"settings.yaml non trouvé à : {config_file}")

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    return config

# --- FIXTURE SPARK ET CONTEXTE ---

@pytest.fixture(scope="session")
def spark_session():
    """Crée une session Spark locale pour l'ensemble des tests."""
    spark = SparkSession.builder \
        .appName("PySparkPandasEquivalenceTest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

# --- FIXTURE DE DONNÉES SIMPLES ---

@pytest.fixture(scope="session")
def simple_orders_data():
    """Jeu de données simple pour tester l'explosion et le filtrage."""
    # Simule la structure JSON avec une liste d'items (pour tester l'explosion)
    return [
        {"order_id": 1, "created_at": "2025-03-01 10:00:00", "payment_status": "paid", "customer_id": "C1",
         "items": [{"qty": 1, "sku": "A", "unit_price": 10.0}]},
        {"order_id": 2, "created_at": "2025-03-01 11:00:00", "payment_status": "paid", "customer_id": "C1",
         "items": [{"qty": 2, "sku": "B", "unit_price": 5.0}]},
        # Commande à filtrer (non paid)
        {"order_id": 3, "created_at": "2025-03-02 12:00:00", "payment_status": "pending", "customer_id": "C2",
         "items": [{"qty": 1, "sku": "X", "unit_price": 50.0}]},
        # Ligne à rejeter (prix négatif)
        {"order_id": 4, "created_at": "2025-03-02 13:00:00", "payment_status": "paid", "customer_id": "C3",
         "items": [{"qty": 1, "sku": "Z", "unit_price": -1.0}]}, 
    ]



# import pytest
# from pyspark.sql import SparkSession
# import pandas as pd

# @pytest.fixture(scope="session")
# def spark_session():
#     spark = (
#         SparkSession.builder
#         .master("local[*]")
#         .appName("unit_tests")
#         .getOrCreate()
#     )
#     return spark
