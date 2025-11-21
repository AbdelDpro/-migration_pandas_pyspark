# Étape 1 : Utiliser Python 3.12
FROM python:3.12-slim

# Étape 2 : Installer Java 21
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Étape 3 : Configurer JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Étape 4 : Créer le dossier de travail
WORKDIR /app

# Étape 5 : Copier tout le code
COPY . .

# Étape 6 : Installer les dépendances
RUN pip install --no-cache-dir \
    pandas>=2.0.0 \
    pyspark>=3.5.0 \
    pytest>=7.4.0 \
    pytest-cov>=4.1.0

# Étape 7 : Configurer PYTHONPATH pour trouver le module src
# CORRECTION : On pointe vers /app (pas /app/src)
ENV PYTHONPATH=/app

# Étape 8 : Lancer les tests
CMD ["pytest", "tests/", "-v", "--color=yes"]