import sys
sys.path.append('scripts')

from pyspark.sql.functions import col, trim, current_timestamp
from spark_session import get_spark

BRONZE_PATH = "data/bronze/tracks"
SILVER_PATH = "data/silver/tracks"

spark = get_spark("silver_tracks")

df = spark.read.format("delta").load(BRONZE_PATH)

# 1. Dropar colunas lixo (indices duplicados do pandas)
df = df.drop("_c0", "Unnamed:_0", "arquivo_origem")

# 2. Converter tipos (o coracao do Silver)
df = (
    df
    .withColumn("popularity", col("popularity").cast("int"))
    .withColumn("duration_ms", col("duration_ms").cast("int"))
    .withColumn("explicit", col("explicit").cast("boolean"))
    .withColumn("danceability", col("danceability").cast("double"))
    .withColumn("energy", col("energy").cast("double"))
    .withColumn("key", col("key").cast("int"))
    .withColumn("loudness", col("loudness").cast("double"))
    .withColumn("mode", col("mode").cast("int"))
    .withColumn("speechiness", col("speechiness").cast("double"))
    .withColumn("acousticness", col("acousticness").cast("double"))
    .withColumn("liveness", col("liveness").cast("double"))
    .withColumn("valence", col("valence").cast("double"))
)

# 3. Limpar espacos sobrando em textos
df = (
    df
    .withColumn("artists", trim(col("artists")))
    .withColumn("track_name", trim(col("track_name")))
    .withColumn("track_genre", trim(col("track_genre")))
)

# 4. Remover linhas 100% identicas (mantem multi-genero de proposito)
df = df.dropDuplicates()

# 5. Remover linhas sem chaves criticas E generos invalidos (numericos = CSV deslocado)
df = df.filter(
    col("track_id").isNotNull()
    & col("track_genre").isNotNull()
    & ~col("track_genre").rlike("^[0-9.-]+$")
)

# 6. Carimbo de processamento Silver
df = df.withColumn("data_processamento_silver", current_timestamp())

# 7. Escrever em Delta, PARTICIONADO por genero
(
    df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("track_genre")
    .save(SILVER_PATH)
)

print(f"Silver criado. Linhas: {df.count()}")
print(f"Generos (particoes): {df.select('track_genre').distinct().count()}")
spark.stop()