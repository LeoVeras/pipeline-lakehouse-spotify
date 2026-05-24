import sys
sys.path.append('scripts')

from pyspark.sql.functions import col, avg, count, round as sround, row_number, current_timestamp
from pyspark.sql.window import Window
from spark_session import get_spark

SILVER_PATH = "data/silver/tracks"
GOLD_METRICAS = "data/gold/metricas_por_genero"
GOLD_TOP = "data/gold/top5_por_genero"

spark = get_spark("gold_tracks")

df = spark.read.format("delta").load(SILVER_PATH)

# ============================================
# TABELA 1: Metricas por genero (GROUP BY)
# ============================================
metricas = (
    df.groupBy("track_genre")
      .agg(
          count("*").alias("qtd_musicas"),
          sround(avg("popularity"), 2).alias("popularidade_media"),
          sround(avg("danceability"), 3).alias("dancabilidade_media"),
          sround(avg("energy"), 3).alias("energia_media")
      )
      .orderBy(col("popularidade_media").desc())
      .withColumn("data_processamento_gold", current_timestamp())
)

metricas.write.format("delta").mode("overwrite").save(GOLD_METRICAS)
print("Tabela 1 (metricas por genero) criada.")

# ============================================
# TABELA 2: Top 5 musicas por genero (WINDOW FUNCTION)
# ============================================
janela = Window.partitionBy("track_genre").orderBy(col("popularity").desc())

top5 = (
    df.withColumn("ranking", row_number().over(janela))
      .filter(col("ranking") <= 5)
      .select("track_genre", "ranking", "track_name", "artists", "popularity")
      .withColumn("data_processamento_gold", current_timestamp())
)

top5.write.format("delta").mode("overwrite").save(GOLD_TOP)
print("Tabela 2 (top 5 por genero) criada.")

# Preview
print("\n=== TOP 5 do genero POP ===")
top5.filter(col("track_genre") == "pop").orderBy("ranking").show(truncate=False)

spark.stop()