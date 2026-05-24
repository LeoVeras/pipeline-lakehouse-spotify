from pyspark.sql.functions import current_timestamp, input_file_name
from spark_session import get_spark
from utils import limpar_nome_coluna

RAW_PATH = "data/raw/spotify-tracks-dataset.csv"
BRONZE_PATH = "data/bronze/tracks"

spark = get_spark("bronze_tracks")

df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)

# Delta nao aceita caracteres especiais em nomes de coluna.
# Sanitiza apenas os NOMES (os dados continuam crus).
for col_antiga in df.columns:
    df = df.withColumnRenamed(col_antiga, limpar_nome_coluna(col_antiga))

df_bronze = (
    df.withColumn("data_ingestao", current_timestamp())
      .withColumn("arquivo_origem", input_file_name())
)

df_bronze.write.format("delta").mode("overwrite").save(BRONZE_PATH)

print(f"Bronze criado. Linhas: {df_bronze.count()}")
spark.stop()