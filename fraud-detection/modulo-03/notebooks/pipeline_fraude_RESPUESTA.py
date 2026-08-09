# Databricks notebook source
# MAGIC %md
# MAGIC # 🔗 Módulo 3 · Pipeline de fraude — VERSIÓN RESPUESTA (completa)
# MAGIC
# MAGIC Este es el pipeline **con todo resuelto**: bronze, silver y gold, sin ningún `TODO`.
# MAGIC La medallion completa en un solo pipeline.
# MAGIC
# MAGIC ## ¿Cuándo usar este archivo?
# MAGIC
# MAGIC | Usa `pipeline_fraude` (el del ejercicio) si… | Usa **este** (`_RESPUESTA`) si… |
# MAGIC |---|---|
# MAGIC | Quieres escribir silver y gold tú mismo | No tienes experiencia y prefieres verlo funcionar |
# MAGIC | Vas siguiendo el guía paso a paso | Te trabaste y quieres avanzar |
# MAGIC | — | Eres TA y necesitas desbloquear a alguien |
# MAGIC
# MAGIC > La bronze se llama `bronze_pl_*` para no chocar con la del Módulo 2. Silver y gold
# MAGIC > usan el nombre canónico: son las que consumen los módulos siguientes.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos.

# COMMAND ----------

import dlt
import re
from pyspark.sql import functions as F


def _detectar_raw():
    try:
        v = spark.conf.get("z2h.raw_path")
        if v:
            return v
    except Exception:
        pass
    usuario = spark.sql("SELECT current_user()").collect()[0][0]
    schema = "fin_" + re.sub(r"[^a-z0-9_]", "_", usuario.split("@")[0].lower())
    ocultos = ("system", "samples", "__databricks_internal", "hive_metastore")
    for c in [r[0] for r in spark.sql("SHOW CATALOGS").collect() if r[0].lower() not in ocultos]:
        try:
            if schema in [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{c}`").collect()]:
                return f"/Volumes/{c}/{schema}/raw"
        except Exception:
            continue
    raise Exception("No pude detectar tu volumen raw. Corre los módulos 0-2 primero.")


RAW = _detectar_raw()


@dlt.table(
    name="bronze_pl_transacciones",
    comment="Transacciones crudas ingeridas por el pipeline, tal como llegaron.",
)
def bronze_pl_transacciones():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/transacciones")
        .selectExpr("*", "current_timestamp() AS _ingerido_en",
                    "_metadata.file_path AS _archivo_origen")
    )


@dlt.table(name="bronze_pl_clientes", comment="Clientes crudos, ingeridos por el pipeline (CSV).")
def bronze_pl_clientes():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/clientes")
    )


@dlt.table(
    name="silver_transacciones",
    comment="Transacciones limpias, tipadas y enriquecidas con datos del cliente.",
)
@dlt.expect_or_drop("id_valido", "transaccion_id IS NOT NULL")
@dlt.expect_or_drop("monto_positivo", "monto > 0")
@dlt.expect("moneda_conocida", "moneda IN ('COP','USD')")
def silver_transacciones():
    tx = dlt.read_stream("bronze_pl_transacciones")
    clientes = dlt.read("bronze_pl_clientes").drop("region", "_datos_rescatados")
    return (
        tx.dropDuplicates(["transaccion_id"])
        .join(clientes, "cliente_id", "left")
        .withColumn("fecha", F.to_timestamp("fecha"))
        .select(
            "transaccion_id", "cliente_id", "numero_tarjeta", "monto", "moneda",
            "comercio", "categoria_comercio", "canal", "pais", "region", "fecha",
            "es_fraude", "segmento", "antiguedad_meses", "score_crediticio",
            "_archivo_origen",
        )
    )


@dlt.table(
    name="gold_riesgo_diario",
    comment="Riesgo de fraude agregado por día, región y categoría de comercio.",
)
def gold_riesgo_diario():
    s = dlt.read("silver_transacciones")
    return (
        s.withColumn("dia", F.to_date("fecha"))
        .groupBy("dia", "region", "categoria_comercio")
        .agg(
            F.count("*").alias("total_transacciones"),
            F.round(F.sum("monto"), 0).alias("monto_total"),
            F.sum(F.when(F.col("es_fraude"), 1).otherwise(0)).alias("transacciones_fraude"),
            F.round(F.sum(F.when(F.col("es_fraude"), F.col("monto")).otherwise(0)), 0)
                .alias("monto_fraude"),
        )
        .withColumn("tasa_fraude",
                    F.round(F.col("transacciones_fraude") / F.col("total_transacciones"), 4))
        .withColumn("ticket_promedio",
                    F.round(F.col("monto_total") / F.col("total_transacciones"), 0))
    )
