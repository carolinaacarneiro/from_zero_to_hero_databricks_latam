# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🔗 Módulo 3 · El pipeline de fraude
# MAGIC
# MAGIC **Este archivo ES el pipeline.** No lo corras celda por celda: se pega dentro de un
# MAGIC **Lakeflow pipeline** (el notebook `03_guia_pipeline` te guía).
# MAGIC
# MAGIC ## Qué construye — la medallion completa, en un solo pipeline
# MAGIC
# MAGIC ```
# MAGIC   bronze_pl_transacciones ─┐
# MAGIC                            ├─→ silver_transacciones ─→ gold_riesgo_diario
# MAGIC   bronze_pl_clientes ──────┘
# MAGIC ```
# MAGIC
# MAGIC Aquí ves el flujo **de bronze a gold en un mismo lugar** — la buena práctica: un
# MAGIC pipeline declarativo de punta a punta.
# MAGIC
# MAGIC | Dataset | Estado |
# MAGIC |---|---|
# MAGIC | `bronze_pl_transacciones`, `bronze_pl_clientes` | ✅ ya declarados abajo |
# MAGIC | `silver_transacciones` | 📝 **TU TURNO** |
# MAGIC | `gold_riesgo_diario` | 📝 **TU TURNO** |
# MAGIC
# MAGIC > 💡 **¿Por qué la bronze se llama `bronze_pl_*` y no `bronze_*`?** Porque en el Módulo 2
# MAGIC > ya creaste `bronze_transacciones` a mano con Auto Loader. Este pipeline crea **su
# MAGIC > propia** bronze (con otro nombre) para no chocar con aquella. En un proyecto real
# MAGIC > tendrías **uno** de los dos caminos —ingesta manual **o** pipeline—, no ambos; acá los
# MAGIC > ves los dos para aprender. Silver y gold sí usan el nombre canónico: son las que
# MAGIC > consumen los módulos siguientes.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos.

# COMMAND ----------

import dlt
import re
from pyspark.sql import functions as F

# ═══════════════════════════════════════════════════════════════════════════
# ¿Dónde están los archivos crudos? Se detecta solo desde tu usuario.
# ═══════════════════════════════════════════════════════════════════════════


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


# ═══════════════════════════════════════════════════════════════════════════
# CAPA BRONZE — ya declarada. El pipeline la crea desde los archivos crudos.
# Nombre bronze_pl_* para no chocar con la bronze del Módulo 2.
# ═══════════════════════════════════════════════════════════════════════════


@dlt.table(
    name="bronze_pl_transacciones",
    comment="Transacciones crudas ingeridas por el pipeline, tal como llegaron. Sin transformar.",
)
def bronze_pl_transacciones():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/transacciones")
        .selectExpr(
            "*",
            "current_timestamp() AS _ingerido_en",
            "_metadata.file_path AS _archivo_origen",
        )
    )


@dlt.table(
    name="bronze_pl_clientes",
    comment="Dimensión de clientes cruda, ingerida por el pipeline (CSV).",
)
def bronze_pl_clientes():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/clientes")
    )


# ═══════════════════════════════════════════════════════════════════════════
# 📝 TU TURNO 1 · CAPA SILVER
#
# Declara silver_transacciones: la bronze limpia, tipada y enriquecida con los datos
# del cliente. Ya vienen las 3 expectativas — tú completas el cuerpo.
#
# Lee las bronze de ESTE pipeline con dlt.read_stream / dlt.read.
# Lo que tiene que hacer:
#   · deduplicar por transaccion_id
#   · unir con los clientes por cliente_id (left join)
#   · tipar 'fecha' de string a timestamp   ← el pendiente del M1 y M2
#   · quedarte con las columnas útiles
# ═══════════════════════════════════════════════════════════════════════════


@dlt.table(
    name="silver_transacciones",
    comment="Transacciones limpias, tipadas y enriquecidas con datos del cliente.",
)
@dlt.expect_or_drop("id_valido", "transaccion_id IS NOT NULL")
@dlt.expect_or_drop("monto_positivo", "monto > 0")           # descarta el ~2% inválido
@dlt.expect("moneda_conocida", "moneda IN ('COP','USD')")    # solo avisa
def silver_transacciones():
    tx = dlt.read_stream("bronze_pl_transacciones")
    # la transacción ya trae 'region'; se quita del lado de clientes para que el join no
    # genere una columna 'region' ambigua (existe en las dos tablas)
    clientes = dlt.read("bronze_pl_clientes").drop("region", "_datos_rescatados")

    # 📝 COMPLETA: deduplica, une con clientes, tipa la fecha y selecciona columnas.
    #
    # return (
    #     tx.dropDuplicates(["transaccion_id"])
    #       .join(clientes, "cliente_id", "left")
    #       .withColumn("fecha", F.to_timestamp("fecha"))
    #       .select(
    #           "transaccion_id", "cliente_id", "numero_tarjeta", "monto", "moneda",
    #           "comercio", "categoria_comercio", "canal", "pais", "region", "fecha",
    #           "es_fraude", "segmento", "antiguedad_meses", "score_crediticio",
    #           "_archivo_origen",
    #       )
    # )
    raise NotImplementedError("Completa silver_transacciones — ver la solución en el guía")


# ═══════════════════════════════════════════════════════════════════════════
# 📝 TU TURNO 2 · CAPA GOLD
#
# Declara gold_riesgo_diario: silver agregada por día × región × categoría.
# Es una vista materializada (recalcula el agregado), no un stream.
#
# Métricas por grupo:
#   total_transacciones · monto_total · transacciones_fraude · monto_fraude
#   tasa_fraude (fraudes/total) · ticket_promedio (monto_total/total)
# ═══════════════════════════════════════════════════════════════════════════


@dlt.table(
    name="gold_riesgo_diario",
    comment="Riesgo de fraude agregado por día, región y categoría de comercio.",
)
def gold_riesgo_diario():
    s = dlt.read("silver_transacciones")

    # 📝 COMPLETA: agrupa por fecha (solo el día), region y categoria_comercio.
    #
    # return (
    #     s.withColumn("dia", F.to_date("fecha"))
    #      .groupBy("dia", "region", "categoria_comercio")
    #      .agg(
    #          F.count("*").alias("total_transacciones"),
    #          F.round(F.sum("monto"), 0).alias("monto_total"),
    #          F.sum(F.when(F.col("es_fraude"), 1).otherwise(0)).alias("transacciones_fraude"),
    #          F.round(F.sum(F.when(F.col("es_fraude"), F.col("monto")).otherwise(0)), 0)
    #              .alias("monto_fraude"),
    #      )
    #      .withColumn("tasa_fraude",
    #                  F.round(F.col("transacciones_fraude") / F.col("total_transacciones"), 4))
    #      .withColumn("ticket_promedio",
    #                  F.round(F.col("monto_total") / F.col("total_transacciones"), 0))
    # )
    raise NotImplementedError("Completa gold_riesgo_diario — ver la solución en el guía")
