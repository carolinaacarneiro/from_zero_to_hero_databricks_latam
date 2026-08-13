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
# MAGIC | `bronze_pl_transacciones`, `bronze_pl_clientes` | ✅ ya declaradas |
# MAGIC | `silver_transacciones` | ✅ ya declarada |
# MAGIC | `gold_riesgo_diario` | ✅ ya declarada |
# MAGIC
# MAGIC > 📖 **El código ya está completo.** No hay nada que editar: el foco del módulo es
# MAGIC > **entender** cómo se declara un pipeline, leer cada capa y, sobre todo, entender las
# MAGIC > **expectativas de calidad**. El guía `03_guia_pipeline` te lleva paso a paso.
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
from pyspark.sql import functions as F

# ¿Dónde están los archivos crudos? El pipeline necesita tu catálogo y tu schema.
# El CATÁLOGO se lee solo del Default catalog del pipeline. El SCHEMA hay que declararlo en
# Settings -> Configuration como  z2h.schema = <tu_schema>  (el Default schema del pipeline
# NO es visible desde el código). Ver el guía 03_guia_pipeline, Paso 4.
def _conf(clave):
    try:
        v = spark.conf.get(clave)
        return v.strip() if v else ""
    except Exception:
        return ""


# Lo más confiable en un pipeline: declararlo en Settings -> Configuration
#   z2h.catalogo = <tu_catálogo>   ·   z2h.schema = <tu_schema>
# Si no está, intenta leer el Default catalog/schema del pipeline (no siempre disponible).
_CATALOGO = _conf("z2h.catalogo") or spark.sql("SELECT current_catalog()").collect()[0][0]
_SCHEMA = _conf("z2h.schema")
if not _SCHEMA:
    _s = spark.sql("SELECT current_schema()").collect()[0][0]
    _SCHEMA = "" if (not _s or _s.lower() == "default") else _s

if (not _CATALOGO or not _SCHEMA
        or _CATALOGO.lower() in ("hive_metastore", "spark_catalog")):
    raise Exception(
        f"Falta el destino del pipeline (catálogo='{_CATALOGO}', schema='{_SCHEMA}'). "
        f"En Settings -> Configuration del pipeline agrega 'z2h.catalogo' y 'z2h.schema' "
        f"con tus valores (los mismos del M1/M2)."
    )

RAW = f"/Volumes/{_CATALOGO}/{_SCHEMA}/raw"


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
# CAPA SILVER — el dato se vuelve CONFIABLE
#
# silver_transacciones = bronze limpia, tipada y enriquecida con datos del cliente.
# Lee de las bronze de ESTE pipeline: dlt.read_stream() para lo que crece (streaming),
# dlt.read() para la dimensión estática de clientes. Esas llamadas son las que crean las
# DEPENDENCIAS del DAG — nadie escribe el orden.
#
# ── Las 3 EXPECTATIVAS de calidad (lo más importante de leer) ──────────────
#   @dlt.expect_or_drop("id_valido", ...)       → DESCARTA la fila si no cumple
#   @dlt.expect_or_drop("monto_positivo", ...)  → DESCARTA (limpia el ~2% inválido plantado)
#   @dlt.expect("moneda_conocida", ...)         → solo AVISA (la fila pasa igual)
# La decisión de diseño: monto ≤ 0 es basura que ensucia los agregados → se descarta;
# una moneda rara es algo que quieres SABER sin perder la fila → solo se avisa.
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

    return (
        tx.dropDuplicates(["transaccion_id"])               # una transacción, una fila
        .join(clientes, "cliente_id", "left")               # trae segmento, score, etc.
        .withColumn("fecha", F.to_timestamp("fecha"))       # ← el pendiente del M1/M2: texto → timestamp
        .select(
            "transaccion_id", "cliente_id", "numero_tarjeta", "monto", "moneda",
            "comercio", "categoria_comercio", "canal", "pais", "region", "fecha",
            "es_fraude", "segmento", "antiguedad_meses", "score_crediticio",
            "_archivo_origen",
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# CAPA GOLD — el dato se vuelve ÚTIL para el negocio
#
# gold_riesgo_diario = silver agregada por día × región × categoría de comercio.
# Es una MATERIALIZED VIEW: recalcula el agregado para reflejar el estado actual (no es un
# stream). Lee de silver con dlt.read() → gold depende de silver en el DAG.
# Responde la pregunta del área de riesgo: «¿cuánto fraude hubo, por región y categoría,
# cada día?».
# ═══════════════════════════════════════════════════════════════════════════


@dlt.table(
    name="gold_riesgo_diario",
    comment="Riesgo de fraude agregado por día, región y categoría de comercio.",
)
def gold_riesgo_diario():
    s = dlt.read("silver_transacciones")

    return (
        s.withColumn("dia", F.to_date("fecha"))             # agrupa por día (sin la hora)
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
