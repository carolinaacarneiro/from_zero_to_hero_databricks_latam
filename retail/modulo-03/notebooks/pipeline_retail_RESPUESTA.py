# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🔗 Módulo 3 · El pipeline de ventas — VERSIÓN RESPUESTA
# MAGIC
# MAGIC **Este archivo es el pipeline COMPLETO, con silver y gold ya resueltos.**
# MAGIC
# MAGIC Si no tienes experiencia o prefieres no escribir el código, adjunta **este** archivo al
# MAGIC pipeline en vez de `pipeline_retail`, y sigue el guía desde el paso 4. Verlo funcionar y
# MAGIC leerlo también enseña — no es trampa.
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
    schema = "retail_" + re.sub(r"[^a-z0-9_]", "_", usuario.split("@")[0].lower())
    ocultos = ("system", "samples", "__databricks_internal", "hive_metastore")
    for c in [r[0] for r in spark.sql("SHOW CATALOGS").collect() if r[0].lower() not in ocultos]:
        try:
            if schema in [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{c}`").collect()]:
                return f"/Volumes/{c}/{schema}/raw"
        except Exception:
            continue
    raise Exception("No pude detectar tu volumen raw. Corre los módulos 0-2 primero.")


RAW = _detectar_raw()


# ═══════════════════════════════ BRONZE ═══════════════════════════════════


@dlt.table(name="bronze_pl_ventas",
           comment="Ventas crudas ingeridas por el pipeline, tal como llegaron.")
def bronze_pl_ventas():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/ventas")
        .selectExpr("*", "current_timestamp() AS _ingerido_en",
                    "_metadata.file_path AS _archivo_origen")
    )


@dlt.table(name="bronze_pl_productos",
           comment="Catálogo de productos crudo (CSV).")
def bronze_pl_productos():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/productos")
    )


@dlt.table(name="bronze_pl_inventario",
           comment="Inventario crudo (stock por producto y país), CSV.")
def bronze_pl_inventario():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/inventario")
    )


# ═══════════════════════════════ SILVER ═══════════════════════════════════


@dlt.table(
    name="silver_ventas",
    comment="Ventas limpias, tipadas y enriquecidas con datos del producto (categoría, marca, costo).",
)
@dlt.expect_or_drop("id_valido", "venta_id IS NOT NULL")
@dlt.expect_or_drop("cantidad_positiva", "cantidad > 0")
@dlt.expect("monto_positivo", "monto > 0")
def silver_ventas():
    v = dlt.read_stream("bronze_pl_ventas")
    productos = dlt.read("bronze_pl_productos").drop("_datos_rescatados")
    return (
        v.dropDuplicates(["venta_id"])
        .join(productos, "producto_id", "left")
        .withColumn("fecha", F.to_timestamp("fecha"))
        .withColumn("utilidad",
                    F.round(F.col("monto") - F.col("cantidad") * F.col("costo_unitario"), 2))
        .select(
            "venta_id", "fecha", "producto_id", "nombre_producto", "categoria",
            "subcategoria", "marca", "pais", "canal", "cantidad", "precio_unitario",
            "descuento_pct", "monto", "costo_unitario", "utilidad", "_archivo_origen",
        )
    )


# ═══════════════════════════════ GOLD ═════════════════════════════════════


@dlt.table(
    name="gold_ventas_diarias",
    comment="Ventas agregadas por día, país y categoría. Serie de tiempo base del pronóstico.",
)
def gold_ventas_diarias():
    s = dlt.read("silver_ventas")
    return (
        s.withColumn("dia", F.to_date("fecha"))
        .groupBy("dia", "pais", "categoria")
        .agg(
            F.sum("cantidad").alias("unidades"),
            F.round(F.sum("monto"), 2).alias("monto"),
            F.round(F.sum("utilidad"), 2).alias("utilidad"),
            F.count("*").alias("lineas"),
        )
    )


@dlt.table(
    name="gold_ventas_producto",
    comment="Resumen por producto: totales de venta y unidades. Para el ranking del dashboard.",
)
def gold_ventas_producto():
    s = dlt.read("silver_ventas")
    return (
        s.groupBy("producto_id", "nombre_producto", "categoria", "marca")
        .agg(
            F.sum("cantidad").alias("unidades_vendidas"),
            F.round(F.sum("monto"), 2).alias("monto_total"),
            F.round(F.sum("utilidad"), 2).alias("utilidad_total"),
            F.countDistinct("pais").alias("paises_con_venta"),
        )
        .withColumn("margen_pct",
                    F.round(F.col("utilidad_total") / F.col("monto_total"), 4))
    )


@dlt.table(
    name="gold_inventario_estado",
    comment="Inventario cruzado con la demanda reciente: qué productos hay que reabastecer.",
)
def gold_inventario_estado():
    demanda = (
        dlt.read("silver_ventas")
        .filter(F.col("fecha") >= F.date_sub(F.current_date(), 90))
        .groupBy("producto_id", "pais")
        .agg(F.round(F.sum("cantidad") / F.lit(90.0), 2).alias("demanda_diaria"))
    )
    inv = dlt.read("bronze_pl_inventario").drop("_datos_rescatados")
    prod = dlt.read("bronze_pl_productos").select("producto_id", "nombre_producto", "categoria")
    return (
        inv.join(demanda, ["producto_id", "pais"], "left")
        .join(prod, "producto_id", "left")
        .withColumn("demanda_diaria", F.coalesce(F.col("demanda_diaria"), F.lit(0.0)))
        .withColumn(
            "dias_cobertura",
            F.when(F.col("demanda_diaria") > 0,
                   F.round(F.col("stock_actual") / F.col("demanda_diaria"), 1))
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "necesita_reabastecer",
            (F.col("stock_actual") < F.col("stock_minimo"))
            | (F.col("dias_cobertura") < F.col("lead_time_dias")),
        )
        .select("producto_id", "nombre_producto", "categoria", "pais", "stock_actual",
                "stock_minimo", "demanda_diaria", "dias_cobertura", "lead_time_dias",
                "necesita_reabastecer")
    )
