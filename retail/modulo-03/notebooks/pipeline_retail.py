# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🔗 Módulo 3 · El pipeline de ventas
# MAGIC
# MAGIC **Este archivo ES el pipeline.** No lo corras celda por celda: se pega dentro de un
# MAGIC **Lakeflow pipeline** (el notebook `03_guia_pipeline` te guía).
# MAGIC
# MAGIC ## Qué construye — la medallion completa, en un solo pipeline
# MAGIC
# MAGIC ```
# MAGIC   bronze_pl_ventas ──────┐
# MAGIC   bronze_pl_productos ───┼─→ silver_ventas ─┬─→ gold_ventas_diarias   (serie del pronóstico)
# MAGIC   bronze_pl_inventario ──┘                  ├─→ gold_ventas_producto  (top productos)
# MAGIC                                             └─→ gold_inventario_estado (qué reabastecer)
# MAGIC ```
# MAGIC
# MAGIC | Dataset | Estado |
# MAGIC |---|---|
# MAGIC | `bronze_pl_*` | ✅ ya declarados abajo |
# MAGIC | `silver_ventas` | 📝 **TU TURNO** |
# MAGIC | `gold_ventas_diarias` | 📝 **TU TURNO** (la serie que pronosticará el Módulo 6) |
# MAGIC | `gold_ventas_producto`, `gold_inventario_estado` | ✅ ya resueltos (alimentan el dashboard) |
# MAGIC
# MAGIC > 💡 **¿Por qué `bronze_pl_*`?** En el Módulo 2 ya creaste `bronze_ventas` a mano con Auto
# MAGIC > Loader. Este pipeline crea **su propia** bronze (con otro nombre) para no chocar. En un
# MAGIC > proyecto real tendrías **uno** de los dos caminos, no ambos; acá los ves los dos para
# MAGIC > aprender.
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


# ═══════════════════════════════════════════════════════════════════════════
# CAPA BRONZE — ya declarada. El pipeline la crea desde los archivos crudos.
# ═══════════════════════════════════════════════════════════════════════════


@dlt.table(
    name="bronze_pl_ventas",
    comment="Ventas crudas ingeridas por el pipeline, tal como llegaron. Sin transformar.",
)
def bronze_pl_ventas():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/ventas")
        .selectExpr(
            "*",
            "current_timestamp() AS _ingerido_en",
            "_metadata.file_path AS _archivo_origen",
        )
    )


@dlt.table(
    name="bronze_pl_productos",
    comment="Catálogo de productos crudo, ingerido por el pipeline (CSV).",
)
def bronze_pl_productos():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/productos")
    )


@dlt.table(
    name="bronze_pl_inventario",
    comment="Inventario crudo (stock por producto y país), ingerido por el pipeline (CSV).",
)
def bronze_pl_inventario():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/inventario")
    )


# ═══════════════════════════════════════════════════════════════════════════
# 📝 TU TURNO 1 · CAPA SILVER
#
# Declara silver_ventas: la bronze limpia, tipada y enriquecida con datos del producto.
# Ya vienen las 3 expectativas — tú completas el cuerpo.
#
# Lo que tiene que hacer:
#   · deduplicar por venta_id
#   · unir con los productos por producto_id (left join) para traer categoria, marca,
#     precio_lista y costo_unitario
#   · tipar 'fecha' de string a timestamp   ← el pendiente del M1 y M2
#   · calcular 'utilidad' = monto - (cantidad * costo_unitario)
#   · quedarte con las columnas útiles
# ═══════════════════════════════════════════════════════════════════════════


@dlt.table(
    name="silver_ventas",
    comment="Ventas limpias, tipadas y enriquecidas con datos del producto (categoría, marca, costo).",
)
@dlt.expect_or_drop("id_valido", "venta_id IS NOT NULL")
@dlt.expect_or_drop("cantidad_positiva", "cantidad > 0")     # descarta el ~2% inválido
@dlt.expect("monto_positivo", "monto > 0")                   # solo avisa
def silver_ventas():
    v = dlt.read_stream("bronze_pl_ventas")
    productos = dlt.read("bronze_pl_productos").drop("_datos_rescatados")

    # 📝 COMPLETA: deduplica, une con productos, tipa la fecha, calcula utilidad, selecciona.
    #
    # return (
    #     v.dropDuplicates(["venta_id"])
    #      .join(productos, "producto_id", "left")
    #      .withColumn("fecha", F.to_timestamp("fecha"))
    #      .withColumn("utilidad",
    #                  F.round(F.col("monto") - F.col("cantidad") * F.col("costo_unitario"), 2))
    #      .select(
    #          "venta_id", "fecha", "producto_id", "nombre_producto", "categoria",
    #          "subcategoria", "marca", "pais", "canal", "cantidad", "precio_unitario",
    #          "descuento_pct", "monto", "costo_unitario", "utilidad", "_archivo_origen",
    #      )
    # )
    raise NotImplementedError("Completa silver_ventas — ver la solución en el guía")


# ═══════════════════════════════════════════════════════════════════════════
# 📝 TU TURNO 2 · CAPA GOLD — la serie diaria del pronóstico
#
# Declara gold_ventas_diarias: silver agregada por DÍA × país × categoría.
# Esta es la tabla que el Módulo 6 le va a dar a ai_forecast: una serie de tiempo limpia.
# Es una vista materializada (recalcula el agregado), no un stream.
#
# Métricas por grupo (día, pais, categoria):
#   unidades (sum cantidad) · monto (sum monto) · utilidad (sum utilidad) · lineas (count)
# ═══════════════════════════════════════════════════════════════════════════


@dlt.table(
    name="gold_ventas_diarias",
    comment="Ventas agregadas por día, país y categoría. Serie de tiempo base del pronóstico.",
)
def gold_ventas_diarias():
    s = dlt.read("silver_ventas")

    # 📝 COMPLETA: agrupa por fecha (solo el día), pais y categoria.
    #
    # return (
    #     s.withColumn("dia", F.to_date("fecha"))
    #      .groupBy("dia", "pais", "categoria")
    #      .agg(
    #          F.sum("cantidad").alias("unidades"),
    #          F.round(F.sum("monto"), 2).alias("monto"),
    #          F.round(F.sum("utilidad"), 2).alias("utilidad"),
    #          F.count("*").alias("lineas"),
    #      )
    # )
    raise NotImplementedError("Completa gold_ventas_diarias — ver la solución en el guía")


# ═══════════════════════════════════════════════════════════════════════════
# CAPA GOLD — ya resueltas. Alimentan el dashboard y la app del Módulo 7.
# ═══════════════════════════════════════════════════════════════════════════


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
    # demanda diaria promedio de los últimos 90 días, por producto y país
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
        # días de cobertura: cuánto dura el stock actual al ritmo de venta reciente
        .withColumn(
            "dias_cobertura",
            F.when(F.col("demanda_diaria") > 0,
                   F.round(F.col("stock_actual") / F.col("demanda_diaria"), 1))
            .otherwise(F.lit(None)),
        )
        # ¿hay que reabastecer? si está por debajo del mínimo, o no alcanza a cubrir el lead time
        .withColumn(
            "necesita_reabastecer",
            (F.col("stock_actual") < F.col("stock_minimo"))
            | (F.col("dias_cobertura") < F.col("lead_time_dias")),
        )
        .select("producto_id", "nombre_producto", "categoria", "pais", "stock_actual",
                "stock_minimo", "demanda_diaria", "dias_cobertura", "lead_time_dias",
                "necesita_reabastecer")
    )
