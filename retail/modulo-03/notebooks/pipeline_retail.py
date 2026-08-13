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
# MAGIC | `bronze_pl_ventas`, `bronze_pl_productos`, `bronze_pl_inventario` | ✅ ya declaradas |
# MAGIC | `silver_ventas` | ✅ ya declarada |
# MAGIC | `gold_ventas_diarias`, `gold_ventas_producto`, `gold_inventario_estado` | ✅ ya declaradas |
# MAGIC
# MAGIC > 📖 **El código ya está completo.** No hay nada que editar: el foco del módulo es **entender**
# MAGIC > cómo se declara un pipeline, leer cada capa, entender las **expectativas de calidad**, crear
# MAGIC > el pipeline en la UI, correrlo y leer el DAG y el panel de calidad. El guía `03_guia_pipeline`
# MAGIC > te lleva paso a paso.
# MAGIC
# MAGIC > 💡 **¿Por qué `bronze_pl_*`?** En el Módulo 2 ya creaste `bronze_ventas` a mano con Auto
# MAGIC > Loader. Este pipeline crea **su propia** bronze (con otro nombre) para no chocar. En un
# MAGIC > proyecto real tendrías **uno** de los dos caminos, no ambos; acá los ves los dos para
# MAGIC > aprender. Silver y gold sí usan el nombre canónico: son las que consumen los módulos siguientes.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# ═══════════════════════════════════════════════════════════════════════════
# ¿Dónde están los archivos crudos? El pipeline necesita tu catálogo y tu schema.
# El CATÁLOGO se lee solo del Default catalog del pipeline. El SCHEMA hay que declararlo en
# Settings -> Configuration como  z2h.schema = <tu_schema>  (el Default schema del pipeline
# NO es visible desde el código). Ver el guía 03_guia_pipeline, Paso 4.
# ═══════════════════════════════════════════════════════════════════════════

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
# CAPA SILVER — el dato se vuelve CONFIABLE
#
# silver_ventas = bronze limpia, tipada y enriquecida con datos del producto.
# Lee de las bronze de ESTE pipeline: dlt.read_stream() para la venta (stream incremental),
# dlt.read() para el catálogo de productos (dimensión estática). Esas llamadas crean las
# DEPENDENCIAS del DAG — nadie escribe el orden.
#
# ── Las 3 EXPECTATIVAS de calidad (lo más importante de leer) ──────────────
#   @dlt.expect_or_drop("id_valido", ...)       → DESCARTA la fila si no cumple
#   @dlt.expect_or_drop("cantidad_positiva", ...) → DESCARTA (limpia el ~2% inválido plantado)
#   @dlt.expect("monto_positivo", ...)         → solo AVISA (la fila pasa igual)
# La decisión de diseño: cantidad ≤ 0 es basura que ensucia los agregados → se descarta;
# un monto raro es algo que quieres SABER sin perder la fila → solo se avisa.
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


# ═══════════════════════════════════════════════════════════════════════════
# CAPA GOLD — el dato se vuelve ÚTIL para el negocio
#
# gold_ventas_diarias = silver agregada por día × país × categoría.
# Es una MATERIALIZED VIEW: recalcula el agregado para reflejar el estado actual (no es un stream).
# Lee de silver con dlt.read() → gold depende de silver en el DAG.
# Esta es la serie de tiempo que el Módulo 6 le dará a ai_forecast para pronosticar.
# Responde: ¿cuántas unidades se vendieron, por cuánto dinero, con qué utilidad, cada día
# en cada país y categoría?
# ═══════════════════════════════════════════════════════════════════════════


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
