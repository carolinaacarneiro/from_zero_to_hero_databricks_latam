# Databricks notebook source
# MAGIC %md
# MAGIC # ✔️ Módulo 5 · Tarea de validación
# MAGIC
# MAGIC Este notebook es **la tercera tarea del job**. No se corre a mano: el job lo ejecuta al
# MAGIC final, después de la ingesta y el pipeline, para confirmar que los datos quedaron bien.
# MAGIC
# MAGIC Lee el parámetro `fecha_proceso` del job y corre unas consultas de control sobre `gold`.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos.

# COMMAND ----------

# lee el parámetro del job (con un valor por defecto para poder probarlo suelto)
dbutils.widgets.text("fecha_proceso", "2026-03-01", "Fecha a procesar")
fecha_proceso = dbutils.widgets.get("fecha_proceso")

# MAGIC %md
# MAGIC # Paso 1 · Declara tu catálogo y tu schema
# MAGIC
# MAGIC **Corre la celda de abajo una vez** para que aparezcan dos campos arriba del notebook.
# MAGIC Luego **escribe en ellos el catálogo y el schema donde estás trabajando** — los
# MAGIC **mismos** que declaraste en el Módulo 1 y que vienes usando en todos los módulos.
# MAGIC
# MAGIC > ⚠️ **No adivinamos nada por ti.** Tú declaras explícitamente dónde trabajas, para que
# MAGIC > no haya duda de en qué catálogo y schema estás. Si los dejas vacíos, el notebook se
# MAGIC > detiene y te lo pide.

# COMMAND ----------

# ═══ Declara aquí tu espacio de trabajo ═══════════════════════════════════
# Crea los dos campos (arriba). Escribe en ellos tu catálogo y tu schema — los MISMOS
# de siempre. No se infiere ni se adivina nada: tú decides dónde trabajas.
dbutils.widgets.text("catalogo", "", "1 · Catálogo")
dbutils.widgets.text("schema",   "", "2 · Schema")

print("👆 Escribe tu CATÁLOGO y tu SCHEMA en los dos campos de arriba, y sigue con la "
      "siguiente celda.")

# COMMAND ----------

# ═══ Lee y valida lo que declaraste ═══════════════════════════════════════
catalogo = dbutils.widgets.get("catalogo").strip()
schema = dbutils.widgets.get("schema").strip()
usuario = spark.sql("SELECT current_user()").collect()[0][0]

# 1 · no puede estar vacío — obliga a declarar conscientemente
if not catalogo or not schema:
    raise Exception(
        "\n❌ Falta declarar tu espacio de trabajo.\n"
        "   Escribe el CATÁLOGO y el SCHEMA en los campos de arriba (los mismos que usaste "
        "en el Módulo 1) y vuelve a correr esta celda.\n"
    )

# 2 · el catálogo tiene que existir
catalogos = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
if catalogo not in catalogos:
    _visibles = [c for c in catalogos
                 if c.lower() not in ("system", "samples", "__databricks_internal", "hive_metastore")]
    raise Exception(
        f"\n❌ El catálogo '{catalogo}' no existe o no lo ves.\n"
        f"   👉 Escribe uno de estos en el campo de arriba:\n"
        + "".join(f"        · {c}\n" for c in _visibles)
    )

# 3 · el schema tiene que existir (lo creaste en el Módulo 1)
schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{catalogo}`").collect()]
if schema not in schemas:
    raise Exception(
        f"\n❌ El schema '{catalogo}.{schema}' no existe.\n"
        f"   Corre los módulos anteriores con estos mismos valores, o corrige el nombre "
        f"arriba.\n"
    )

spark.sql(f"USE `{catalogo}`.`{schema}`")

print(f"👤 Usuario       : {usuario}")
print(f"📁 Trabajarás en : {catalogo}.{schema}")
print("✅ Espacio declarado y verificado. Nadie más toca este schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas de control
# MAGIC Confirman que el pipeline produjo datos coherentes. Si algo falla, la tarea falla —
# MAGIC y el job manda la notificación que configuraste.

# COMMAND ----------

from pyspark.sql import functions as F

errores = []

# 1 · gold existe y tiene filas
try:
    filas = spark.table("gold_riesgo_diario").count()
    print(f"✔️  gold_riesgo_diario: {filas:,} filas")
    if filas == 0:
        errores.append("gold_riesgo_diario está vacía")
except Exception as e:
    errores.append(f"no se pudo leer gold: {str(e)[:60]}")

# 2 · hay fraudes registrados (el dato tiene sentido)
try:
    fr = spark.sql("SELECT SUM(transacciones_fraude) f FROM gold_riesgo_diario").collect()[0][0]
    print(f"✔️  transacciones de fraude: {fr:,}")
    if not fr or fr == 0:
        errores.append("no hay transacciones de fraude — ¿silver quedó bien?")
except Exception as e:
    errores.append(f"no se pudo leer la métrica de fraude: {str(e)[:60]}")

# 3 · la tasa de fraude está en un rango razonable (control de calidad de negocio)
try:
    tasa = spark.sql("""
        SELECT SUM(transacciones_fraude) / SUM(total_transacciones) AS t
        FROM gold_riesgo_diario
    """).collect()[0][0]
    print(f"✔️  tasa de fraude global: {tasa:.2%}")
    if tasa is None or not (0.005 <= tasa <= 0.05):
        errores.append(f"tasa de fraude fuera de rango esperado (0,5%–5%): {tasa}")
except Exception as e:
    errores.append(f"no se pudo calcular la tasa: {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado
# MAGIC Si hay errores, la tarea falla a propósito — así el job lo marca en rojo y avisa.

# COMMAND ----------

print("=" * 60)
print(f"  VALIDACIÓN · fecha_proceso = {fecha_proceso}")
print("=" * 60)
if errores:
    for e in errores:
        print(f"  ❌ {e}")
    print("=" * 60)
    raise Exception(f"La validación encontró {len(errores)} problema(s). El job debe fallar.")
else:
    print("  ✅ Todos los controles pasaron. Los datos del día están bien.")
    print("=" * 60)
    # el job puede leer esta salida
    dbutils.notebook.exit("OK")
