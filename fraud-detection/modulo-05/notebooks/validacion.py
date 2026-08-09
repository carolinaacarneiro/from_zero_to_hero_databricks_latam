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

import re
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
schema = "fin_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())
_ocultos = ("system", "samples", "__databricks_internal", "hive_metastore")
catalogo = None
for c in [r[0] for r in spark.sql("SHOW CATALOGS").collect() if r[0].lower() not in _ocultos]:
    try:
        if schema in [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{c}`").collect()]:
            catalogo = c
            break
    except Exception:
        continue
spark.sql(f"USE `{catalogo}`.`{schema}`")

print(f"Validando {catalogo}.{schema}")
print(f"Parámetro fecha_proceso = {fecha_proceso}")

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
