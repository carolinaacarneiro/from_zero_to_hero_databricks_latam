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
schema = "retail_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())
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
# MAGIC Confirman que el pipeline produjo datos coherentes. Si algo falla, la tarea falla — y el
# MAGIC job manda la notificación que configuraste.

# COMMAND ----------

errores = []

# 1 · gold existe y tiene filas
try:
    filas = spark.table("gold_ventas_diarias").count()
    print(f"✔️  gold_ventas_diarias: {filas:,} filas")
    if filas == 0:
        errores.append("gold_ventas_diarias está vacía")
except Exception as e:
    errores.append(f"no se pudo leer gold: {str(e)[:60]}")

# 2 · hay ventas registradas (el dato tiene sentido)
try:
    monto = spark.sql("SELECT SUM(monto) m FROM gold_ventas_diarias").collect()[0][0]
    print(f"✔️  monto total de ventas: ${monto:,.0f} USD")
    if not monto or monto == 0:
        errores.append("no hay monto de ventas — ¿silver quedó bien?")
except Exception as e:
    errores.append(f"no se pudo leer el monto: {str(e)[:60]}")

# 3 · la serie cubre varios meses (control de que el pronóstico tendrá historia suficiente)
try:
    rango = spark.sql("""
        SELECT MIN(dia) lo, MAX(dia) hi,
               MONTHS_BETWEEN(MAX(dia), MIN(dia)) meses
        FROM gold_ventas_diarias
    """).collect()[0]
    print(f"✔️  rango de fechas: {rango['lo']} → {rango['hi']} (~{rango['meses']:.0f} meses)")
    if rango["meses"] is None or rango["meses"] < 12:
        errores.append(f"la serie cubre menos de 12 meses: {rango['meses']}")
except Exception as e:
    errores.append(f"no se pudo calcular el rango de fechas: {str(e)[:60]}")

# 4 · el inventario tiene productos marcados para reabastecer (el dato del dashboard existe)
try:
    nr = spark.sql("""
        SELECT SUM(CASE WHEN necesita_reabastecer THEN 1 ELSE 0 END) n
        FROM gold_inventario_estado
    """).collect()[0][0]
    print(f"✔️  productos para reabastecer: {nr:,}")
except Exception as e:
    errores.append(f"no se pudo leer el estado de inventario: {str(e)[:60]}")

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
    dbutils.notebook.exit("OK")
