# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🔗 Módulo 3 · Guía del pipeline
# MAGIC
# MAGIC Este notebook te guía para crear y correr el pipeline. El código del
# MAGIC pipeline vive en el archivo **`pipeline_retail.py`** (al lado de este notebook).
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - Este notebook es la **guía**: lees acá, editas el pipeline allá, y lanzas desde la UI.
# MAGIC - Donde dice **📝 TU TURNO**, editas `pipeline_retail.py`.
# MAGIC - 🙋 Trabado más de 3 minutos: levanta la mano.
# MAGIC
# MAGIC ## De dónde venimos
# MAGIC Tienes la capa **bronze** del Módulo 2. Hoy construyes **silver** (limpia) y **gold**
# MAGIC (agregada) — declarándolas, sin escribir el orden en que corren.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos. El método —pipelines declarativos
# MAGIC > con calidad incorporada— es el mismo de cualquier industria.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔎 Antes de empezar: ¿qué es un pipeline declarativo (Lakeflow Declarative Pipelines)?
# MAGIC
# MAGIC En un minuto, para que sepas qué vas a construir.
# MAGIC
# MAGIC ## Qué es
# MAGIC **Lakeflow Declarative Pipelines** es un framework para construir pipelines de datos —batch y
# MAGIC streaming— en **SQL o Python**. La palabra clave es **declarativo**: tú **declaras qué tablas
# MAGIC quieres y de dónde salen**, y la plataforma se encarga del **cómo** — el orden de ejecución,
# MAGIC el paralelismo, los reintentos y el procesamiento incremental.
# MAGIC
# MAGIC ## Declarativo vs. manual — la diferencia
# MAGIC
# MAGIC | ETL manual (imperativo) | Pipeline declarativo |
# MAGIC |---|---|
# MAGIC | Tú escribes **el orden**: primero esto, luego aquello | Declaras las tablas; la plataforma **deduce el orden** (el DAG) de las dependencias |
# MAGIC | Tú orquestas, reintentas y manejas errores a mano | Orquestación, reintentos y recuperación **incorporados** |
# MAGIC | La calidad de datos es un chequeo aparte | La calidad vive **dentro** del pipeline (expectativas) |
# MAGIC | Cientos de líneas de Spark + Structured Streaming | Se reduce a **unas pocas**, declarativas |
# MAGIC
# MAGIC ## Las piezas que vas a usar
# MAGIC
# MAGIC | Pieza | Qué es | Para qué |
# MAGIC |---|---|---|
# MAGIC | **Streaming table** | Procesa cada registro **una sola vez** (append) | Ingesta y crecimiento incremental — tu **bronze** y **silver** |
# MAGIC | **Materialized view** | Recalcula el resultado para reflejar el estado actual | Transformaciones y **agregados** — tu **gold** |
# MAGIC | **Vista** | Se evalúa al momento, no se persiste | Pasos intermedios |
# MAGIC | **Expectativas** | Reglas de calidad fila por fila | Descartar/avisar/detener ante datos malos *(Paso 1.5)* |
# MAGIC
# MAGIC ## Por qué en Databricks — el diferencial
# MAGIC - **El DAG se infiere solo**: describes las piezas, no la coreografía. Agregas un dataset y el
# MAGIC   orden **se recalcula solo**.
# MAGIC - **Batch y streaming en un mismo framework** — no son dos herramientas distintas.
# MAGIC - **Calidad de datos incorporada** (expectativas) — con métricas en cada corrida.
# MAGIC - **Procesamiento incremental** eficiente: las materialized views se mantienen sin reprocesar todo.
# MAGIC - **Observabilidad**: cada corrida deja un event log con linaje, calidad y rendimiento.
# MAGIC - **CDC simplificado** (AUTO CDC, SCD tipo 1 y 2) para cuando el origen manda cambios.
# MAGIC
# MAGIC > 💡 **En resumen:** en vez de escribir *cómo* corre el ETL, declaras *qué* tablas quieres y con
# MAGIC > qué reglas de calidad. Eso es justo lo que hace la medallion (bronze → silver → gold) fácil de
# MAGIC > construir y de confiar. Ahora lo vas a armar.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 · Entiende lo que ya está hecho
# MAGIC
# MAGIC Abre **`pipeline_retail.py`** (en la misma carpeta). Fíjate en tres cosas:
# MAGIC
# MAGIC 1. **Las tres bronze `bronze_pl_*` ya están declaradas** — el pipeline las crea desde los
# MAGIC    archivos crudos. Se llaman `bronze_pl_*` para no chocar con la bronze que hiciste a
# MAGIC    mano en el M2.
# MAGIC 2. **No hay ningún orden escrito.** Cada `@dlt.table` dice qué produce y de quién lee
# MAGIC    (`dlt.read` / `dlt.read_stream`). La plataforma arma el DAG con eso.
# MAGIC 3. **`silver_ventas` y `gold_ventas_diarias` están a medias** — son tus dos TODOs. Las
# MAGIC    otras dos gold (`gold_ventas_producto`, `gold_inventario_estado`) **ya están resueltas**
# MAGIC    porque alimentan el dashboard del Módulo 7.
# MAGIC
# MAGIC | `@dlt.table` | declara una tabla del pipeline |
# MAGIC | `dlt.read("x")` | lee la tabla `x` **completa** (batch) — crea dependencia |
# MAGIC | `dlt.read_stream("x")` | lee `x` de forma **incremental** (streaming) |
# MAGIC | `@dlt.expect...` | una regla de calidad |
# MAGIC
# MAGIC > 🆘 **¿No tienes experiencia o prefieres no escribir el código?** Al lado hay un archivo
# MAGIC > **`pipeline_retail_RESPUESTA`** con todo resuelto. Puedes adjuntar ESE al pipeline en
# MAGIC > vez de `pipeline_retail`, y seguir igual desde el paso 4.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1.5 · ¿Qué son las expectativas (*expectations*)?
# MAGIC
# MAGIC Una **expectativa es una regla que cada fila debe cumplir.** La declaras junto a la tabla,
# MAGIC y el pipeline la revisa **en cada ejecución**, fila por fila. Es la calidad de datos
# MAGIC escrita como código.
# MAGIC
# MAGIC ```python
# MAGIC @dlt.expect_or_drop( "cantidad_positiva",  "cantidad > 0" )
# MAGIC #    │                 │                    │
# MAGIC #    acción            nombre               condición (SQL que debe ser verdadera)
# MAGIC ```
# MAGIC
# MAGIC ## Los tres tipos — la diferencia es qué pasa cuando una fila NO cumple
# MAGIC
# MAGIC | Decorador | Si la fila no cumple… | Cuándo usarlo |
# MAGIC |---|---|---|
# MAGIC | `@dlt.expect` | **Deja pasar la fila**, pero la cuenta y avisa | Vigilar algo sin perder datos |
# MAGIC | `@dlt.expect_or_drop` | **Descarta esa fila** | La fila es inservible pero no crítica |
# MAGIC | `@dlt.expect_or_fail` | **Detiene el pipeline entero** | Si esto falla, nada debe continuar |
# MAGIC
# MAGIC Van **encima de la función** de la tabla, como decoradores. Las tres de silver ya vienen
# MAGIC escritas en `pipeline_retail.py`:
# MAGIC
# MAGIC | Expectativa | Acción | Por qué |
# MAGIC |---|---|---|
# MAGIC | `id_valido` | `expect_or_drop` | Una venta sin id no sirve |
# MAGIC | `cantidad_positiva` | `expect_or_drop` | Descarta el ~2% inválido que plantamos |
# MAGIC | `monto_positivo` | `expect` (solo avisa) | Lo vigilamos sin descartar |
# MAGIC
# MAGIC > 💡 **La decisión de diseño:** ¿por qué `cantidad_positiva` descarta pero `monto_positivo`
# MAGIC > solo avisa? Porque una cantidad ≤ 0 es basura que ensucia los agregados; un monto raro es
# MAGIC > algo que quieres **saber** sin perder la fila. Esa elección es el corazón del módulo.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · 📝 TU TURNO — declara SILVER
# MAGIC
# MAGIC En `pipeline_retail.py`, completa el cuerpo de **`silver_ventas`**.
# MAGIC
# MAGIC | Paso | Cómo |
# MAGIC |---|---|
# MAGIC | Deduplicar | `.dropDuplicates(["venta_id"])` |
# MAGIC | Unir productos | `.join(productos, "producto_id", "left")` |
# MAGIC | Tipar la fecha | `.withColumn("fecha", F.to_timestamp("fecha"))` ← el pendiente del M1/M2 |
# MAGIC | Calcular utilidad | `monto - cantidad * costo_unitario` |
# MAGIC | Seleccionar columnas | `.select(...)` |
# MAGIC
# MAGIC > 💡 Tipar `fecha` a `timestamp` no es un detalle: **el pronóstico del Módulo 6 necesita
# MAGIC > una columna de fecha de verdad** para armar la serie de tiempo. Acá es donde se arregla.
# MAGIC
# MAGIC <details>
# MAGIC <summary>💡 Solución de silver</summary>
# MAGIC
# MAGIC <pre>v = dlt.read_stream("bronze_pl_ventas")
# MAGIC productos = dlt.read("bronze_pl_productos").drop("_datos_rescatados")
# MAGIC return (
# MAGIC     v.dropDuplicates(["venta_id"])
# MAGIC      .join(productos, "producto_id", "left")
# MAGIC      .withColumn("fecha", F.to_timestamp("fecha"))
# MAGIC      .withColumn("utilidad",
# MAGIC                  F.round(F.col("monto") - F.col("cantidad") * F.col("costo_unitario"), 2))
# MAGIC      .select(
# MAGIC          "venta_id", "fecha", "producto_id", "nombre_producto", "categoria",
# MAGIC          "subcategoria", "marca", "pais", "canal", "cantidad", "precio_unitario",
# MAGIC          "descuento_pct", "monto", "costo_unitario", "utilidad", "_archivo_origen",
# MAGIC      )
# MAGIC )</pre>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · 📝 TU TURNO — declara GOLD (la serie del pronóstico)
# MAGIC
# MAGIC Completa **`gold_ventas_diarias`**: silver agregada por **día × país × categoría**. Esta
# MAGIC es la tabla que en el Módulo 6 le vas a dar a `ai_forecast`.
# MAGIC
# MAGIC | Columna | Cálculo |
# MAGIC |---|---|
# MAGIC | `unidades` | `sum(cantidad)` |
# MAGIC | `monto` | `sum(monto)` |
# MAGIC | `utilidad` | `sum(utilidad)` |
# MAGIC | `lineas` | `count(*)` |
# MAGIC
# MAGIC > 💡 Usa `F.to_date("fecha")` para agrupar por día (sin la hora). Una serie de tiempo para
# MAGIC > pronosticar necesita **un valor por día**, no por transacción.
# MAGIC
# MAGIC <details>
# MAGIC <summary>💡 Solución de gold</summary>
# MAGIC
# MAGIC <pre>return (
# MAGIC     s.withColumn("dia", F.to_date("fecha"))
# MAGIC      .groupBy("dia", "pais", "categoria")
# MAGIC      .agg(
# MAGIC          F.sum("cantidad").alias("unidades"),
# MAGIC          F.round(F.sum("monto"), 2).alias("monto"),
# MAGIC          F.round(F.sum("utilidad"), 2).alias("utilidad"),
# MAGIC          F.count("*").alias("lineas"),
# MAGIC      )
# MAGIC )</pre>
# MAGIC </details>
# MAGIC
# MAGIC 👀 **Las otras dos gold ya están resueltas** en el archivo (`gold_ventas_producto` y
# MAGIC `gold_inventario_estado`). Léelas: verás cómo se calcula el margen por producto y los
# MAGIC **días de cobertura** de stock que usará el dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · Crea el pipeline y pega el código
# MAGIC
# MAGIC ## 4.1 · Crea el pipeline
# MAGIC Menú izquierdo → **Jobs & Pipelines → Create → ETL Pipeline** (Lakeflow).
# MAGIC
# MAGIC El editor abre con una carpeta `transformations/` y un archivo de ejemplo. **El código del
# MAGIC pipeline vive dentro de esa carpeta** — no hay un campo para "importar" un notebook: se
# MAGIC pega el código en el archivo.
# MAGIC
# MAGIC ## 4.2 · Pega tu código
# MAGIC 1. Haz clic en `transformations/my_transformation.py`
# MAGIC 2. Selecciona todo (`Cmd/Ctrl + A`) y bórralo
# MAGIC 3. Abre `pipeline_retail.py` (o `pipeline_retail_RESPUESTA.py`), copia **todo** su
# MAGIC    contenido y pégalo ahí
# MAGIC
# MAGIC ## 4.3 · No tienes que configurar la ruta
# MAGIC El código **detecta solo** tu volumen `raw` desde tu usuario. No hace falta tocar Settings.
# MAGIC
# MAGIC ## 4.4 · Elige el destino y ejecuta
# MAGIC - Arriba, confirma el **catálogo** y **schema** de destino (los tuyos, los del M2). La
# MAGIC   celda de abajo te dice cuáles son.
# MAGIC - Presiona **Run pipeline** (arriba a la derecha). Tarda 1–2 minutos en arrancar.

# COMMAND ----------

import re
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
_schema = "retail_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())
_ocultos = ("system", "samples", "__databricks_internal", "hive_metastore")
_cat = None
for c in [r[0] for r in spark.sql("SHOW CATALOGS").collect() if r[0].lower() not in _ocultos]:
    try:
        if _schema in [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{c}`").collect()]:
            _cat = c
            break
    except Exception:
        continue

print("En Settings del pipeline, elige como DESTINO:\n")
print(f"  Default catalog : {_cat}")
print(f"  Default schema  : {_schema}")
print()
print("La ruta de los archivos se detecta sola — no tienes que configurar nada más.")
print("Presiona 'Run pipeline' cuando esté listo.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 5 · Mira el DAG que armó la plataforma
# MAGIC
# MAGIC Mientras el pipeline arranca, mira el **grafo** en el centro:
# MAGIC
# MAGIC ```
# MAGIC   bronze_pl_ventas ──────┐
# MAGIC   bronze_pl_productos ───┼─→ silver_ventas ─┬─→ gold_ventas_diarias
# MAGIC   bronze_pl_inventario ──┘                  ├─→ gold_ventas_producto
# MAGIC                                             └─→ gold_inventario_estado
# MAGIC ```
# MAGIC
# MAGIC **Seis nodos, la medallion completa, y tú nunca escribiste el orden.** La plataforma lo
# MAGIC dedujo de los `dlt.read`.
# MAGIC
# MAGIC 👀 Si agregaras un dataset nuevo en el medio, el DAG **se recalcularía solo**. Eso es lo
# MAGIC que significa declarativo: describes las piezas, no la coreografía.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · El panel de calidad
# MAGIC
# MAGIC Cuando el pipeline termine (todo en verde), haz clic en el nodo **`silver_ventas`**. En el
# MAGIC panel de la derecha verás las **métricas de las expectativas**:
# MAGIC
# MAGIC | Expectativa | Qué mirar |
# MAGIC |---|---|
# MAGIC | `cantidad_positiva` | **cuántas filas descartó** — debería ser ~2% |
# MAGIC | `monto_positivo` | cuántas avisó (sin descartar) |
# MAGIC | `id_valido` | cuántas descartó por id nulo |
# MAGIC
# MAGIC Esas cifras no las calculaste tú: el pipeline las produce en **cada** ejecución.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · Rompe algo a propósito ⭐ *(opcional)*
# MAGIC
# MAGIC > 🟡 **Paso opcional.** Si vas con el tiempo justo, sáltalo y pasa al checkpoint (Paso 8) —
# MAGIC > tu pipeline ya está completo. Este paso es para **ver la calidad en acción**; vale la pena
# MAGIC > si te queda tiempo.
# MAGIC
# MAGIC Vas a meter ventas inválidas al origen, re-ejecutar el pipeline, y **ver subir el conteo
# MAGIC de descartes**. Ejecuta esta celda: agrega un archivo con 500 ventas de `cantidad = 0`.

# COMMAND ----------

import re
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
_schema = "retail_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())
_cat = None
for c in [r[0] for r in spark.sql("SHOW CATALOGS").collect()
          if r[0].lower() not in ("system", "samples", "__databricks_internal", "hive_metastore")]:
    try:
        if _schema in [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{c}`").collect()]:
            _cat = c
            break
    except Exception:
        continue
RAW = f"/Volumes/{_cat}/{_schema}/raw"

from pyspark.sql import functions as F
malas = (spark.range(500)
    .withColumn("venta_id", F.concat(F.lit("VTAMALA"), F.col("id").cast("string")))
    .withColumn("fecha", F.lit("2026-03-01 12:00:00"))
    .withColumn("producto_id", F.lit("PRD00010"))
    .withColumn("pais", F.lit("CO"))
    .withColumn("canal", F.lit("online"))
    .withColumn("cantidad", F.lit(0))              # ← inválido: la expectativa lo descarta
    .withColumn("precio_unitario", F.lit(19.99))
    .withColumn("descuento_pct", F.lit(0.0))
    .withColumn("monto", F.lit(0.0))
    .drop("id"))

# OJO: la carpeta NO puede empezar con "_" ni "." — Auto Loader (y Spark) ignoran esos nombres.
(malas.coalesce(1).write.mode("overwrite").format("json")
    .save(f"{RAW}/ventas/lote_invalido"))

print(f"✅ Metí 500 ventas con cantidad = 0 en {RAW}/ventas/lote_invalido")
print("   Ahora vuelve al pipeline y presiona 'Run pipeline' otra vez.")
print("   Cuando termine, mira 'cantidad_positiva' en el panel de calidad: descarta ~500.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Vuelve al pipeline → Start.** Cuando termine, abre otra vez el panel de calidad de
# MAGIC `silver_ventas`: el conteo de filas descartadas por `cantidad_positiva` **subió en ~500**.
# MAGIC
# MAGIC 🎉 Eso es la calidad como código: no escribiste ninguna consulta de verificación. El
# MAGIC pipeline atrapó los datos malos **solo**.
# MAGIC
# MAGIC > 💡 Para dejar tus datos limpios después: borra el lote con
# MAGIC > `dbutils.fs.rm(f"{RAW}/ventas/lote_invalido", True)` y re-ejecuta.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 8 · Verifica tu checkpoint
# MAGIC Ejecuta esta celda (el catálogo/schema se detectan solos).

# COMMAND ----------

spark.sql(f"USE `{_cat}`.`{_schema}`")
resultados = []

try:
    fg = spark.table("gold_ventas_diarias").count()
    resultados.append((fg > 0, f"gold_ventas_diarias existe y tiene {fg:,} filas (días×país×categoría)"))
except Exception:
    resultados.append((False, "No encuentro gold_ventas_diarias — ¿corrió el pipeline?"))

try:
    tipo = spark.table("silver_ventas").schema["fecha"].dataType.simpleString()
    resultados.append((tipo == "timestamp", f"silver.fecha quedó tipada como {tipo} (se esperaba timestamp)"))
except Exception:
    resultados.append((False, "No pude leer silver_ventas"))

try:
    nr = spark.sql("SELECT SUM(CASE WHEN necesita_reabastecer THEN 1 ELSE 0 END) n "
                   "FROM gold_inventario_estado").collect()[0][0]
    resultados.append((nr and nr > 0, f"gold_inventario_estado marca {nr:,} productos para reabastecer"))
except Exception:
    resultados.append((False, "No pude leer gold_inventario_estado"))

print("=" * 68)
print("  MÓDULO 3 · CHECKPOINT")
print("=" * 68)
for ok, msg in resultados:
    print(f"  {'✅' if ok else '❌'}  {msg}")
print("=" * 68)
if all(ok for ok, _ in resultados):
    print("""
  🎉 ¡Módulo 3 completo!
    · Declaraste silver y gold — la plataforma resolvió el orden
    · Definiste reglas de calidad y viste cuántas filas afectaron
    · Rompiste algo a propósito y viste subir los descartes

  👉 En el Módulo 4 vas a gobernar estas tablas: quién ve qué filas
     (por país), qué columnas se enmascaran (el costo), y de dónde
     viene cada dato.
""")
else:
    print("  ⚠️  Revisa los ❌. Lo más común: el pipeline no terminó, o falta completar un TODO.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Lo que te llevas
# MAGIC
# MAGIC | Concepto | Dónde lo viviste |
# MAGIC |---|---|
# MAGIC | **Declarativo**: declaras el qué, no el cómo | Nunca escribiste el orden del DAG |
# MAGIC | **El DAG se infiere** de las dependencias | 6 nodos ordenados solos |
# MAGIC | **Silver limpia, gold agrega** | `fecha` por fin es timestamp; gold responde negocio |
# MAGIC | **La calidad es código** | 3 expectativas, en cada corrida |
# MAGIC | **Gold para varios consumos** | serie del pronóstico, ranking y estado de inventario |
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC Tienes datos **confiables y útiles**, pero **cualquiera con acceso los ve completos**. En
# MAGIC el **Módulo 4** aplicas gobierno: filtras filas por país y enmascaras el costo unitario
# MAGIC — y compruebas el efecto en tus propios datos.
