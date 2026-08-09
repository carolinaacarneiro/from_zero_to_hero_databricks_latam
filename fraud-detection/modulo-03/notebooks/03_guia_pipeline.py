# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🔗 Módulo 3 · Guía del pipeline
# MAGIC
# MAGIC Este notebook te guía para crear y correr el pipeline. El código del
# MAGIC pipeline vive en el archivo **`pipeline_fraude.py`** (al lado de este notebook).
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - Este notebook es la **guía**: lees acá, editas el pipeline allá, y lanzas desde la UI.
# MAGIC - Donde dice **📝 TU TURNO**, editas `pipeline_fraude.py`.
# MAGIC - 🙋 Trabado más de 3 minutos: levanta la mano.
# MAGIC
# MAGIC ## De dónde venimos
# MAGIC Tienes la capa **bronze** del Módulo 2. Hoy construyes **silver** (limpia) y **gold**
# MAGIC (agregada), declarándolas — sin escribir el orden en que corren.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos. El método —pipelines
# MAGIC > declarativos con calidad incorporada— es el mismo de cualquier industria.

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
# MAGIC Abre **`pipeline_fraude.py`** (en la misma carpeta). Fíjate en tres cosas:
# MAGIC
# MAGIC 1. **`bronze_pl_transacciones` y `bronze_pl_clientes` ya están declaradas** — el
# MAGIC    pipeline las crea desde los archivos crudos. Se llaman `bronze_pl_*` para no chocar
# MAGIC    con la bronze que hiciste a mano en el M2.
# MAGIC 2. **No hay ningún orden escrito.** Cada `@dlt.table` dice qué produce y de quién lee
# MAGIC    (`dlt.read` / `dlt.read_stream`). La plataforma arma el DAG con eso.
# MAGIC 3. **`silver_transacciones` y `gold_riesgo_diario` están a medias** — son tus dos TODOs.
# MAGIC
# MAGIC | `@dlt.table` | declara una tabla del pipeline |
# MAGIC | `dlt.read("x")` | lee la tabla `x` **completa** (batch) — crea dependencia |
# MAGIC | `dlt.read_stream("x")` | lee `x` de forma **incremental** (streaming) |
# MAGIC | `@dlt.expect...` | una regla de calidad |
# MAGIC
# MAGIC No ejecutes nada todavía: primero completa los TODOs, después lanzas el pipeline entero.
# MAGIC
# MAGIC > 🆘 **¿No tienes experiencia o prefieres no escribir el código?** Al lado hay un
# MAGIC > archivo **`pipeline_fraude_RESPUESTA`** con todo resuelto. Puedes adjuntar ESE al
# MAGIC > pipeline en vez de `pipeline_fraude`, y seguir igual desde el paso 4. Verlo funcionar
# MAGIC > y leerlo también enseña — no es trampa.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1.5 · ¿Qué son las expectativas (*expectations*)?
# MAGIC
# MAGIC Antes de declarar silver, entiende su parte más importante: las **expectativas de
# MAGIC calidad**.
# MAGIC
# MAGIC ## Qué son
# MAGIC Una **expectativa es una regla que cada fila debe cumplir.** La declaras junto a la
# MAGIC tabla, y el pipeline la revisa **en cada ejecución**, fila por fila. Es la calidad de
# MAGIC datos escrita como código — no un chequeo manual que alguien corre el viernes.
# MAGIC
# MAGIC Una expectativa tiene tres partes:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.expect_or_drop( "monto_positivo",   "monto > 0" )
# MAGIC #    │                 │                  │
# MAGIC #    acción            nombre             condición (SQL que debe ser verdadera)
# MAGIC ```
# MAGIC
# MAGIC ## Los tres tipos — la diferencia es qué pasa cuando una fila NO cumple
# MAGIC
# MAGIC | Decorador | Si la fila no cumple… | Cuándo usarlo |
# MAGIC |---|---|---|
# MAGIC | `@dlt.expect` | **Deja pasar la fila**, pero la cuenta y avisa | Quieres vigilar algo sin perder datos |
# MAGIC | `@dlt.expect_or_drop` | **Descarta esa fila** (no entra a la tabla) | La fila es inservible pero no crítica |
# MAGIC | `@dlt.expect_or_fail` | **Detiene el pipeline entero** | Si esto falla, nada debe continuar |
# MAGIC
# MAGIC ## Cómo se declaran
# MAGIC Van **encima de la función** de la tabla, como decoradores. Puedes poner varias:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="silver_transacciones")
# MAGIC @dlt.expect_or_drop("id_valido", "transaccion_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("monto_positivo", "monto > 0")
# MAGIC @dlt.expect("moneda_conocida", "moneda IN ('COP','USD')")
# MAGIC def silver_transacciones():
# MAGIC     ...
# MAGIC ```
# MAGIC
# MAGIC La **condición** es SQL que debe dar verdadero para una fila buena. `monto > 0` significa
# MAGIC *«espero que el monto sea positivo»*; la fila que no lo cumpla recibe la acción del
# MAGIC decorador.
# MAGIC
# MAGIC > 💡 **La decisión de diseño del módulo** es elegir la acción correcta para cada regla:
# MAGIC > ¿esta condición amerita **descartar** la fila, solo **avisar**, o **detener** todo?
# MAGIC > Es lo que separa una regla útil de una molesta.
# MAGIC >
# MAGIC > 📊 Cada expectativa deja una **métrica** en cada corrida: cuántas filas cumplieron y
# MAGIC > cuántas no. Eso lo verás en el panel de calidad (paso 6) y en el event log.
# MAGIC
# MAGIC Las tres expectativas de silver ya vienen escritas en `pipeline_fraude.py`. En el
# MAGIC siguiente paso completas el **cuerpo** de la función.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · 📝 TU TURNO — declara SILVER
# MAGIC
# MAGIC En `pipeline_fraude.py`, completa el cuerpo de **`silver_transacciones`**.
# MAGIC
# MAGIC ## Qué tiene que lograr
# MAGIC Convertir bronze en datos **confiables**: sin duplicados, con tipos correctos, y con
# MAGIC los datos del cliente pegados a cada transacción.
# MAGIC
# MAGIC | Paso | Cómo |
# MAGIC |---|---|
# MAGIC | Deduplicar | `.dropDuplicates(["transaccion_id"])` |
# MAGIC | Unir clientes | `.join(clientes, "cliente_id", "left")` *(quita antes `region` de clientes)* |
# MAGIC | Tipar la fecha | `.withColumn("fecha", F.to_timestamp("fecha"))` ← el pendiente del M1/M2 |
# MAGIC | Seleccionar columnas | `.select(...)` con las útiles |
# MAGIC
# MAGIC ## Las 3 expectativas de silver (ya escritas en el archivo)
# MAGIC
# MAGIC Aplicando lo que viste en el paso 1.5:
# MAGIC
# MAGIC | Expectativa | Acción | Por qué |
# MAGIC |---|---|---|
# MAGIC | `id_valido` | `expect_or_drop` | Una fila sin id no sirve para nada |
# MAGIC | `monto_positivo` | `expect_or_drop` | Descarta el ~2% inválido que plantamos |
# MAGIC | `moneda_conocida` | `expect` (solo avisa) | Queremos vigilarlo, no descartarlo |
# MAGIC
# MAGIC > 💡 **La decisión de diseño:** ¿por qué `monto_positivo` descarta pero
# MAGIC > `moneda_conocida` solo avisa? Porque un monto ≤ 0 es basura que ensucia los agregados;
# MAGIC > una moneda rara es algo que quieres **saber** sin perder la fila. Esa elección —qué
# MAGIC > acción para cada regla— es el corazón del módulo.
# MAGIC
# MAGIC <details>
# MAGIC <summary>💡 Solución de silver</summary>
# MAGIC
# MAGIC <pre>tx = dlt.read_stream("bronze_pl_transacciones")
# MAGIC clientes = dlt.read("bronze_pl_clientes").drop("region", "_datos_rescatados")
# MAGIC return (
# MAGIC     tx.dropDuplicates(["transaccion_id"])
# MAGIC       .join(clientes, "cliente_id", "left")
# MAGIC       .withColumn("fecha", F.to_timestamp("fecha"))
# MAGIC       .select(
# MAGIC           "transaccion_id", "cliente_id", "numero_tarjeta", "monto", "moneda",
# MAGIC           "comercio", "categoria_comercio", "canal", "pais", "region", "fecha",
# MAGIC           "es_fraude", "segmento", "antiguedad_meses", "score_crediticio",
# MAGIC           "_archivo_origen",
# MAGIC       )
# MAGIC )</pre>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · 📝 TU TURNO — declara GOLD
# MAGIC
# MAGIC Completa **`gold_riesgo_diario`**: silver agregada por **día × región × categoría**.
# MAGIC
# MAGIC ## Las métricas por grupo
# MAGIC
# MAGIC | Columna | Cálculo |
# MAGIC |---|---|
# MAGIC | `total_transacciones` | `count(*)` |
# MAGIC | `monto_total` | `sum(monto)` |
# MAGIC | `transacciones_fraude` | `sum(es_fraude)` |
# MAGIC | `monto_fraude` | `sum(monto)` donde es fraude |
# MAGIC | `tasa_fraude` | fraudes / total |
# MAGIC | `ticket_promedio` | monto_total / total |
# MAGIC
# MAGIC > 💡 Usa `F.to_date("fecha")` para agrupar por día (sin la hora).
# MAGIC
# MAGIC <details>
# MAGIC <summary>💡 Solución de gold</summary>
# MAGIC
# MAGIC <pre>return (
# MAGIC     s.withColumn("dia", F.to_date("fecha"))
# MAGIC      .groupBy("dia", "region", "categoria_comercio")
# MAGIC      .agg(
# MAGIC          F.count("*").alias("total_transacciones"),
# MAGIC          F.round(F.sum("monto"), 0).alias("monto_total"),
# MAGIC          F.sum(F.when(F.col("es_fraude"), 1).otherwise(0)).alias("transacciones_fraude"),
# MAGIC          F.round(F.sum(F.when(F.col("es_fraude"), F.col("monto")).otherwise(0)), 0)
# MAGIC              .alias("monto_fraude"),
# MAGIC      )
# MAGIC      .withColumn("tasa_fraude",
# MAGIC                  F.round(F.col("transacciones_fraude") / F.col("total_transacciones"), 4))
# MAGIC      .withColumn("ticket_promedio",
# MAGIC                  F.round(F.col("monto_total") / F.col("total_transacciones"), 0))
# MAGIC )</pre>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · Crea el pipeline y pega el código
# MAGIC
# MAGIC ## 4.1 · Crea el pipeline
# MAGIC Menú izquierdo → **Jobs & Pipelines → Create → ETL Pipeline** (Lakeflow).
# MAGIC
# MAGIC El editor abre con una carpeta `transformations/` y un archivo de ejemplo
# MAGIC `my_transformation.py`. **El código del pipeline vive dentro de esa carpeta** — no hay
# MAGIC un campo para "importar" un notebook: se pega el código en el archivo.
# MAGIC
# MAGIC ## 4.2 · Pega tu código
# MAGIC 1. Haz clic en `transformations/my_transformation.py`
# MAGIC 2. Selecciona todo (`Cmd/Ctrl + A`) y bórralo
# MAGIC 3. Abre `pipeline_fraude.py` (o `pipeline_fraude_RESPUESTA.py`), copia **todo** su
# MAGIC    contenido y pégalo ahí
# MAGIC 4. *(opcional)* renombra el archivo a `pipeline_fraude.py` con el menú de los tres puntos
# MAGIC
# MAGIC > 💡 Las líneas `# Databricks notebook source` y `# MAGIC %md` quedan como comentarios
# MAGIC > inofensivos. No hace falta quitarlas.
# MAGIC
# MAGIC ## 4.3 · No tienes que configurar la ruta
# MAGIC El código **detecta solo** tu volumen `raw` desde tu usuario. No hace falta tocar
# MAGIC Settings ni definir `z2h.raw_path` — lo hicimos automático para esta parte.
# MAGIC
# MAGIC ## 4.4 · Elige el destino y ejecuta
# MAGIC - Arriba, junto al nombre, confirma el **catálogo** y **schema** de destino (los tuyos,
# MAGIC   los del M2). La celda de abajo te dice cuáles son.
# MAGIC - Presiona **Run pipeline** (arriba a la derecha). Tarda 1–2 minutos en arrancar.

# COMMAND ----------

import re
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
_schema = "fin_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())
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
# MAGIC Mientras el pipeline arranca, mira el **grafo** en el centro de la pantalla:
# MAGIC
# MAGIC ```
# MAGIC   bronze_pl_transacciones ─┐
# MAGIC                            ├─→ silver_transacciones ─→ gold_riesgo_diario
# MAGIC   bronze_pl_clientes ──────┘
# MAGIC ```
# MAGIC
# MAGIC **Cuatro nodos, la medallion completa, y tú nunca escribiste el orden.** La plataforma lo dedujo de los
# MAGIC `dlt.read`: gold lee de silver, silver lee de las dos bronze, así que bronze va primero.
# MAGIC
# MAGIC 👀 Si agregaras un dataset nuevo en el medio, el DAG **se recalcularía solo**. Eso es lo
# MAGIC que significa declarativo: describes las piezas, no la coreografía.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · El panel de calidad
# MAGIC
# MAGIC Cuando el pipeline termine (todo en verde), haz clic en el nodo **`silver_transacciones`**.
# MAGIC En el panel de la derecha vas a ver las **métricas de las expectativas**:
# MAGIC
# MAGIC | Expectativa | Qué mirar |
# MAGIC |---|---|
# MAGIC | `monto_positivo` | **cuántas filas descartó** — debería ser ~2% |
# MAGIC | `moneda_conocida` | cuántas avisó (sin descartar) |
# MAGIC | `id_valido` | cuántas descartó por id nulo |
# MAGIC
# MAGIC Esas cifras no las calculaste tú: el pipeline las produce en **cada** ejecución. La
# MAGIC calidad quedó registrada, no depende de que alguien la revise.
# MAGIC
# MAGIC También puedes leerlas por SQL desde el event log:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Reemplaza <catalogo> y <schema> por los tuyos.
# MAGIC -- El event log registra cada expectativa y cuántas filas afectó.
# MAGIC SELECT
# MAGIC     timestamp,
# MAGIC     details:flow_progress:data_quality:expectations
# MAGIC FROM event_log(TABLE(`<catalogo>`.`<schema>`.gold_riesgo_diario))
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 5
# MAGIC ```
# MAGIC
# MAGIC > 💡 El `event_log()` es una función que expone el log del pipeline como tabla. Ahí vive
# MAGIC > el linaje, las métricas de calidad y el rendimiento de cada corrida.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · Rompe algo a propósito ⭐ *(opcional)*
# MAGIC
# MAGIC > 🟡 **Paso opcional.** Si vas con el tiempo justo, sáltalo y pasa al checkpoint (Paso 8) —
# MAGIC > tu pipeline ya está completo. Este paso es para **ver la calidad en acción**; vale la pena
# MAGIC > si te queda tiempo.
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Meter transacciones inválidas al origen, re-ejecutar el pipeline, y **ver subir el
# MAGIC conteo de descartes**. Así la calidad deja de ser teoría.
# MAGIC
# MAGIC Ejecuta esta celda: agrega un archivo con 500 transacciones de `monto = 0` (que la
# MAGIC expectativa `monto_positivo` debe descartar).

# COMMAND ----------

import re
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
_schema = "fin_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())
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
    .withColumn("transaccion_id", F.concat(F.lit("TXMALA"), F.col("id").cast("string")))
    .withColumn("cliente_id", F.lit("CLI0000010"))
    .withColumn("monto", F.lit(0.0))               # ← inválido: la expectativa lo descarta
    .withColumn("moneda", F.lit("COP"))
    .withColumn("comercio", F.lit("SuperMercado Andino"))
    .withColumn("categoria_comercio", F.lit("supermercado"))
    .withColumn("canal", F.lit("online"))
    .withColumn("pais", F.lit("CO"))
    .withColumn("region", F.lit("centro"))
    .withColumn("fecha", F.lit("2026-03-01 12:00:00"))
    .withColumn("es_fraude", F.lit(False))
    .withColumn("dispositivo_id", F.lit(None).cast("string"))
    .withColumn("numero_tarjeta", F.lit("4539-0000-0000-0000"))
    .drop("id"))

# OJO: la carpeta NO puede empezar con "_" ni "." — Auto Loader (y Spark) ignoran esos
# nombres (los tratan como archivos internos, tipo _delta_log). Si la llamas "_lote_invalido"
# el pipeline no la ve y no descarta nada.
(malas.coalesce(1).write.mode("overwrite").format("json")
    .save(f"{RAW}/transacciones/lote_invalido"))

print(f"✅ Metí 500 transacciones con monto = 0 en {RAW}/transacciones/lote_invalido")
print("   Ahora vuelve al pipeline y presiona 'Run pipeline' otra vez.")
print("   Cuando termine, mira 'monto_positivo' en el panel de calidad: descarta ~500.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Vuelve al pipeline → Start.** Cuando termine, abre otra vez el panel de calidad de
# MAGIC `silver_transacciones` y compara: el conteo de filas descartadas por `monto_positivo`
# MAGIC **subió en ~500**.
# MAGIC
# MAGIC 🎉 Eso es la calidad como código: no tuviste que escribir ninguna consulta de
# MAGIC verificación. El pipeline atrapó los datos malos **solo**, y los dejó registrados.
# MAGIC
# MAGIC > 💡 Para dejar tus datos limpios después: borra el lote inválido con
# MAGIC > `dbutils.fs.rm(f"{RAW}/transacciones/lote_invalido", True)` y re-ejecuta.
# MAGIC
# MAGIC ### 🏆 Reto opcional
# MAGIC Agrega una **cuarta expectativa de negocio** a silver, por ejemplo
# MAGIC `@dlt.expect("monto_razonable", "monto < 1000000")`. Y razona: ¿debería ser `expect`
# MAGIC (avisar) o `expect_or_drop` (descartar)? *Pista: una transacción de más de un millón
# MAGIC puede ser legítima — ¿la quieres perder?*

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 8 · Verifica tu checkpoint
# MAGIC
# MAGIC Ejecuta esta celda (ajusta el catálogo/schema si hace falta — se detectan solos).

# COMMAND ----------

spark.sql(f"USE `{_cat}`.`{_schema}`")
resultados = []

try:
    fg = spark.table("gold_riesgo_diario").count()
    resultados.append((fg > 0, f"gold_riesgo_diario existe y tiene {fg:,} filas"))
except Exception:
    resultados.append((False, "No encuentro gold_riesgo_diario — ¿corrió el pipeline?"))

try:
    cols = spark.table("silver_transacciones").schema["fecha"].dataType.simpleString()
    resultados.append((cols == "timestamp", f"silver.fecha quedó tipada como {cols} (se esperaba timestamp)"))
except Exception:
    resultados.append((False, "No pude leer silver_transacciones"))

try:
    fr = spark.sql("SELECT SUM(transacciones_fraude) f FROM gold_riesgo_diario").collect()[0][0]
    resultados.append((fr and fr > 0, f"gold registra {fr:,} transacciones de fraude"))
except Exception:
    resultados.append((False, "No pude leer las métricas de gold"))

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
    · Definiste 3 reglas de calidad y viste cuántas filas afectaron
    · Rompiste algo a propósito y viste subir los descartes

  👉 En el Módulo 4 vas a gobernar estas tablas: quién ve qué filas,
     qué columnas se enmascaran, y de dónde viene cada dato.
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
# MAGIC | **El DAG se infiere** de las dependencias | 4 nodos ordenados solos |
# MAGIC | **Silver limpia, gold agrega** | `fecha` por fin es timestamp; gold responde negocio |
# MAGIC | **La calidad es código** | 3 expectativas, ejecutadas en cada corrida |
# MAGIC | **El event log** registra todo | Linaje, calidad, rendimiento por ejecución |
# MAGIC
# MAGIC ## ⚠️ Recordatorio
# MAGIC En producción, este pipeline se despliega con **Asset Bundles** (YAML versionado), se
# MAGIC promueve dev → staging → prod, y corre dentro de un job programado (Módulo 5). Los
# MAGIC conceptos son idénticos.
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC Tienes datos **confiables y útiles**, pero **cualquiera con acceso los ve completos**.
# MAGIC En el **Módulo 4** aplicas gobierno: filtras filas por región y enmascaras el número de
# MAGIC tarjeta — y compruebas el efecto en tus propios datos.
