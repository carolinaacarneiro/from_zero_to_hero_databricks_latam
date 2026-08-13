# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🔗 Módulo 3 · Guía del pipeline
# MAGIC
# MAGIC Este notebook te guía para crear y correr el pipeline. El código del
# MAGIC pipeline vive en el archivo **`pipeline_fraude.py`** (al lado de este notebook).
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - **El código del pipeline ya está completo** (`pipeline_fraude.py`). No editas nada.
# MAGIC - El foco de hoy es **entender**: leer el código, entender las **expectativas de
# MAGIC   calidad**, crear el pipeline en la UI, correrlo y leer el DAG y el panel de calidad.
# MAGIC - Este notebook es la **guía**: lees acá y sigues los pasos en la UI del pipeline.
# MAGIC - 🙋 Trabado más de 3 minutos: levanta la mano.
# MAGIC
# MAGIC ## De dónde venimos
# MAGIC Tienes la capa **bronze** del Módulo 2. Hoy ves cómo un pipeline declarativo construye
# MAGIC **silver** (limpia) y **gold** (agregada) — sin que nadie escriba el orden en que corren.
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
# MAGIC # Paso 1 · Lee el código del pipeline (ya está completo)
# MAGIC
# MAGIC Abre **`pipeline_fraude.py`** (en la misma carpeta) y léelo de arriba a abajo. **No hay
# MAGIC nada que editar** — el objetivo es *entender* cómo se declara un pipeline. Cuatro cosas:
# MAGIC
# MAGIC 1. **Las dos bronze `bronze_pl_*`** — el pipeline las crea desde los archivos crudos con
# MAGIC    Auto Loader (`cloudFiles`). Se llaman `bronze_pl_*` para no chocar con la bronze que
# MAGIC    hiciste a mano en el M2.
# MAGIC 2. **`silver_transacciones`** — limpia: deduplica, une con clientes y **tipa `fecha` de
# MAGIC    texto a timestamp** (el pendiente del M1/M2). Lleva 3 expectativas (Paso 2).
# MAGIC 3. **`gold_riesgo_diario`** — agrega por día × región × categoría; responde la pregunta
# MAGIC    del negocio. Es una *materialized view*.
# MAGIC 4. **No hay ningún orden escrito.** Cada `@dlt.table` dice qué produce y de quién lee
# MAGIC    (`dlt.read` / `dlt.read_stream`). La plataforma **deduce el DAG** de eso.
# MAGIC
# MAGIC | Símbolo en el código | Qué significa |
# MAGIC |---|---|
# MAGIC | `@dlt.table` | declara una tabla del pipeline |
# MAGIC | `dlt.read("x")` | lee la tabla `x` **completa** (batch) — crea dependencia |
# MAGIC | `dlt.read_stream("x")` | lee `x` de forma **incremental** (streaming) |
# MAGIC | `@dlt.expect...` | una regla de calidad (expectativa) |
# MAGIC
# MAGIC > 💡 Lee con calma silver y gold: son pocas líneas y en ellas está toda la medallion.
# MAGIC > Lo que más vale entender son las **expectativas** — el siguiente paso es solo sobre eso.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · El corazón del módulo: las expectativas (*expectations*)
# MAGIC
# MAGIC La parte más importante que hay que entender de silver: las **expectativas de calidad**.
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
# MAGIC ## Las 3 expectativas de silver en este pipeline
# MAGIC
# MAGIC Búscalas encima de `silver_transacciones` en `pipeline_fraude.py`:
# MAGIC
# MAGIC | Expectativa | Acción | Por qué |
# MAGIC |---|---|---|
# MAGIC | `id_valido` | `expect_or_drop` | Una fila sin id no sirve para nada |
# MAGIC | `monto_positivo` | `expect_or_drop` | Descarta el ~2% inválido que plantamos en los datos |
# MAGIC | `moneda_conocida` | `expect` (solo avisa) | Queremos vigilarlo, no descartar la fila |
# MAGIC
# MAGIC > 💡 **La decisión de diseño:** ¿por qué `monto_positivo` descarta pero `moneda_conocida`
# MAGIC > solo avisa? Un monto ≤ 0 es basura que ensucia los agregados de gold; una moneda rara
# MAGIC > es algo que quieres **saber** sin perder la fila. Elegir la acción correcta para cada
# MAGIC > regla es lo que separa una regla útil de una molesta — y es el corazón del módulo.
# MAGIC >
# MAGIC > 📊 En el Paso 5 verás, en el panel de calidad, **cuántas filas** afectó cada una.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · Lee silver y gold en detalle
# MAGIC
# MAGIC Recorre en `pipeline_fraude.py` el cuerpo de las dos tablas. No hay nada que escribir —
# MAGIC solo entender qué hace cada línea.
# MAGIC
# MAGIC ## `silver_transacciones` — el dato se vuelve confiable
# MAGIC
# MAGIC | Línea | Qué hace |
# MAGIC |---|---|
# MAGIC | `dlt.read_stream("bronze_pl_transacciones")` | lee la bronze de forma incremental |
# MAGIC | `dlt.read("bronze_pl_clientes").drop("region", …)` | dimensión de clientes (se quita `region` para que el join no quede ambiguo) |
# MAGIC | `.dropDuplicates(["transaccion_id"])` | una transacción, una fila |
# MAGIC | `.join(clientes, "cliente_id", "left")` | pega segmento, score y antigüedad a cada transacción |
# MAGIC | `.withColumn("fecha", F.to_timestamp("fecha"))` | **el pendiente del M1/M2**: `fecha` pasa de texto a `timestamp` |
# MAGIC | `.select(...)` | se queda con las columnas útiles |
# MAGIC
# MAGIC ## `gold_riesgo_diario` — el dato se vuelve útil
# MAGIC
# MAGIC Es una *materialized view*: agrega silver por **día × región × categoría**. Cada métrica
# MAGIC responde algo del negocio:
# MAGIC
# MAGIC | Columna | Cálculo | Responde |
# MAGIC |---|---|---|
# MAGIC | `total_transacciones` | `count(*)` | volumen |
# MAGIC | `monto_total` | `sum(monto)` | cuánto se movió |
# MAGIC | `transacciones_fraude` | `sum(es_fraude)` | cuántos fraudes |
# MAGIC | `monto_fraude` | `sum(monto)` en fraudes | exposición en dinero |
# MAGIC | `tasa_fraude` | fraudes / total | qué tan grave |
# MAGIC | `ticket_promedio` | monto_total / total | tamaño típico |
# MAGIC
# MAGIC > 💡 Fíjate que **gold lee de silver** (`dlt.read("silver_transacciones")`) y **silver lee
# MAGIC > de las dos bronze**. Esas lecturas son las que arman el DAG — que verás en el Paso 5.

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
# MAGIC 3. Abre `pipeline_fraude.py`, copia **todo** su contenido y pégalo ahí
# MAGIC 4. *(opcional)* renombra el archivo a `pipeline_fraude.py` con el menú de los tres puntos
# MAGIC
# MAGIC > 💡 Las líneas `# Databricks notebook source` y `# MAGIC %md` quedan como comentarios
# MAGIC > inofensivos. No hace falta quitarlas.
# MAGIC
# MAGIC ## 4.3 · Configura el destino (con TUS valores) — DOS lugares
# MAGIC El pipeline **no adivina** dónde trabajas. Con tus valores (los mismos del M1/M2),
# MAGIC configura **dos cosas** en la UI del pipeline:
# MAGIC
# MAGIC 1. **Default location for data assets** (botón *Edit catalog and schema*) — es **dónde
# MAGIC    se publican** tus tablas:
# MAGIC    - **Default catalog**: tu catálogo · **Default schema**: tu schema.
# MAGIC 2. **Settings → Configuration** (Advanced) — agrega **un** parámetro para que el pipeline
# MAGIC    encuentre tu volumen `raw`:
# MAGIC    - clave `z2h.schema`  ·  valor: **tu schema** (el mismo de arriba).
# MAGIC
# MAGIC > ⚠️ **¿Por qué el paso 2?** Dentro del pipeline, el **Default catalog sí** es visible desde
# MAGIC > el código, pero el **Default schema NO**. Por eso el schema se declara además en
# MAGIC > Configuration (`z2h.schema`). Es un campo, en el mismo panel de Settings.
# MAGIC
# MAGIC ## 4.4 · Ejecuta
# MAGIC Presiona **Run pipeline** (arriba a la derecha). Tarda 1–2 minutos en arrancar.

# COMMAND ----------

# ═══ Recordatorio de qué poner en la UI del pipeline (los mismos del M1/M2) ══
# Campos VACÍOS: tú declaras dónde trabajas. No se infiere nada.
dbutils.widgets.text("catalogo", "", "1 · Catálogo")
dbutils.widgets.text("schema",   "", "2 · Schema")

catalogo = dbutils.widgets.get("catalogo").strip()
schema = dbutils.widgets.get("schema").strip()

if not catalogo or not schema:
    raise Exception(
        "\n❌ Escribe tu CATÁLOGO y tu SCHEMA en los dos campos de arriba (los mismos que "
        "usaste en el M1/M2) y vuelve a correr esta celda.\n"
    )

print("En la UI del pipeline, configura:\n")
print("  1) Default location for data assets (Edit catalog and schema):")
print(f"       Default catalog : {catalogo}")
print(f"       Default schema  : {schema}")
print("  2) Settings → Configuration (Advanced), agrega este parámetro:")
print(f"       z2h.schema = {schema}")
print("\nCon eso el pipeline publica en tu schema y encuentra tu volumen")
print("raw. Presiona 'Run pipeline'.")

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

# Reusa el catálogo/schema que declaraste en el Paso 4. Si corriste esta celda suelta,
# vuelve a correr la celda de declaración (Paso 4.3) primero.
try:
    catalogo, schema
except NameError:
    raise Exception(
        "\n❌ Primero declara tu catálogo y schema en la celda del Paso 4.3 "
        "(y luego vuelve aquí).\n"
    )
RAW = f"/Volumes/{catalogo}/{schema}/raw"

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
# MAGIC Ejecuta esta celda. Usa el catálogo y schema que declaraste en el Paso 4.

# COMMAND ----------

try:
    catalogo, schema
except NameError:
    raise Exception(
        "\n❌ Primero declara tu catálogo y schema en la celda del Paso 4 (y vuelve aquí).\n"
    )
spark.sql(f"USE `{catalogo}`.`{schema}`")
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
    · Entendiste un pipeline declarativo — la plataforma resolvió el orden (el DAG)
    · Entendiste las 3 expectativas de calidad y viste cuántas filas afectaron
    · Rompiste algo a propósito y viste subir los descartes

  👉 En el Módulo 4 vas a gobernar estas tablas: quién ve qué filas,
     qué columnas se enmascaran, y de dónde viene cada dato.
""")
else:
    print("  ⚠️  Revisa los ❌. Lo más común: el pipeline no terminó de correr, o el destino")
    print("      (Default catalog/schema) no es el mismo donde generaste los datos.")

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
