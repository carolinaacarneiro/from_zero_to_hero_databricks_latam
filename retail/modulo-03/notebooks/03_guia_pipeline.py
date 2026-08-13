# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🔗 Módulo 3 · Guía del pipeline
# MAGIC
# MAGIC Este notebook te guía para crear y correr el pipeline. El código del
# MAGIC pipeline vive en el archivo **`pipeline_retail.py`** (al lado de este notebook).
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - **El código del pipeline ya está completo** (`pipeline_retail.py`). No editas nada.
# MAGIC - El foco de hoy es **entender**: leer el código, entender las **expectativas de
# MAGIC   calidad**, crear el pipeline en la UI, correrlo y leer el DAG y el panel de calidad.
# MAGIC - Este notebook es la **guía**: lees acá y sigues los pasos en la UI del pipeline.
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
# MAGIC # Paso 1 · Lee el código del pipeline (ya está completo)
# MAGIC
# MAGIC Abre **`pipeline_retail.py`** (en la misma carpeta) y léelo de arriba a abajo. **No hay
# MAGIC nada que editar** — el objetivo es *entender* cómo se declara un pipeline. Cuatro cosas:
# MAGIC
# MAGIC 1. **Las tres bronze `bronze_pl_*`** — el pipeline las crea desde los archivos crudos con
# MAGIC    Auto Loader (`cloudFiles`). Se llaman `bronze_pl_*` para no chocar con la bronze que
# MAGIC    hiciste a mano en el M2.
# MAGIC 2. **`silver_ventas`** — limpia: deduplica, une con productos y **tipa `fecha` de texto
# MAGIC    a timestamp** (el pendiente del M1/M2). Lleva 3 expectativas (Paso 2).
# MAGIC 3. **Tres gold** — `gold_ventas_diarias` (la serie para pronosticar), `gold_ventas_producto`
# MAGIC    (ranking de productos) y `gold_inventario_estado` (qué reabastecer).
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
# MAGIC Una **expectativa es una regla que cada fila debe cumplir.** La declaras junto a la tabla,
# MAGIC y el pipeline la revisa **en cada ejecución**, fila por fila. Es la calidad de datos
# MAGIC escrita como código — no un chequeo manual que alguien corre el viernes.
# MAGIC
# MAGIC Una expectativa tiene tres partes:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.expect_or_drop( "cantidad_positiva",   "cantidad > 0" )
# MAGIC #    │                 │                    │
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
# MAGIC @dlt.table(name="silver_ventas")
# MAGIC @dlt.expect_or_drop("id_valido", "venta_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("cantidad_positiva", "cantidad > 0")
# MAGIC @dlt.expect("monto_positivo", "monto > 0")
# MAGIC def silver_ventas():
# MAGIC     ...
# MAGIC ```
# MAGIC
# MAGIC La **condición** es SQL que debe dar verdadero para una fila buena. `cantidad > 0` significa
# MAGIC *«espero que la cantidad sea positiva»*; la fila que no lo cumpla recibe la acción del
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
# MAGIC Búscalas encima de `silver_ventas` en `pipeline_retail.py`:
# MAGIC
# MAGIC | Expectativa | Acción | Por qué |
# MAGIC |---|---|---|
# MAGIC | `id_valido` | `expect_or_drop` | Una venta sin id no sirve para nada |
# MAGIC | `cantidad_positiva` | `expect_or_drop` | Descarta el ~2% inválido que plantamos en los datos |
# MAGIC | `monto_positivo` | `expect` (solo avisa) | Queremos vigilarlo, no descartar la fila |
# MAGIC
# MAGIC > 💡 **La decisión de diseño:** ¿por qué `cantidad_positiva` descarta pero `monto_positivo`
# MAGIC > solo avisa? Una cantidad ≤ 0 es basura que ensucia los agregados de gold; un monto raro
# MAGIC > es algo que quieres **saber** sin perder la fila. Elegir la acción correcta para cada
# MAGIC > regla es lo que separa una regla útil de una molesta — y es el corazón del módulo.
# MAGIC >
# MAGIC > 📊 En el Paso 6 verás, en el panel de calidad, **cuántas filas** afectó cada una.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · Lee silver y gold en detalle
# MAGIC
# MAGIC Recorre en `pipeline_retail.py` el cuerpo de las tablas. No hay nada que escribir —
# MAGIC solo entender qué hace cada línea.
# MAGIC
# MAGIC ## `silver_ventas` — el dato se vuelve confiable
# MAGIC
# MAGIC | Línea | Qué hace |
# MAGIC |---|---|
# MAGIC | `dlt.read_stream("bronze_pl_ventas")` | lee la bronze de forma incremental |
# MAGIC | `dlt.read("bronze_pl_productos").drop("_datos_rescatados")` | catálogo de productos (se quita la columna de rescatados) |
# MAGIC | `.dropDuplicates(["venta_id"])` | una venta, una fila |
# MAGIC | `.join(productos, "producto_id", "left")` | pega categoria, marca, costo a cada venta |
# MAGIC | `.withColumn("fecha", F.to_timestamp("fecha"))` | **el pendiente del M1/M2**: `fecha` pasa de texto a `timestamp` |
# MAGIC | `.select(...)` | se queda con las columnas útiles |
# MAGIC
# MAGIC ## `gold_ventas_diarias` — el dato se vuelve útil
# MAGIC
# MAGIC Es una *materialized view*: agrega silver por **día × país × categoría**. Cada métrica
# MAGIC responde algo del negocio (y es lo que el Módulo 6 le dará a ai_forecast):
# MAGIC
# MAGIC | Columna | Cálculo | Responde |
# MAGIC |---|---|---|
# MAGIC | `unidades` | `sum(cantidad)` | volumen de ventas |
# MAGIC | `monto` | `sum(monto)` | ingresos totales |
# MAGIC | `utilidad` | `sum(utilidad)` | ganancia neta |
# MAGIC | `lineas` | `count(*)` | número de transacciones |
# MAGIC
# MAGIC ## Las otras dos gold
# MAGIC
# MAGIC **`gold_ventas_producto`**: agrupa sin tiempo — qué producto se vende más, cuál tiene
# MAGIC mejor margen, en cuántos países se vende. Alimenta el ranking del dashboard.
# MAGIC
# MAGIC **`gold_inventario_estado`**: cruza el stock actual con la demanda histórica para
# MAGIC calcular **días de cobertura** y decidir qué reabastecer. Responde: ¿este producto
# MAGIC durará 30 días? ¿Lo necesito reponer ahora?
# MAGIC
# MAGIC > 💡 Fíjate que **cada gold lee de silver** (`dlt.read("silver_ventas")`) y **silver lee
# MAGIC > de las bronze**. Esas lecturas son las que arman el DAG — que verás en el Paso 5.

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
# MAGIC 3. Abre `pipeline_retail.py`, copia **todo** su contenido y pégalo ahí
# MAGIC 4. *(opcional)* renombra el archivo a `pipeline_retail.py` con el menú de los tres puntos
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

# ═══ CELDA A · crea los campos ════════════════════════════════════════════
# Corre esta celda. Aparecen dos campos ARRIBA. Escribe en ellos tu catálogo y tu schema
# (los mismos del M1/M2) y luego corre la CELDA B de abajo.
dbutils.widgets.text("catalogo", "", "1 · Catálogo")
dbutils.widgets.text("schema",   "", "2 · Schema")
print("👆 Escribe tu CATÁLOGO y tu SCHEMA en los dos campos de arriba, y corre la CELDA B.")

# COMMAND ----------

# ═══ CELDA B · lee lo que escribiste y te recuerda qué poner en la UI ═════
# Esta celda SOLO imprime un recordatorio (no configura nada). Si dejas los campos vacíos,
# muestra un marcador de posición — no bloquea.
catalogo = dbutils.widgets.get("catalogo").strip() or "<tu_catálogo>"
schema = dbutils.widgets.get("schema").strip() or "<tu_schema>"

print("👉 Esto es un RECORDATORIO — configúralo en la UI del pipeline, no aquí.\n")
print("En la UI del pipeline, configura:\n")
print("  1) Default location for data assets (Edit catalog and schema):")
print(f"       Default catalog : {catalogo}")
print(f"       Default schema  : {schema}")
print("  2) Settings → Configuration (Advanced), agrega este parámetro:")
print(f"       z2h.schema = {schema}")
print("\nCon eso el pipeline publica en tu schema y encuentra tu volumen raw.")
print("Presiona 'Run pipeline'.")

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
    · Entendiste un pipeline declarativo — la plataforma resolvió el orden (el DAG)
    · Entendiste las 3 expectativas de calidad y viste cuántas filas afectaron
    · Rompiste algo a propósito y viste subir los descartes

  👉 En el Módulo 4 vas a gobernar estas tablas: quién ve qué filas (por país),
     qué columnas se enmascaran (el costo), y de dónde viene cada dato.
""")
else:
    print("  ⚠️  Revisa los ❌. Lo más común: el pipeline no terminó, o el destino")
    print("      (Default catalog/schema) no es el mismo donde generaste los datos.")

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
