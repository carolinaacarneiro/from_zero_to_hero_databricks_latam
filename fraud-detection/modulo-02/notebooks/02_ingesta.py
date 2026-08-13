# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🥉 Módulo 2 · Tu capa bronze con Auto Loader
# MAGIC
# MAGIC Ahora construyes la capa bronze de verdad.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Cómo trabajar en este notebook
# MAGIC
# MAGIC - **Ejecuta celda por celda** con `Shift + Enter`. No uses "Run all".
# MAGIC - Donde dice **📝 TU TURNO**, te toca completar algo. Debajo hay una solución.
# MAGIC - 🙋 **¿Trabado más de 3 minutos?** Levanta la mano. Estamos acá para eso.
# MAGIC
# MAGIC ## De dónde venimos y a dónde vamos
# MAGIC
# MAGIC En el Módulo 1 creaste una tabla de **práctica** con 1.000 filas para aprender Delta
# MAGIC Lake. Hoy construyes **la capa bronze real**: las 375.000 transacciones completas,
# MAGIC con un proceso que puedes volver a correr sin miedo.
# MAGIC
# MAGIC ```
# MAGIC   archivos crudos  →  🥉 BRONZE  →  🥈 silver  →  🥇 gold
# MAGIC      (ya están)        ↑ HOY        (Módulo 3)   (Módulo 3)
# MAGIC ```
# MAGIC
# MAGIC ## ⚠️ Esto es material de aprendizaje
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | 🎓 **Objetivo** | Enseñar los conceptos de ingesta. Optimizado para entenderse, no para producción |
# MAGIC | 🔒 **Datos 100% sintéticos** | Ninguna persona ni institución real |
# MAGIC | 🚫 **No lo copies a producción tal cual** | Ahí Auto Loader corre dentro de un pipeline o un job, con monitoreo y reintentos (Módulos 3 y 5) |
# MAGIC | ✅ **Los conceptos sí son reales** | Auto Loader, checkpoints y la capa bronze son lo que usan las empresas de verdad |
# MAGIC
# MAGIC > 💡 **El caso es un banco, pero el método es de cualquier industria.** Ingerir archivos
# MAGIC > que llegan solos —ventas de retail, sensores de una fábrica, eventos de una app— es el
# MAGIC > mismo patrón. Cambia el dato, no la técnica.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 · Declara tu catálogo y tu schema
# MAGIC
# MAGIC **Corre la celda de abajo una vez** para que aparezcan dos campos arriba del notebook.
# MAGIC Luego **escribe en ellos el catálogo y el schema donde estás trabajando** — los
# MAGIC **mismos** que declaraste en el Módulo 1 (`00_generar_datos`) y que vienes usando en
# MAGIC todos los módulos.
# MAGIC
# MAGIC > ⚠️ **No adivinamos nada por ti.** Tú declaras explícitamente dónde trabajas, para que
# MAGIC > no haya duda de en qué catálogo y schema quedan tus datos. Si los dejas vacíos, el
# MAGIC > notebook se detiene y te lo pide.

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

# 3 · el schema tiene que existir (lo creaste en el M1)
schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{catalogo}`").collect()]
if schema not in schemas:
    raise Exception(
        f"\n❌ El schema '{catalogo}.{schema}' no existe.\n"
        f"   ¿Corriste 00_generar_datos con estos mismos valores? Córrelo primero, o "
        f"corrige el nombre arriba.\n"
    )

spark.sql(f"USE `{catalogo}`.`{schema}`")
RAW = f"/Volumes/{catalogo}/{schema}/raw"

# comprobar que los archivos crudos están ahí
try:
    n = len([f for f in dbutils.fs.ls(f"{RAW}/transacciones") if f.name.endswith(".json")])
    assert n > 0
except Exception:
    raise Exception(
        f"\n❌ No encuentro archivos en {RAW}/transacciones\n"
        f"   Corre primero 00_generar_datos con estos mismos valores.\n"
    )

print(f"👤 Usuario   : {usuario}")
print(f"📁 Trabajarás en : {catalogo}.{schema}")
print(f"🗂️  Archivos  : {RAW}  ({n} archivos de transacciones)")
print()
print("✅ Espacio declarado y verificado. Nadie más toca este schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · Mira el origen antes de ingerir
# MAGIC
# MAGIC ## Por qué importa
# MAGIC El primer hábito de cualquier ingesta: **mira los archivos antes de construir sobre
# MAGIC ellos**. Cuántos son, qué traen, qué tamaño tienen.
# MAGIC
# MAGIC Estos archivos JSON llegaron a un **volumen** de Unity Catalog — igual que llegan en la
# MAGIC vida real desde un sistema de pagos que deja un archivo cada cierto tiempo.

# COMMAND ----------

archivos = [f for f in dbutils.fs.ls(f"{RAW}/transacciones") if f.name.endswith(".json")]
print(f"📁 {len(archivos)} archivos en el origen\n")
for f in archivos[:3]:
    print(f"   {f.name[:38]}…  ({f.size / 1024 / 1024:.1f} MB)")
print(f"   … y {len(archivos) - 3} más")

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora **el contenido**. Fíjate: es el dato crudo, sin tocar. Más adelante verás que
# MAGIC `fecha` llega como texto — es esperado, y lo dejamos así en bronze.

# COMMAND ----------

display(spark.read.json(f"{RAW}/transacciones").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔎 Antes de empezar: ¿qué es Auto Loader?
# MAGIC
# MAGIC En un minuto, para que sepas qué vas a usar en el Paso 3.
# MAGIC
# MAGIC ## Qué es
# MAGIC **Auto Loader** es la forma recomendada de Databricks para **ingerir archivos que llegan
# MAGIC de a poco** a un almacenamiento (un volumen, S3, ADLS, GCS). En vez de que tú lleves la
# MAGIC cuenta de qué archivos ya procesaste, Auto Loader **lo recuerda solo** y procesa **únicamente
# MAGIC los nuevos** cada vez que corre.
# MAGIC
# MAGIC Se usa como una lectura de streaming, con `.format("cloudFiles")`.
# MAGIC
# MAGIC ## Por qué importa — el diferencial
# MAGIC
# MAGIC | Sin Auto Loader | Con Auto Loader |
# MAGIC |---|---|
# MAGIC | Tú decides qué archivos son nuevos (por fecha, por nombre…) — frágil y propenso a errores | Lleva la cuenta solo, con un **checkpoint** |
# MAGIC | Reprocesar por error **duplica** los datos | Correrlo de nuevo **no duplica**: ya sabe qué leyó |
# MAGIC | Si el origen cambia una columna, se rompe en silencio | **Infiere y evoluciona el esquema**, y rescata lo que no encaja |
# MAGIC | Listar millones de archivos es lento y caro | Escala a **millones de archivos** de forma eficiente (puede usar notificaciones de la nube) |
# MAGIC
# MAGIC ## Cómo se usa (las piezas clave)
# MAGIC - `cloudFiles.format` → el formato de origen (`json`, `csv`, `parquet`…)
# MAGIC - `cloudFiles.schemaLocation` → dónde recuerda el esquema que infirió
# MAGIC - `checkpointLocation` → **la memoria del proceso**: qué archivos ya leyó
# MAGIC - `rescuedDataColumn` → dónde guarda lo que no encajó (no se pierde)
# MAGIC
# MAGIC ## ¿Cómo descubre los archivos nuevos? — dos modos
# MAGIC
# MAGIC | Modo | Cómo funciona | Cuándo |
# MAGIC |---|---|---|
# MAGIC | **Directory listing** *(por defecto)* | Lista el directorio para ver qué llegó. Sin configurar nada extra | Empezar rápido, cualquier nube, volúmenes chicos/medianos |
# MAGIC | **File events / notificaciones** | Usa eventos de la nube (colas de notificación): no re-lista todo cada vez | Producción, **millones de archivos**, menor costo y más escala |
# MAGIC
# MAGIC > 💡 Hoy usamos **directory listing** (lo más simple). En producción, Databricks recomienda
# MAGIC > **file events** para escalar. En cualquiera de los dos, Auto Loader **no garantiza el orden**
# MAGIC > en que descubre los archivos — por eso la trazabilidad (`_archivo_origen`) importa.
# MAGIC
# MAGIC ## Esquema: lo infiere, y sabe qué hacer cuando cambia
# MAGIC Auto Loader **infiere el esquema** (muestrea los primeros archivos) y lo recuerda en el
# MAGIC `schemaLocation`. Lo interesante es qué pasa **cuando el origen cambia** — lo controla
# MAGIC `cloudFiles.schemaEvolutionMode`:
# MAGIC
# MAGIC | Modo | Si aparece una columna nueva… |
# MAGIC |---|---|
# MAGIC | **addNewColumns** *(por defecto)* | El stream **se detiene a propósito** (`UnknownFieldException`), actualiza el esquema, y al reiniciar ya la incluye |
# MAGIC | **rescue** | Nunca cambia el esquema; lo nuevo va a `_rescued_data` |
# MAGIC | **failOnNewColumns** | Falla y espera que actualices el esquema a mano |
# MAGIC | **none** | Ignora las columnas nuevas |
# MAGIC
# MAGIC > 👀 **Que el stream se detenga NO es un error — es diseño.** Te obliga a enterarte de que el
# MAGIC > origen cambió, en vez de tragarte datos en silencio. Por eso en producción Auto Loader corre
# MAGIC > dentro de un **job o pipeline que lo reinicia solo** (Módulos 3 y 5): se detiene, se reinicia,
# MAGIC > y sigue con el esquema nuevo.
# MAGIC >
# MAGIC > 🔢 **Type widening** (DBR 16.4+): con `addNewColumnsWithTypeWidening`, si una columna cambia
# MAGIC > de tipo *compatible* (ej. `int → long`), amplía el tipo sin reescribir los datos. Los cambios
# MAGIC > incompatibles van a `_rescued_data`.
# MAGIC
# MAGIC ## Auto Loader + Unity Catalog
# MAGIC - Lee desde un **volumen** (o external location) gobernado por UC — como el `raw` que creaste.
# MAGIC - El **checkpoint y el schemaLocation NO pueden ir dentro del directorio de la tabla**: por eso
# MAGIC   los ponemos en una ruta aparte (`_checkpoints/…`).
# MAGIC - Los permisos son de UC: `READ FILES` en el origen, `WRITE FILES` + `CREATE TABLE` en el destino.
# MAGIC
# MAGIC ## Cuándo usarlo
# MAGIC
# MAGIC | Úsalo cuando… | Quizás no lo necesites si… |
# MAGIC |---|---|
# MAGIC | Los archivos **llegan continuamente** (cada hora, cada día) | Cargas **una sola vez** un archivo fijo que no cambia |
# MAGIC | Quieres una ingesta **incremental y repetible** | Un `CREATE TABLE ... SELECT` puntual te alcanza (como el M1) |
# MAGIC | El volumen de archivos es **grande o crece** | — |
# MAGIC
# MAGIC ## 🏭 Buenas prácticas (para cuando lo lleves a producción)
# MAGIC - **Un checkpoint por stream/origen** — nunca los compartas ni les apliques políticas que los borren.
# MAGIC - Conserva `_rescued_data` y vigílalo: es **cómo te enteras de que el origen cambió**.
# MAGIC - Anota el origen con `_metadata.file_path` (lo hacemos en el Paso 3) para trazabilidad.
# MAGIC - En producción, córrelo dentro de un **Lakeflow pipeline o job** (autoscaling, reinicio, calidad).
# MAGIC - A escala, usa **file events** y `cloudFiles.cleanSource` para archivar lo ya procesado.
# MAGIC
# MAGIC > 💡 **En resumen:** Auto Loader convierte *«una carpeta donde caen archivos»* en *«una tabla
# MAGIC > que se mantiene al día sola, sin duplicar»*. Eso es exactamente lo que necesita una capa
# MAGIC > **bronze**. Ahora lo vas a construir.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · La ingesta con Auto Loader
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Escribir el proceso que lee los archivos y los carga a `bronze_transacciones`, con
# MAGIC **trazabilidad** y **checkpoint**.
# MAGIC
# MAGIC ## Las piezas de una ingesta con Auto Loader
# MAGIC
# MAGIC | Opción | Qué hace |
# MAGIC |---|---|
# MAGIC | `.format("cloudFiles")` | Activa Auto Loader |
# MAGIC | `cloudFiles.format` | El formato de los archivos de origen |
# MAGIC | `cloudFiles.schemaLocation` | Dónde recuerda el esquema que infirió |
# MAGIC | `rescuedDataColumn` | Dónde guarda lo que no encaja *(no se pierde)* |
# MAGIC | `checkpointLocation` | **La memoria del proceso**: qué archivos ya leyó |
# MAGIC | `.trigger(availableNow=True)` | Procesa lo que haya y termina *(modo batch)* |
# MAGIC
# MAGIC Y agregamos las dos columnas de trazabilidad de la capa bronze:
# MAGIC `_ingerido_en` y `_archivo_origen`.
# MAGIC
# MAGIC ### 📝 TU TURNO
# MAGIC
# MAGIC Completa las **dos líneas** marcadas con `TODO`: el **formato** de los archivos (lo
# MAGIC viste en el paso 2) y el **nombre de la tabla** destino (`bronze_transacciones`).

# COMMAND ----------

# el checkpoint y el esquema viven FUERA del directorio de la tabla (regla de Unity Catalog)
ruta_checkpoint = f"{RAW}/_checkpoints/bronze_transacciones"

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "FORMATO")                     # 📝 TODO: ¿json? ¿csv?
    .option("cloudFiles.schemaLocation", ruta_checkpoint)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_datos_rescatados")
    .load(f"{RAW}/transacciones")
    # las dos columnas de trazabilidad de bronze
    .selectExpr(
        "*",
        "current_timestamp() AS _ingerido_en",
        "_metadata.file_path AS _archivo_origen",
    )
    .writeStream
    .option("checkpointLocation", ruta_checkpoint)
    .trigger(availableNow=True)
    .toTable("TABLA_DESTINO")                                   # 📝 TODO: bronze_transacciones
    .awaitTermination())

print("✅ Ingesta terminada")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary>💡 <b>Solución del paso 3</b> — ábrela si te trabaste</summary>
# MAGIC
# MAGIC <pre>.option("cloudFiles.format", "json")
# MAGIC ...
# MAGIC .toTable("bronze_transacciones")</pre>
# MAGIC
# MAGIC Si prefieres no escribirlo, pon `USAR_SOLUCION = True` en la celda de abajo y ejecútala.
# MAGIC </details>

# COMMAND ----------

# --- Solución del paso 3 ---
# Si te trabaste, cambia USAR_SOLUCION a True y ejecuta esta celda.
USAR_SOLUCION = False

if USAR_SOLUCION:
    ruta_checkpoint = f"{RAW}/_checkpoints/bronze_transacciones"
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", ruta_checkpoint)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/transacciones")
        .selectExpr("*", "current_timestamp() AS _ingerido_en",
                    "_metadata.file_path AS _archivo_origen")
        .writeStream
        .option("checkpointLocation", ruta_checkpoint)
        .trigger(availableNow=True)
        .toTable("bronze_transacciones")
        .awaitTermination())
    print("✅ Ingesta terminada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuenta las filas — y anota el número
# MAGIC
# MAGIC Lo vas a comparar en el paso 4. Deberían ser **~375.000**.

# COMMAND ----------

filas_primera_carga = spark.table("bronze_transacciones").count()
print(f"📊 bronze_transacciones tiene {filas_primera_carga:,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mira tu capa bronze
# MAGIC
# MAGIC Fíjate en las **dos últimas columnas**: `_ingerido_en` y `_archivo_origen`. El origen
# MAGIC no las trae — las agregamos nosotros. También aparece `_datos_rescatados`.

# COMMAND ----------

display(spark.table("bronze_transacciones").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · La incrementalidad ⭐
# MAGIC ## El momento clave del módulo
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC **Correr exactamente la misma ingesta otra vez.**
# MAGIC
# MAGIC Piensa antes de ejecutar: no llegaron archivos nuevos. ¿Qué debería pasar con el
# MAGIC conteo?
# MAGIC
# MAGIC En un proceso ingenuo —leer todo cada vez— el conteo se **duplicaría** a 750.000. Con
# MAGIC el checkpoint, Auto Loader ya sabe qué archivos leyó, así que **no debería procesar
# MAGIC nada**.

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", ruta_checkpoint)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_datos_rescatados")
    .load(f"{RAW}/transacciones")
    .selectExpr("*", "current_timestamp() AS _ingerido_en",
                "_metadata.file_path AS _archivo_origen")
    .writeStream
    .option("checkpointLocation", ruta_checkpoint)
    .trigger(availableNow=True)
    .toTable("bronze_transacciones")
    .awaitTermination())

filas_segunda_carga = spark.table("bronze_transacciones").count()
print(f"📊 Primera carga : {filas_primera_carga:,} filas")
print(f"📊 Segunda carga : {filas_segunda_carga:,} filas")
print()
if filas_segunda_carga == filas_primera_carga:
    print("✅ El conteo NO cambió. El checkpoint hizo su trabajo: no reprocesó nada.")
    print("   Ese es el corazón de una ingesta incremental.")
else:
    print(f"⚠️  El conteo cambió en {filas_segunda_carga - filas_primera_carga:,}. "
          "Revisa que el checkpointLocation sea el mismo de la primera carga.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 El mismo código, corrido dos veces, mismo resultado
# MAGIC
# MAGIC No configuraste nada especial. El checkpoint —una sola línea— es lo que hace que
# MAGIC ingerir sea **repetible sin duplicar**.
# MAGIC
# MAGIC ### Por qué esto importa en la vida real
# MAGIC
# MAGIC | Situación | Con checkpoint |
# MAGIC |---|---|
# MAGIC | El proceso corre cada hora | Solo procesa lo que llegó en esa hora |
# MAGIC | Se cayó a mitad y lo reinicias | Retoma donde quedó, sin duplicar |
# MAGIC | Reprocesar por error humano | No pasa: ya sabe qué leyó |

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 5 · Llega un archivo nuevo
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Simular lo que pasa todos los días: **llega un archivo nuevo al origen**. En
# MAGIC `00_generar_datos` se reservó un lote aparte (`transacciones_nuevas`) justo para esto.
# MAGIC
# MAGIC Vas a copiarlo a la carpeta de origen y correr la ingesta una vez más. Esta vez el
# MAGIC conteo **sí** debe crecer — pero **solo** por las filas nuevas.

# COMMAND ----------

# copiar el lote "que llega después" a la carpeta de origen
import os

origen_nuevo = f"{RAW}/transacciones_nuevas"
nuevos = [f for f in dbutils.fs.ls(origen_nuevo) if f.name.endswith(".json")]
for f in nuevos:
    dbutils.fs.cp(f.path, f"{RAW}/transacciones/{f.name}")

print(f"📥 Copiado(s) {len(nuevos)} archivo(s) nuevo(s) al origen.")
print("   Ahora la carpeta tiene un archivo que Auto Loader todavía no vio.")

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", ruta_checkpoint)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_datos_rescatados")
    .load(f"{RAW}/transacciones")
    .selectExpr("*", "current_timestamp() AS _ingerido_en",
                "_metadata.file_path AS _archivo_origen")
    .writeStream
    .option("checkpointLocation", ruta_checkpoint)
    .trigger(availableNow=True)
    .toTable("bronze_transacciones")
    .awaitTermination())

filas_tercera_carga = spark.table("bronze_transacciones").count()
print(f"📊 Antes del archivo nuevo : {filas_segunda_carga:,}")
print(f"📊 Después                 : {filas_tercera_carga:,}")
print(f"📊 Creció en               : {filas_tercera_carga - filas_segunda_carga:,} filas")
print()
if filas_tercera_carga > filas_segunda_carga:
    print("✅ Creció SOLO por las filas nuevas. Auto Loader no volvió a leer los 12 archivos")
    print("   anteriores — solo el que acababa de llegar. Eso es ingesta incremental.")
else:
    print("⚠️  No creció. ¿Se copió el archivo nuevo en la celda anterior?")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · La trazabilidad en acción
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Usar `_archivo_origen` para ver **exactamente qué archivo trajo qué**. Esta es la
# MAGIC columna que convierte *"algo salió mal anoche"* en *"el archivo X trajo estas filas"*.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     regexp_extract(_archivo_origen, '([^/]+)$', 1) AS archivo,
# MAGIC     COUNT(*)                                       AS filas
# MAGIC FROM bronze_transacciones
# MAGIC GROUP BY 1
# MAGIC ORDER BY filas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 👀 **Cada archivo aparece con su conteo de filas.** El que copiaste en el paso 5 está
# MAGIC ahí, separado del resto.
# MAGIC
# MAGIC Si mañana te avisan *"el lote de las 3pm venía corrupto"*, sabes exactamente qué filas
# MAGIC borrar — sin tocar las demás. Sin esta columna, tendrías que reprocesar todo.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · Ingiere los clientes (que llegaron como CSV)
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Ingerir la dimensión de clientes, que llegó como **CSV** en vez de JSON. La necesitarás
# MAGIC en el Módulo 3 para enriquecer las transacciones.
# MAGIC
# MAGIC ## Un detalle del CSV que conviene conocer
# MAGIC Un CSV tiene **encabezado**: la primera fila son los nombres de las columnas. A
# MAGIC diferencia del JSON —donde cada valor viene con su nombre— en un CSV hay que saber si
# MAGIC esa primera fila es un encabezado o ya es un dato.
# MAGIC
# MAGIC **La buena noticia:** Auto Loader **asume que los archivos CSV tienen encabezado** al
# MAGIC inferir el esquema. Así que esto simplemente funciona — vas a ver los nombres reales
# MAGIC (`cliente_id`, `segmento`…), no `_c0`, `_c1`.
# MAGIC
# MAGIC ### 📝 TU TURNO
# MAGIC Completa el **formato** de origen (es `csv`) y el **nombre de la tabla**
# MAGIC (`bronze_clientes`).

# COMMAND ----------

ruta_ckpt_clientes = f"{RAW}/_checkpoints/bronze_clientes"

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "FORMATO")                     # 📝 TODO: ¿json? ¿csv?
    .option("cloudFiles.schemaLocation", ruta_ckpt_clientes)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_datos_rescatados")
    .load(f"{RAW}/clientes")
    .selectExpr("*", "current_timestamp() AS _ingerido_en",
                "_metadata.file_path AS _archivo_origen")
    .writeStream
    .option("checkpointLocation", ruta_ckpt_clientes)
    .trigger(availableNow=True)
    .toTable("TABLA_CLIENTES")                                  # 📝 TODO: bronze_clientes
    .awaitTermination())

print("✅ Ingesta de clientes terminada")
print("   Mira las columnas abajo: deben tener nombres reales (cliente_id, segmento…).")
display(spark.table("bronze_clientes").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary>💡 <b>Solución del paso 7</b></summary>
# MAGIC
# MAGIC <pre>.option("cloudFiles.format", "csv")
# MAGIC ...
# MAGIC .toTable("bronze_clientes")</pre>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 8 · Verifica tu checkpoint del módulo
# MAGIC
# MAGIC Ejecuta esta celda. Si sale todo en verde, terminaste el módulo.

# COMMAND ----------

resultados = []

# 1 · bronze_transacciones existe y tiene datos
try:
    ft = spark.table("bronze_transacciones").count()
    resultados.append((ft > 300_000, f"bronze_transacciones tiene {ft:,} filas (se esperan ~375k)"))
except Exception:
    ft = 0
    resultados.append((False, "No encuentro bronze_transacciones — ¿hiciste el paso 3?"))

# 2 · tiene las columnas de trazabilidad
try:
    cols = spark.table("bronze_transacciones").columns
    tiene = "_ingerido_en" in cols and "_archivo_origen" in cols
    resultados.append((tiene, f"Tiene trazabilidad (_ingerido_en, _archivo_origen): {tiene}"))
except Exception:
    resultados.append((False, "No pude leer las columnas"))

# 3 · el archivo nuevo se ingirió (más de un archivo de origen distinto)
try:
    n_arch = spark.sql("""
        SELECT COUNT(DISTINCT _archivo_origen) c FROM bronze_transacciones
    """).collect()[0][0]
    resultados.append((n_arch >= 13, f"Se ingirieron {n_arch} archivos distintos (12 + el nuevo)"))
except Exception:
    resultados.append((False, "No pude contar los archivos de origen"))

# 4 · bronze_clientes con encabezado correcto (no _c0)
try:
    cc = spark.table("bronze_clientes").columns
    bien = "cliente_id" in cc
    resultados.append((bien, f"bronze_clientes tiene el encabezado correcto (cliente_id, …): {bien}"))
except Exception:
    resultados.append((False, "No encuentro bronze_clientes — ¿hiciste el paso 7?"))

# ---- resultado ----
print("=" * 70)
print("  MÓDULO 2 · CHECKPOINT")
print("=" * 70)
for ok, msg in resultados:
    print(f"  {'✅' if ok else '❌'}  {msg}")
print("=" * 70)

if all(ok for ok, _ in resultados):
    print("""
  🎉 ¡Módulo 2 completo!

  Construiste tu capa bronze real:
    · 375.000 transacciones ingeridas con Auto Loader
    · Un proceso incremental: correrlo de nuevo no duplica
    · Trazabilidad completa de qué archivo trajo qué
    · La dimensión de clientes, con el encabezado bien leído

  👉 En el Módulo 3 vas a limpiar esta bronze hacia la capa silver:
     tipos correctos, sin duplicados, con reglas de calidad.
""")
else:
    print("""
  ⚠️  Falta algo. Revisa los ❌ de arriba. Lo más común:
     · no completaste el formato 'csv' o el nombre bronze_clientes en el paso 7
     · no copiaste el archivo nuevo en el paso 5
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Lo que te llevas de este módulo
# MAGIC
# MAGIC | Concepto | Dónde lo viviste |
# MAGIC |---|---|
# MAGIC | **Auto Loader ingiere solo lo nuevo** | Correr dos veces no duplicó |
# MAGIC | **El checkpoint es la memoria del proceso** | Una línea, `checkpointLocation` |
# MAGIC | **Bronze no se transforma** | `fecha` sigue como texto, a propósito |
# MAGIC | **Trazabilidad** | `_archivo_origen` te dice qué trajo cada archivo |
# MAGIC | **Los datos malos no se pierden** | `_datos_rescatados` |
# MAGIC | **Auto Loader maneja el encabezado del CSV** | Ingeriste `bronze_clientes` con nombres reales |
# MAGIC
# MAGIC ## ⚠️ Recordatorio antes de seguir
# MAGIC
# MAGIC En producción, esta ingesta **no se corre a mano celda por celda**. Vive dentro de un
# MAGIC pipeline de Lakeflow (Módulo 3) o de un job programado (Módulo 5), con reintentos,
# MAGIC monitoreo y reinicio automático ante cambios de esquema.
# MAGIC
# MAGIC **Los conceptos son idénticos** — lo que cambia es la ingeniería alrededor.
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC
# MAGIC Tienes **bronze**: el dato crudo, completo y trazable. En el **Módulo 3** empieza la
# MAGIC transformación — de bronze a **silver** (limpio) y **gold** (listo para consumir), con
# MAGIC un pipeline declarativo que se encarga del orden y la calidad por ti.
