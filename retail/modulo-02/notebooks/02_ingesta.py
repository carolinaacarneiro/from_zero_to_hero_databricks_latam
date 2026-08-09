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
# MAGIC - **Ejecuta celda por celda** con `Shift + Enter`. No uses "Run all".
# MAGIC - Donde dice **📝 TU TURNO**, te toca completar algo. Debajo hay una solución.
# MAGIC - 🙋 **¿Trabado más de 3 minutos?** Levanta la mano.
# MAGIC
# MAGIC ## De dónde venimos y a dónde vamos
# MAGIC En el Módulo 1 creaste una tabla de **práctica** con 1.000 filas. Hoy construyes **la capa
# MAGIC bronze real**: las ~585.000 líneas de venta completas, con un proceso que puedes volver a
# MAGIC correr sin miedo.
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
# MAGIC | 🔒 **Datos 100% sintéticos** | Ninguna empresa ni persona real |
# MAGIC | 🚫 **No lo copies a producción tal cual** | Ahí Auto Loader corre dentro de un pipeline o job, con monitoreo y reintentos |
# MAGIC | ✅ **Los conceptos sí son reales** | Auto Loader, checkpoints y la capa bronze son lo que usan las empresas de verdad |
# MAGIC
# MAGIC > 💡 **El caso es retail, pero el método es de cualquier industria.** Ingerir archivos que
# MAGIC > llegan solos —ventas, sensores de una fábrica, eventos de una app— es el mismo patrón.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 · Tu espacio de trabajo
# MAGIC
# MAGIC **Celda A** crea los campos y busca dónde quedaron tus datos. **Celda B** los lee. Usa
# MAGIC los **mismos valores** que en `00_generar_datos`.

# COMMAND ----------

# ═══ CELDA A · crear los campos ═══════════════════════════════════════════
import re

_usuario = spark.sql("SELECT current_user()").collect()[0][0]
_schema_sugerido = "retail_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())

_ocultos = ("system", "samples", "__databricks_internal", "hive_metastore")
_disponibles = [r[0] for r in spark.sql("SHOW CATALOGS").collect()
                if r[0].lower() not in _ocultos]

_encontrado = None
for _c in _disponibles:
    try:
        if _schema_sugerido in [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{_c}`").collect()]:
            _encontrado = _c
            break
    except Exception:
        continue

dbutils.widgets.text("catalogo", _encontrado or (_disponibles[0] if _disponibles else ""),
                     "1 · Catálogo")
dbutils.widgets.text("schema", _schema_sugerido, "2 · Schema")

print("👆 Aparecieron dos campos ARRIBA del notebook.\n")
if _encontrado:
    print(f"   ✅ Encontré tu schema del taller: {_encontrado}.{_schema_sugerido}")
    print("      Los campos ya quedaron listos. Sigue con la celda B.")
else:
    print(f"   ⚠️  No encontré un schema '{_schema_sugerido}'. Revisa los campos.")
    print("   📦 Catálogos disponibles:", ", ".join(_disponibles))

# COMMAND ----------

# ═══ CELDA B · leer los campos y comprobar ════════════════════════════════
catalogo = dbutils.widgets.get("catalogo").strip()
schema = dbutils.widgets.get("schema").strip()
# se recalcula acá para que esta celda sea autosuficiente (re-ejecutable sola)
usuario = spark.sql("SELECT current_user()").collect()[0][0]

catalogos = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
if catalogo not in catalogos:
    raise Exception(
        f"\n❌ El catálogo '{catalogo}' no existe.\n"
        f"   👉 Escribe uno de estos en el campo de arriba:\n"
        + "".join(f"        · {c}\n" for c in _disponibles)
    )

schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{catalogo}`").collect()]
if schema not in schemas:
    raise Exception(
        f"\n❌ El schema '{catalogo}.{schema}' no existe.\n"
        f"   ¿Corriste 00_generar_datos con estos mismos valores? Córrelo primero.\n"
    )

spark.sql(f"USE `{catalogo}`.`{schema}`")
RAW = f"/Volumes/{catalogo}/{schema}/raw"

try:
    n = len([f for f in dbutils.fs.ls(f"{RAW}/ventas") if f.name.endswith(".json")])
    assert n > 0
except Exception:
    raise Exception(
        f"\n❌ No encuentro archivos en {RAW}/ventas\n"
        f"   Corre primero 00_generar_datos con estos mismos valores.\n"
    )

print(f"👤 Usuario   : {usuario}")
print(f"📁 Schema    : {catalogo}.{schema}")
print(f"🗂️  Archivos  : {RAW}  ({n} archivos de ventas)")
print()
print("✅ Todo listo. Este es tu espacio: nadie más lo toca.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · Mira el origen antes de ingerir
# MAGIC
# MAGIC El primer hábito de cualquier ingesta: **mira los archivos antes de construir sobre
# MAGIC ellos**. Estos JSON llegaron a un **volumen** de Unity Catalog — igual que llegan en la
# MAGIC vida real desde el sistema del punto de venta.

# COMMAND ----------

archivos = [f for f in dbutils.fs.ls(f"{RAW}/ventas") if f.name.endswith(".json")]
print(f"📁 {len(archivos)} archivos en el origen\n")
for f in archivos[:3]:
    print(f"   {f.name[:38]}…  ({f.size / 1024 / 1024:.1f} MB)")
print(f"   … y {len(archivos) - 3} más")

# COMMAND ----------

# MAGIC %md
# MAGIC El contenido crudo, sin tocar. `fecha` llega como texto — es esperado, y lo dejamos así
# MAGIC en bronze.

# COMMAND ----------

display(spark.read.json(f"{RAW}/ventas").limit(5))

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
# MAGIC Escribir el proceso que lee los archivos y los carga a `bronze_ventas`, con
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
# MAGIC Y agregamos las dos columnas de trazabilidad de bronze: `_ingerido_en` y `_archivo_origen`.
# MAGIC
# MAGIC ### 📝 TU TURNO
# MAGIC Completa las **dos líneas** marcadas con `TODO`: el **formato** de los archivos (lo viste
# MAGIC en el paso 2) y el **nombre de la tabla** destino (`bronze_ventas`).

# COMMAND ----------

# el checkpoint y el esquema viven FUERA del directorio de la tabla (regla de Unity Catalog)
ruta_checkpoint = f"{RAW}/_checkpoints/bronze_ventas"

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "FORMATO")                     # 📝 TODO: ¿json? ¿csv?
    .option("cloudFiles.schemaLocation", ruta_checkpoint)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_datos_rescatados")
    .load(f"{RAW}/ventas")
    .selectExpr(
        "*",
        "current_timestamp() AS _ingerido_en",
        "_metadata.file_path AS _archivo_origen",
    )
    .writeStream
    .option("checkpointLocation", ruta_checkpoint)
    .trigger(availableNow=True)
    .toTable("TABLA_DESTINO")                                   # 📝 TODO: bronze_ventas
    .awaitTermination())

print("✅ Ingesta terminada")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary>💡 <b>Solución del paso 3</b> — ábrela si te trabaste</summary>
# MAGIC
# MAGIC <pre>.option("cloudFiles.format", "json")
# MAGIC ...
# MAGIC .toTable("bronze_ventas")</pre>
# MAGIC
# MAGIC Si prefieres no escribirlo, pon `USAR_SOLUCION = True` en la celda de abajo y ejecútala.
# MAGIC </details>

# COMMAND ----------

# --- Solución del paso 3 ---
USAR_SOLUCION = False

if USAR_SOLUCION:
    ruta_checkpoint = f"{RAW}/_checkpoints/bronze_ventas"
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", ruta_checkpoint)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/ventas")
        .selectExpr("*", "current_timestamp() AS _ingerido_en",
                    "_metadata.file_path AS _archivo_origen")
        .writeStream
        .option("checkpointLocation", ruta_checkpoint)
        .trigger(availableNow=True)
        .toTable("bronze_ventas")
        .awaitTermination())
    print("✅ Ingesta terminada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuenta las filas — y anota el número
# MAGIC Lo vas a comparar en el paso 4. Deberían ser **~585.000**.

# COMMAND ----------

filas_primera_carga = spark.table("bronze_ventas").count()
print(f"📊 bronze_ventas tiene {filas_primera_carga:,} filas")
display(spark.table("bronze_ventas").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC 👀 Fíjate en las **dos últimas columnas**: `_ingerido_en` y `_archivo_origen`. El origen
# MAGIC no las trae — las agregamos nosotros.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · La incrementalidad ⭐
# MAGIC ## El momento clave del módulo
# MAGIC
# MAGIC **Corre exactamente la misma ingesta otra vez.** Piensa antes de ejecutar: no llegaron
# MAGIC archivos nuevos. ¿Qué debería pasar con el conteo?
# MAGIC
# MAGIC En un proceso ingenuo —leer todo cada vez— el conteo se **duplicaría**. Con el checkpoint,
# MAGIC Auto Loader ya sabe qué archivos leyó, así que **no debería procesar nada**.

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", ruta_checkpoint)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_datos_rescatados")
    .load(f"{RAW}/ventas")
    .selectExpr("*", "current_timestamp() AS _ingerido_en",
                "_metadata.file_path AS _archivo_origen")
    .writeStream
    .option("checkpointLocation", ruta_checkpoint)
    .trigger(availableNow=True)
    .toTable("bronze_ventas")
    .awaitTermination())

filas_segunda_carga = spark.table("bronze_ventas").count()
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
# MAGIC El checkpoint —una sola línea— es lo que hace que ingerir sea **repetible sin duplicar**.
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
# MAGIC Simula lo que pasa todos los días: **llega un archivo nuevo al origen**. En
# MAGIC `00_generar_datos` se reservó un lote aparte (`ventas_nuevas`) justo para esto.
# MAGIC
# MAGIC Vas a copiarlo a la carpeta de origen y correr la ingesta una vez más. Esta vez el conteo
# MAGIC **sí** debe crecer — pero **solo** por las filas nuevas.

# COMMAND ----------

origen_nuevo = f"{RAW}/ventas_nuevas"
nuevos = [f for f in dbutils.fs.ls(origen_nuevo) if f.name.endswith(".json")]
for f in nuevos:
    dbutils.fs.cp(f.path, f"{RAW}/ventas/{f.name}")

print(f"📥 Copiado(s) {len(nuevos)} archivo(s) nuevo(s) al origen.")
print("   Ahora la carpeta tiene un archivo que Auto Loader todavía no vio.")

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", ruta_checkpoint)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_datos_rescatados")
    .load(f"{RAW}/ventas")
    .selectExpr("*", "current_timestamp() AS _ingerido_en",
                "_metadata.file_path AS _archivo_origen")
    .writeStream
    .option("checkpointLocation", ruta_checkpoint)
    .trigger(availableNow=True)
    .toTable("bronze_ventas")
    .awaitTermination())

filas_tercera_carga = spark.table("bronze_ventas").count()
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
# MAGIC ## La trazabilidad en acción
# MAGIC `_archivo_origen` convierte *"algo salió mal anoche"* en *"el archivo X trajo estas
# MAGIC filas"*. Cada archivo aparece con su conteo:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     regexp_extract(_archivo_origen, '([^/]+)$', 1) AS archivo,
# MAGIC     COUNT(*)                                       AS filas
# MAGIC FROM bronze_ventas
# MAGIC GROUP BY 1
# MAGIC ORDER BY filas DESC

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · Ingiere los productos (que llegaron como CSV)
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Ingerir el catálogo de productos, que llegó como **CSV**. Lo necesitarás en el Módulo 3
# MAGIC para enriquecer las ventas con categoría, marca y costo.
# MAGIC
# MAGIC **La buena noticia:** Auto Loader **asume que los archivos CSV tienen encabezado** al
# MAGIC inferir el esquema. Así que esto simplemente funciona — vas a ver los nombres reales
# MAGIC (`producto_id`, `categoria`…), no `_c0`, `_c1`.
# MAGIC
# MAGIC ### 📝 TU TURNO
# MAGIC Completa el **formato** de origen (es `csv`) y el **nombre de la tabla** (`bronze_productos`).

# COMMAND ----------

ruta_ckpt_productos = f"{RAW}/_checkpoints/bronze_productos"

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "FORMATO")                     # 📝 TODO: ¿json? ¿csv?
    .option("cloudFiles.schemaLocation", ruta_ckpt_productos)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_datos_rescatados")
    .load(f"{RAW}/productos")
    .selectExpr("*", "current_timestamp() AS _ingerido_en",
                "_metadata.file_path AS _archivo_origen")
    .writeStream
    .option("checkpointLocation", ruta_ckpt_productos)
    .trigger(availableNow=True)
    .toTable("TABLA_PRODUCTOS")                                 # 📝 TODO: bronze_productos
    .awaitTermination())

print("✅ Ingesta de productos terminada")
print("   Mira las columnas abajo: deben tener nombres reales (producto_id, categoria…).")
display(spark.table("bronze_productos").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary>💡 <b>Solución del paso 6</b></summary>
# MAGIC
# MAGIC <pre>.option("cloudFiles.format", "csv")
# MAGIC ...
# MAGIC .toTable("bronze_productos")</pre>
# MAGIC </details>

# COMMAND ----------

# --- Solución del paso 6 ---
USAR_SOLUCION_6 = False

if USAR_SOLUCION_6:
    ruta_ckpt_productos = f"{RAW}/_checkpoints/bronze_productos"
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", ruta_ckpt_productos)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_datos_rescatados")
        .load(f"{RAW}/productos")
        .selectExpr("*", "current_timestamp() AS _ingerido_en",
                    "_metadata.file_path AS _archivo_origen")
        .writeStream
        .option("checkpointLocation", ruta_ckpt_productos)
        .trigger(availableNow=True)
        .toTable("bronze_productos")
        .awaitTermination())
    print("✅ Ingesta de productos terminada")
    display(spark.table("bronze_productos").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Ingiere también el inventario (ya resuelto)
# MAGIC
# MAGIC El inventario (stock por producto y país) también llegó como CSV. Esta celda ya está
# MAGIC resuelta — lo dejamos listo para que el dashboard y la app del Módulo 7 lo tengan.

# COMMAND ----------

ruta_ckpt_inv = f"{RAW}/_checkpoints/bronze_inventario"
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", ruta_ckpt_inv)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_datos_rescatados")
    .load(f"{RAW}/inventario")
    .selectExpr("*", "current_timestamp() AS _ingerido_en",
                "_metadata.file_path AS _archivo_origen")
    .writeStream
    .option("checkpointLocation", ruta_ckpt_inv)
    .trigger(availableNow=True)
    .toTable("bronze_inventario")
    .awaitTermination())

print(f"✅ bronze_inventario: {spark.table('bronze_inventario').count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · Verifica tu checkpoint del módulo
# MAGIC Ejecuta esta celda. Si sale todo en verde, terminaste el módulo.

# COMMAND ----------

resultados = []

try:
    ft = spark.table("bronze_ventas").count()
    resultados.append((ft > 500_000, f"bronze_ventas tiene {ft:,} filas (se esperan ~585k)"))
except Exception:
    resultados.append((False, "No encuentro bronze_ventas — ¿hiciste el paso 3?"))

try:
    cols = spark.table("bronze_ventas").columns
    tiene = "_ingerido_en" in cols and "_archivo_origen" in cols
    resultados.append((tiene, f"Tiene trazabilidad (_ingerido_en, _archivo_origen): {tiene}"))
except Exception:
    resultados.append((False, "No pude leer las columnas"))

try:
    n_arch = spark.sql("SELECT COUNT(DISTINCT _archivo_origen) c FROM bronze_ventas").collect()[0][0]
    resultados.append((n_arch >= 13, f"Se ingirieron {n_arch} archivos distintos (12 + el nuevo)"))
except Exception:
    resultados.append((False, "No pude contar los archivos de origen"))

try:
    cc = spark.table("bronze_productos").columns
    bien = "producto_id" in cc
    resultados.append((bien, f"bronze_productos tiene el encabezado correcto (producto_id, …): {bien}"))
except Exception:
    resultados.append((False, "No encuentro bronze_productos — ¿hiciste el paso 6?"))

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
    · ~585.000 líneas de venta ingeridas con Auto Loader
    · Un proceso incremental: correrlo de nuevo no duplica
    · Trazabilidad completa de qué archivo trajo qué
    · Productos e inventario, con el encabezado bien leído

  👉 En el Módulo 3 vas a limpiar esta bronze hacia silver:
     tipos correctos, sin duplicados, con reglas de calidad.
""")
else:
    print("""
  ⚠️  Falta algo. Revisa los ❌. Lo más común:
     · no completaste el formato o el nombre bronze_productos en el paso 6
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
# MAGIC | **Auto Loader asume header en CSV** | Nombres reales, no `_c0` |
# MAGIC
# MAGIC ## ⚠️ Recordatorio
# MAGIC En producción, esta ingesta **no se corre a mano celda por celda**. Vive dentro de un
# MAGIC pipeline de Lakeflow (Módulo 3) o de un job programado (Módulo 5), con reintentos y
# MAGIC monitoreo. **Los conceptos son idénticos** — lo que cambia es la ingeniería alrededor.
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC Tienes **bronze**: el dato crudo, completo y trazable. En el **Módulo 3** empieza la
# MAGIC transformación — de bronze a **silver** (limpio) y **gold** (listo para el pronóstico y el
# MAGIC dashboard), con un pipeline declarativo.
