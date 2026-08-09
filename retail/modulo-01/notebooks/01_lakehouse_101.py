# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🧱 Módulo 1 · Tu primera tabla Delta
# MAGIC
# MAGIC Acá dejas de escuchar y empiezas a construir.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 Antes de empezar: cómo leer este notebook
# MAGIC
# MAGIC Un **notebook** es un documento donde se mezclan explicaciones y código. Está dividido en
# MAGIC **celdas**: las de texto (como esta) explican, y las de código se ejecutan.
# MAGIC
# MAGIC | Para hacer esto | Usa |
# MAGIC |---|---|
# MAGIC | Ejecutar la celda donde estás y pasar a la siguiente | `Shift + Enter` |
# MAGIC | Ejecutar sin avanzar | `Ctrl + Enter` |
# MAGIC
# MAGIC **Ve celda por celda, en orden.** No uses "Run all": la idea es que veas qué pasa en cada
# MAGIC paso, no que llegues al final rápido.
# MAGIC
# MAGIC Donde dice **📝 TU TURNO** te toca escribir algo — y justo debajo está la solución, por si
# MAGIC te trabas. Usarla no es trampa.
# MAGIC
# MAGIC > 🙋 **¿Trabado más de 3 minutos?** Levanta la mano. Estamos acá para eso.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Lo que vas a hacer hoy
# MAGIC
# MAGIC | Paso | Qué haces | Qué aprendes |
# MAGIC |---|---|---|
# MAGIC | 1 | Preparas tu espacio de trabajo | Cómo se organizan los datos en Databricks |
# MAGIC | 2 | Miras los archivos que llegaron | El primer hábito: nunca construyas a ciegas |
# MAGIC | 3 | **Creas tu primera tabla** *(de práctica, con 1.000 filas)* | Qué es una tabla Delta y por qué es el estándar |
# MAGIC | 4 | Lees el historial de la tabla | Que cada cambio queda registrado solo |
# MAGIC | 5 | Modificas la tabla — y te equivocas | Que en datos, equivocarse es normal |
# MAGIC | 6 | **Recuperas el dato anterior** ⭐ | *Time travel*: el momento clave del módulo |
# MAGIC | 7 | Exploras con el asistente de IA y el explorador | Cómo se trabaja hoy en Databricks |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## El caso: pronóstico de ventas en una cadena de retail
# MAGIC
# MAGIC Trabajas en el área de datos de una cadena de retail con tiendas en varios países de
# MAGIC América Latina. Llegaron archivos con las ventas y el equipo comercial necesita
# MAGIC **pronosticar cuánto se venderá** para planear el inventario.
# MAGIC
# MAGIC Hoy empiezas por el principio: **convertir esos archivos en algo consultable y confiable**.
# MAGIC
# MAGIC > 💡 **El caso es retail, pero lo que vas a aprender no es de retail.**
# MAGIC > Ingerir archivos, versionar cambios, poder volver atrás y gobernar quién ve qué son
# MAGIC > problemas de **cualquier industria**. El mismo flujo sirve para detectar fraude en un
# MAGIC > banco, anticipar fallas en una fábrica o analizar historias clínicas en salud. Cambia el
# MAGIC > dato, no el método.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Importante: esto es material de aprendizaje
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | 🎓 **Objetivo** | Enseñar conceptos. Optimizado para que se entienda, no para producción |
# MAGIC | 🔒 **Datos 100% sintéticos** | Generados por el notebook anterior. Ninguna empresa ni persona real |
# MAGIC | 🚫 **No copies este código a producción tal cual** | Le falta manejo de errores, pruebas, control de costos y CI/CD |
# MAGIC | ✅ **Los conceptos sí son reales** | Delta Lake, Unity Catalog y time travel son lo que usan las empresas de verdad |
# MAGIC
# MAGIC > ⚠️ **¿No corriste `00_generar_datos` todavía?** Hazlo primero: este notebook usa los
# MAGIC > archivos que ese genera.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 · Tu espacio de trabajo
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Le vas a decir al notebook **dónde** trabajar: en qué catálogo y en qué schema.
# MAGIC
# MAGIC ## Por qué importa
# MAGIC En Databricks los datos se organizan en **tres niveles**, como carpetas anidadas:
# MAGIC
# MAGIC ```
# MAGIC catálogo  →  schema  →  tabla
# MAGIC    │           │          └── mis_ventas
# MAGIC    │           └── retail_tu_usuario         (tu espacio personal)
# MAGIC    └── el catálogo de tu organización
# MAGIC ```
# MAGIC
# MAGIC Eso se llama **Unity Catalog** y es el sistema de gobierno de Databricks. Con esos tres
# MAGIC niveles se controla **quién ve qué**, se rastrea de dónde viene cada dato y se audita
# MAGIC quién lo tocó — sin instalar ni configurar nada aparte.
# MAGIC
# MAGIC 👉 **Todo lo que crees hoy vive en tu propio schema.** Nadie más lo modifica.
# MAGIC
# MAGIC ## Qué hacer — son dos celdas
# MAGIC **Celda A** crea los dos campos y te muestra qué catálogos puedes usar. **Celda B** los
# MAGIC lee y comprueba que todo esté en su lugar. Van separadas porque los campos **no existen**
# MAGIC hasta que corres la celda A.

# COMMAND ----------

# ═══ CELDA A · crear los campos ═══════════════════════════════════════════
# Ejecuta esta celda. Después mira los dos campos que aparecen ARRIBA del notebook,
# ajústalos si hace falta, y sigue con la celda B.

import re

# current_user() devuelve el correo con el que iniciaste sesión: se usa para proponer un
# nombre de schema único tuyo, así nadie tiene que inventarse uno.
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
_schema_sugerido = "retail_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())

_ocultos = ("system", "samples", "__databricks_internal", "hive_metastore")
_disponibles = [r[0] for r in spark.sql("SHOW CATALOGS").collect()
                if r[0].lower() not in _ocultos]

# En vez de adivinar el catálogo, se BUSCA dónde quedó tu schema del taller: así los campos
# ya vienen con los valores correctos y no hay que recordar qué se usó en el otro notebook.
_encontrado = None
for _c in _disponibles:
    try:
        _sc = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{_c}`").collect()]
        if _schema_sugerido in _sc:
            _encontrado = _c
            break
    except Exception:
        continue   # sin permiso para listar este catálogo: se ignora y se sigue

_sugerido = _encontrado or (_disponibles[0] if _disponibles else "")

dbutils.widgets.text("catalogo", _sugerido, "1 · Catálogo")
dbutils.widgets.text("schema", _schema_sugerido, "2 · Schema")

print("👆 Ya aparecieron los dos campos ARRIBA de este notebook.\n")
if _encontrado:
    print(f"   ✅ Encontré tu schema del taller: {_encontrado}.{_schema_sugerido}")
    print(f"      Los campos ya quedaron con esos valores. Sigue con la celda B.")
else:
    print(f"   ⚠️  No encontré un schema llamado '{_schema_sugerido}' en ningún catálogo.")
    print(f"      Revisa los campos de arriba antes de seguir.\n")
    print("   📦 Catálogos donde puedes buscar:")
    for c in _disponibles:
        print(f"        · {c}")
    print()
    print("   Usa los MISMOS valores que en 00_generar_datos — ahí quedaron tus archivos.")

# COMMAND ----------

# ═══ CELDA B · leer los campos y comprobar ════════════════════════════════
# Si cambias algo en los campos de arriba, vuelve a ejecutar solo esta celda.

catalogo = dbutils.widgets.get("catalogo").strip()
schema = dbutils.widgets.get("schema").strip()
# se recalcula acá para que esta celda sea autosuficiente: puedes re-ejecutar solo la celda B
# sin depender de que la celda A siga en memoria.
usuario = spark.sql("SELECT current_user()").collect()[0][0]

# Este notebook NO crea nada: eso lo hizo 00_generar_datos. Si algo falta, mejor decirlo acá.
catalogos = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
if catalogo not in catalogos:
    raise Exception(
        f"\n❌ El catálogo '{catalogo}' no existe en este workspace.\n\n"
        f"   👉 Escribe uno de estos en el campo 'catalogo' de ARRIBA:\n"
        + "".join(f"        · {c}\n" for c in _disponibles)
        + f"\n   Después vuelve a ejecutar SOLO esta celda (celda B).\n"
    )

schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{catalogo}`").collect()]
if schema not in schemas:
    parecidos = [s for s in schemas if s.startswith("retail_")]
    msg = f"\n❌ El schema '{catalogo}.{schema}' no existe.\n\n"
    if parecidos:
        msg += ("   👉 En este catálogo hay estos schemas del taller:\n"
                + "".join(f"        · {s}\n" for s in parecidos[:5])
                + "\n   Escribe uno en el campo 'schema' de arriba y ejecuta esta celda.\n")
    else:
        msg += ("   No hay ningún schema del taller en este catálogo.\n\n"
                "   ¿Corriste el notebook 00_generar_datos con estos mismos dos valores?\n"
                "   Córrelo primero: este ejercicio usa los archivos que ese genera.\n")
    raise Exception(msg)

# USE fija el schema por defecto: a partir de acá puedes escribir 'mis_ventas' en vez del
# nombre completo 'catalogo.schema.mis_ventas'
spark.sql(f"USE `{catalogo}`.`{schema}`")

# Un VOLUME es el lugar donde Unity Catalog guarda ARCHIVOS (no tablas): JSON, CSV, imágenes.
RAW = f"/Volumes/{catalogo}/{schema}/raw"

try:
    n = len([f for f in dbutils.fs.ls(f"{RAW}/ventas") if f.name.endswith(".json")])
    if n == 0:
        raise Exception("sin archivos")
except Exception:
    raise Exception(
        f"\n❌ No encuentro los archivos en {RAW}/ventas\n\n"
        f"   Corre primero el notebook 00_generar_datos con estos mismos valores de\n"
        f"   catálogo y schema.\n"
    )

print(f"👤 Tu usuario   : {usuario}")
print(f"📦 Tu catálogo  : {catalogo}")
print(f"📁 Tu schema    : {schema}")
print(f"🗂️  Tus archivos : {RAW}  ({n} archivos de ventas)")
print()
print("✅ Todo listo. Este es tu espacio: nadie más lo toca.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · ¿Qué llegó del punto de venta?
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Mirar los archivos crudos **antes** de construir cualquier cosa con ellos.
# MAGIC
# MAGIC ## Por qué importa
# MAGIC Es el primer hábito de cualquier trabajo con datos: **nunca construyas sobre algo que no
# MAGIC viste**. Cuántos archivos hay, qué columnas traen, si vienen valores raros.
# MAGIC
# MAGIC Los datos llegaron como **archivos JSON**, igual que llegan en la vida real: el sistema
# MAGIC del punto de venta deja un archivo cada cierto tiempo y alguien tiene que convertirlos en
# MAGIC información útil. Ese alguien eres tú hoy.

# COMMAND ----------

# dbutils.fs es la utilidad para explorar archivos. ls = "list", como en la terminal.
archivos = [f for f in dbutils.fs.ls(f"{RAW}/ventas") if f.name.endswith(".json")]

print(f"📁 {len(archivos)} archivos de ventas\n")
for f in archivos[:3]:
    print(f"   {f.name[:40]}…  ({f.size / 1024 / 1024:.1f} MB)")
print(f"   … y {len(archivos) - 3} más")

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora **el contenido**. Fíjate en tres columnas, porque las vas a volver a ver:
# MAGIC
# MAGIC | Columna | Qué es | Cuándo la usarás |
# MAGIC |---|---|---|
# MAGIC | `pais` | País de la venta (CO, MX, BR…) | **Módulo 4** — vas a filtrar por seguridad |
# MAGIC | `fecha` | Cuándo se vendió | **Módulo 6** — la serie de tiempo del pronóstico |
# MAGIC | `monto` | Valor de la línea de venta (USD) | **Módulo 6** — lo que se pronostica |
# MAGIC
# MAGIC > 💡 **`display()` es de Databricks**, no de Python. Muestra los datos en una tabla
# MAGIC > navegable, y con el botón **+** de arriba puedes convertirla en gráfico sin escribir
# MAGIC > código. Pruébalo cuando tengas el resultado.

# COMMAND ----------

display(spark.read.json(f"{RAW}/ventas").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · Tu primera tabla Delta
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Convertir esos archivos en una **tabla** que puedas consultar con SQL.
# MAGIC
# MAGIC Vas a usar solo **1.000 ventas** de muestra, a propósito: así todo responde al instante y
# MAGIC puedes experimentar sin esperar.
# MAGIC
# MAGIC > ℹ️ **Esta es una tabla de práctica, no tu capa bronze.** La bronze — completa e
# MAGIC > incremental — la construyes en el Módulo 2. Acá el objetivo es entender qué es Delta
# MAGIC > Lake con una tabla pequeña y rápida.
# MAGIC
# MAGIC ## Por qué importa: qué es Delta Lake
# MAGIC
# MAGIC En Databricks, `CREATE TABLE` crea una **tabla Delta** por defecto. No hay que pedirlo. Y
# MAGIC eso trae cuatro cosas que un archivo suelto **no** tiene:
# MAGIC
# MAGIC | | Un archivo suelto (CSV, JSON) | Una tabla Delta |
# MAGIC |---|---|---|
# MAGIC | **Transacciones** | Si el proceso se cae a mitad, quedan datos corruptos | O todo se escribe, o nada |
# MAGIC | **Historial** | Se sobrescribe y el dato anterior se pierde | Cada cambio queda registrado |
# MAGIC | **Lecturas concurrentes** | Quien lee mientras alguien escribe puede ver basura | Cada lector ve una versión consistente |
# MAGIC | **Rendimiento** | Recorre todo el archivo siempre | Estadísticas que le permiten saltarse datos |
# MAGIC
# MAGIC ### 🔷 Por qué esto es un diferencial de Databricks
# MAGIC Antes había que elegir: un **data warehouse** (rápido y confiable, pero cerrado y caro) o
# MAGIC un **data lake** (barato y flexible, pero sin garantías). Delta Lake trajo las garantías
# MAGIC del warehouse **a archivos en formato abierto**. Eso es lo que hace posible el
# MAGIC **lakehouse**: un solo lugar, sin copias. Y **Delta Lake es open source** — tus datos
# MAGIC quedan en tu propio almacenamiento en la nube, en un formato que otras herramientas
# MAGIC pueden leer. **No quedas encerrado.**
# MAGIC
# MAGIC ## 📝 TU TURNO
# MAGIC
# MAGIC Abajo falta **una palabra**. Para leer archivos directamente en SQL se escribe
# MAGIC `formato.\`ruta\`` — por ejemplo `` csv.`/ruta/archivo` ``.
# MAGIC
# MAGIC Reemplaza `FORMATO` por el formato correcto de estos archivos. *(Pista: lo viste en el
# MAGIC paso 2 — los archivos terminan en `.json`.)*

# COMMAND ----------

# TODO: reemplaza FORMATO por el formato de los archivos (¿json? ¿csv? ¿parquet?)
spark.sql(f"""
    CREATE OR REPLACE TABLE mis_ventas AS
    SELECT
        venta_id,
        fecha,
        producto_id,
        pais,
        canal,
        cantidad,
        precio_unitario,
        monto
    FROM FORMATO.`{RAW}/ventas`
    LIMIT 1000
""")

print(f"✅ Tabla creada con {spark.table('mis_ventas').count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary>💡 <b>Solución del paso 3</b> — ábrela si te trabaste</summary>
# MAGIC
# MAGIC El formato es <code>json</code>. La línea queda:
# MAGIC
# MAGIC <pre>FROM json.`{RAW}/ventas`</pre>
# MAGIC
# MAGIC Si prefieres no escribirlo, pon `USAR_SOLUCION = True` en la celda de abajo y ejecútala.
# MAGIC </details>

# COMMAND ----------

# --- Solución del paso 3 ---
# Si te trabaste, cambia USAR_SOLUCION a True y ejecuta esta celda.
USAR_SOLUCION = False

if USAR_SOLUCION:
    spark.sql(f"""
        CREATE OR REPLACE TABLE mis_ventas AS
        SELECT venta_id, fecha, producto_id, pais, canal,
               cantidad, precio_unitario, monto
        FROM json.`{RAW}/ventas`
        LIMIT 1000
    """)
    print(f"✅ Tabla creada con {spark.table('mis_ventas').count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mira tu tabla
# MAGIC
# MAGIC > 💡 **`%sql` al inicio de una celda** cambia el lenguaje de esa celda a SQL. En el mismo
# MAGIC > notebook puedes mezclar Python y SQL, usando cada uno donde conviene. Eso también es de
# MAGIC > Databricks: no tienes que elegir un solo lenguaje.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mis_ventas LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿De verdad es una tabla Delta?
# MAGIC
# MAGIC No lo pediste en ningún momento. Compruébalo con `DESCRIBE EXTENDED`, que muestra **todo
# MAGIC lo que Databricks sabe** sobre tu tabla. Baja hasta `# Detailed Table Information`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED mis_ventas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Qué acabas de ver
# MAGIC
# MAGIC El resultado tiene **dos partes**.
# MAGIC
# MAGIC ## Parte 1 · Arriba: las columnas y sus tipos
# MAGIC Las primeras filas son tus columnas con el tipo que Databricks **infirió solo** leyendo
# MAGIC el JSON:
# MAGIC
# MAGIC | Columna | Tipo | Por qué ese |
# MAGIC |---|---|---|
# MAGIC | `venta_id`, `producto_id` | `string` | Texto: tienen letras y números |
# MAGIC | `cantidad` | `bigint` | Número entero |
# MAGIC | `monto`, `precio_unitario` | `double` | Número con decimales |
# MAGIC | `fecha` | **`string`** | 👀 **Fíjate en esta** |
# MAGIC
# MAGIC ### 👀 `fecha` quedó como texto, no como fecha
# MAGIC **Eso es un problema real, y es a propósito que lo veas ahora.**
# MAGIC
# MAGIC En JSON no existe el tipo "fecha": las fechas viajan como texto (`"2026-03-14 15:27:00"`).
# MAGIC Databricks leyó texto y guardó texto. **Hace bien en no adivinar**: si interpretara mal el
# MAGIC formato corrompería tus datos en silencio.
# MAGIC
# MAGIC La consecuencia: mientras sea `string` **no puedes** preguntar *"ventas del último mes"*
# MAGIC ni *"agrupa por semana"* de forma confiable. **Y eso es justo lo que necesita un
# MAGIC pronóstico.** Por eso convertir `fecha` a fecha de verdad será una tarea clave del
# MAGIC **Módulo 3**.
# MAGIC
# MAGIC 👉 **Déjala como está por ahora.** Guardar el dato **tal como llegó del origen** es una
# MAGIC decisión deliberada: si más adelante la limpieza sale mal, todavía tienes el original.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Parte 2 · Abajo: los metadatos de la tabla
# MAGIC Después de `# Detailed Table Information`, las líneas que vale la pena entender:
# MAGIC
# MAGIC | Línea | Qué dice | Por qué importa |
# MAGIC |---|---|---|
# MAGIC | **`Provider`** | `delta` | ✅ **La respuesta.** Es Delta sin que lo pidieras |
# MAGIC | **`Type`** | `MANAGED` | Databricks administra los archivos por ti |
# MAGIC | **`Location`** | una ruta `s3://…` o `abfss://…` | 👀 **Tus datos están en tu propia nube** |
# MAGIC | **`Owner`** | tu correo | Base de los permisos del Módulo 4 |
# MAGIC | **`Predictive Optimization`** | `ENABLE` | Databricks optimiza la tabla sola |
# MAGIC
# MAGIC **`Location` apunta a tu propio almacenamiento en la nube.** Tus datos no están *dentro*
# MAGIC de Databricks: están en tu cuenta de S3, ADLS o GCS, en formato Delta abierto. Si mañana
# MAGIC quisieras irte, podrías. Esa es la diferencia entre arquitectura abierta y quedar
# MAGIC encerrado.
# MAGIC
# MAGIC > 💡 **Un hábito que vale adoptar:** cuando llegues a una tabla que no conoces, corre
# MAGIC > `DESCRIBE EXTENDED`. En 5 segundos sabes de qué tipo es, dónde vive y quién la creó.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❓ ¿Esta tabla ya es mi capa bronze?
# MAGIC
# MAGIC **No todavía.** Databricks recomienda organizar los datos en **tres capas** — la
# MAGIC arquitectura **medallion**:
# MAGIC
# MAGIC | Capa | Qué contiene | Cuándo la construyes |
# MAGIC |---|---|---|
# MAGIC | 🥉 **Bronze** | Todo el dato crudo, tal como llegó del origen | **Módulo 2** |
# MAGIC | 🥈 **Silver** | Limpio, tipado, sin duplicados ni filas inválidas | **Módulo 3** |
# MAGIC | 🥇 **Gold** | Agregado y listo para consumir: dashboards, pronóstico | **Módulo 3** |
# MAGIC
# MAGIC Lo que acabas de crear es una **tabla de práctica**: 1.000 filas para aprender Delta Lake
# MAGIC sin esperar. No es bronze porque le falta lo esencial: estar **completa** y cargarse de
# MAGIC forma **incremental** (que si llegan archivos nuevos, ingiere solo esos). Eso lo hace
# MAGIC **Auto Loader** en el Módulo 2.
# MAGIC
# MAGIC ### 🔷 Por qué separar en capas
# MAGIC Porque **la limpieza se hace con reglas, y las reglas cambian**. Si transformas al
# MAGIC ingerir y descartas el original, cuando descubras que una regla estaba mal ya no tienes de
# MAGIC dónde rehacerla. Guardar bronze intacto cuesta almacenamiento (barato) y te compra poder
# MAGIC reprocesar (invaluable cuando algo sale mal).

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · El historial de versiones
# MAGIC
# MAGIC ## Por qué importa
# MAGIC Cada vez que una tabla Delta cambia, **el cambio queda anotado** en un registro llamado
# MAGIC *transaction log*. No configuraste nada para que eso pasara.
# MAGIC
# MAGIC Todavía no modificaste nada, así que deberías ver **una sola versión: la 0**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY mis_ventas

# COMMAND ----------

# MAGIC %md
# MAGIC 👀 **Mira `version`, `timestamp`, `operation` y `userName`.** Eso es **auditoría**: quién
# MAGIC cambió qué y cuándo. En muchas plataformas hay que construirla con tablas de log y
# MAGIC *triggers*. Acá viene por defecto — y para una empresa regulada, poder responder *"¿quién
# MAGIC modificó este dato?"* no es comodidad, es cumplimiento.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 5 · Te equivocas a propósito
# MAGIC
# MAGIC ## La situación
# MAGIC Llega un mensaje urgente del equipo comercial:
# MAGIC
# MAGIC > *"Hubo un error de precio: todos los productos de más de 500 dólares se cargaron con un
# MAGIC > 20% de más. Corrígelo bajando el monto de esas ventas. Lo necesitamos ya."*
# MAGIC
# MAGIC Lo haces. Es lo que pidieron.
# MAGIC
# MAGIC ## Paso 5.1 · Antes de cambiar nada, mide
# MAGIC Vas a modificar datos. **Siempre cuenta primero**: sin un número de referencia, después no
# MAGIC puedes saber si tu cambio hizo lo que esperabas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Cuántas ventas superan los 500 USD, y cuánto suman? Estas son las que vas a tocar.
# MAGIC SELECT
# MAGIC     COUNT(*)              AS ventas_grandes,
# MAGIC     ROUND(SUM(monto), 0)  AS monto_total_de_esas
# MAGIC FROM mis_ventas
# MAGIC WHERE monto > 500

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 TU TURNO · escribe el UPDATE
# MAGIC
# MAGIC ### Qué tienes que lograr
# MAGIC Que las ventas de **más de 500 USD** bajen su `monto` un 20% (multiplicar por `0.8`).
# MAGIC
# MAGIC | | Valor | De dónde sale |
# MAGIC |---|---|---|
# MAGIC | **Qué tabla** | `mis_ventas` | La que creaste en el paso 3 |
# MAGIC | **Qué columna, y a qué valor** | `monto` → `monto * 0.8` | La corrección del 20% |
# MAGIC | **En qué filas** | las que tengan `monto > 500` | El umbral del mensaje |
# MAGIC
# MAGIC ```sql
# MAGIC UPDATE  <la tabla>
# MAGIC SET     <columna> = <valor nuevo>     -- qué cambiar
# MAGIC WHERE   <condición>                   -- en cuáles filas
# MAGIC ```
# MAGIC
# MAGIC > ⚠️ **El `WHERE` no es opcional.** Un `UPDATE` sin `WHERE` cambia **todas** las filas —
# MAGIC > acá bajaría el monto de las 1.000 ventas. Es uno de los errores más caros en bases de
# MAGIC > datos. Acá no pasa nada grave porque tienes time travel, pero toma el hábito de escribir
# MAGIC > el `WHERE` primero.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: escribe acá tu UPDATE (tres líneas) y ejecuta la celda
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary>💡 <b>Solución del paso 5</b> — ábrela si te trabaste</summary>
# MAGIC
# MAGIC <pre>UPDATE mis_ventas
# MAGIC SET monto = monto * 0.8
# MAGIC WHERE monto > 500</pre>
# MAGIC
# MAGIC Databricks te devuelve <b>num_affected_rows</b>: debe coincidir con
# MAGIC <b>ventas_grandes</b> de la celda anterior.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔷 Lo que acabas de hacer es menos trivial de lo que parece
# MAGIC
# MAGIC Acabas de actualizar filas sueltas de datos que viven como **archivos en almacenamiento
# MAGIC de objetos** en la nube. **Antes de Delta Lake eso no se podía hacer:** el almacenamiento
# MAGIC de objetos no permite modificar parte de un archivo, así que para cambiar unas filas había
# MAGIC que **reescribir el archivo completo**. Por eso los data lakes eran, en la práctica, de
# MAGIC solo lectura — y por eso hacían falta dos sistemas.
# MAGIC
# MAGIC `UPDATE` y `DELETE` sobre formato abierto es lo que hizo viable tener los datos en un lake
# MAGIC y aun así poder corregirlos (y cumplir leyes de privacidad que exigen borrar datos de una
# MAGIC persona).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mira el historial otra vez
# MAGIC **Ahora hay una versión 1.** La tabla cambió — pero **la versión 0 sigue ahí**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY mis_ventas

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · Time travel ⭐
# MAGIC ## El momento clave del módulo
# MAGIC
# MAGIC ## La situación, continuación
# MAGIC Vuelve el equipo comercial:
# MAGIC
# MAGIC > *"Nos equivocamos: el error de precio era de otra tienda, tus datos estaban bien.
# MAGIC > ¿Puedes decirnos cómo estaba la tabla antes del cambio?"*
# MAGIC
# MAGIC **Piensa cómo responderías** eso con archivos CSV en un servidor. La respuesta honesta
# MAGIC sería *"tendría que pedir que restauren un backup"* — horas, en el mejor caso.
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Responder con **una consulta**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- La misma tabla, en dos momentos distintos, en una sola consulta
# MAGIC SELECT
# MAGIC     (SELECT ROUND(SUM(monto), 0) FROM mis_ventas VERSION AS OF 0 WHERE monto > 400)
# MAGIC         AS suma_en_version_0,
# MAGIC     (SELECT ROUND(SUM(monto), 0) FROM mis_ventas WHERE monto > 400)
# MAGIC         AS suma_ahora

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Dos números distintos, de la misma tabla
# MAGIC
# MAGIC Eso es **time travel**, y acabas de resolver en segundos algo que normalmente es un
# MAGIC incidente. Piensa en lo que **no** tuviste que hacer: ningún backup, ninguna opción de
# MAGIC versionado activada, ningún almacenamiento extra, ningún permiso pedido.
# MAGIC
# MAGIC **Delta Lake lo hace por defecto.** Guarda solo lo que cambió, no una copia por versión.
# MAGIC
# MAGIC ### 🔷 Para qué se usa en la vida real
# MAGIC
# MAGIC | Situación | Cómo ayuda |
# MAGIC |---|---|
# MAGIC | Un proceso escribió datos mal y ya corrió | Vuelves a la versión anterior |
# MAGIC | *"Este reporte del mes pasado no me cuadra"* | Consultas los datos **como estaban** entonces |
# MAGIC | Un pronóstico dio un resultado extraño | Reproduces **exactamente** los datos con que se generó |
# MAGIC | Auditoría | Muestras el estado de una fecha específica |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🏆 Reto opcional — deshacer el cambio de verdad
# MAGIC
# MAGIC Consultar el pasado es una cosa; **volver** a él es otra. Si vas rápido, prueba en una
# MAGIC celda nueva:
# MAGIC
# MAGIC ```sql
# MAGIC RESTORE TABLE mis_ventas TO VERSION AS OF 0;
# MAGIC DESCRIBE HISTORY mis_ventas;
# MAGIC ```
# MAGIC
# MAGIC **Pregunta:** después del `RESTORE`, ¿en qué versión quedó la tabla?
# MAGIC
# MAGIC <details>
# MAGIC <summary>Ver respuesta</summary>
# MAGIC En la <b>versión 2</b>. El restore no borra el historial: <b>agrega</b> una versión nueva
# MAGIC igual a la 0. Delta nunca destruye historial, solo suma — que es justo lo que una
# MAGIC auditoría necesita ver.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · Explora: el asistente de IA y el catálogo
# MAGIC
# MAGIC ## 7.1 · Pídele algo al asistente de IA
# MAGIC El asistente integrado **conoce tus tablas**: sus nombres, columnas y tipos. Busca su
# MAGIC ícono (arriba a la derecha del notebook) y pídele:
# MAGIC
# MAGIC > *«Escribe una consulta que muestre el monto total de ventas por país, ordenado de mayor
# MAGIC > a menor, sobre la tabla mis_ventas»*
# MAGIC
# MAGIC Pega lo que te dé en la celda de abajo y ejecútalo.
# MAGIC
# MAGIC > ⚠️ **Lee lo que propone antes de ejecutarlo.** No siempre acierta, y en datos un error
# MAGIC > silencioso es peor que uno ruidoso. Revisar la respuesta de la IA es parte del trabajo.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pega acá el SQL que te dio el asistente y ejecútalo
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 · Encuentra tu tabla en el explorador
# MAGIC En el menú de la izquierda abre **Catalog** y navega:
# MAGIC
# MAGIC ```
# MAGIC <tu catálogo>  →  <tu schema>  →  mis_ventas
# MAGIC ```
# MAGIC
# MAGIC Mira las pestañas **Overview** (columnas y tipos) y **History** (el mismo historial que
# MAGIC viste con SQL). 👀 **Fíjate:** el volumen `raw` con los archivos JSON aparece **al lado**
# MAGIC de tu tabla, en el mismo árbol.
# MAGIC
# MAGIC ### 🔷 Por qué ese detalle importa
# MAGIC Unity Catalog gobierna **tablas y archivos con las mismas reglas**. En la mayoría de las
# MAGIC arquitecturas son dos mundos separados. Acá los permisos, el linaje y la auditoría son los
# MAGIC mismos para una tabla, un archivo, un modelo o un dashboard. **Un solo lugar donde definir
# MAGIC el gobierno.**

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 8 · Verifica tu checkpoint
# MAGIC
# MAGIC Esta celda comprueba sola que completaste el módulo. Ejecútala.

# COMMAND ----------

resultados = []

# 1 · la tabla existe y tiene datos
try:
    filas = spark.table("mis_ventas").count()
    resultados.append((filas > 0, f"Tu tabla existe y tiene {filas} filas"))
except Exception:
    resultados.append((False, "No encuentro la tabla mis_ventas — ¿hiciste el paso 3?"))
    filas = 0

# 2 · al menos 2 versiones: la creaste (v0) y la modificaste (v1)
try:
    versiones = spark.sql("DESCRIBE HISTORY mis_ventas").count()
    resultados.append((versiones >= 2,
                       f"Tiene {versiones} versión(es) — se necesitan al menos 2"))
except Exception:
    versiones = 0
    resultados.append((False, "No pude leer el historial de la tabla"))

# 3 · time travel devuelve algo distinto de lo actual
try:
    v0 = spark.sql("SELECT ROUND(SUM(monto),0) s FROM mis_ventas VERSION AS OF 0 "
                   "WHERE monto > 400").collect()[0][0]
    ahora = spark.sql("SELECT ROUND(SUM(monto),0) s FROM mis_ventas "
                      "WHERE monto > 400").collect()[0][0]
    resultados.append((True, f"Time travel funciona · v0: ${v0:,.0f} · ahora: ${ahora:,.0f}"))
    if versiones >= 2 and v0 == ahora:
        resultados.append((False, "Los dos montos son iguales — ¿ejecutaste el UPDATE del paso 5?"))
except Exception as e:
    resultados.append((False, f"Time travel falló: {str(e)[:60]}"))

print("=" * 70)
print("  MÓDULO 1 · CHECKPOINT")
print("=" * 70)
for ok, msg in resultados:
    print(f"  {'✅' if ok else '❌'}  {msg}")
print("=" * 70)

if all(ok for ok, _ in resultados):
    print("""
  🎉 ¡Módulo 1 completo!

  Lo que acabas de hacer:
    · Convertiste archivos crudos en una tabla consultable
    · Viste que Delta Lake registra cada cambio sin que se lo pidas
    · Recuperaste un dato que habías sobrescrito, con una consulta

  Ojo: esta tabla es de PRÁCTICA, no es tu capa bronze todavía.
  Son 1.000 filas de muestra, cargadas a mano.

  👉 En el Módulo 2 construyes la capa bronze de verdad: las ~585.000
     líneas de venta completas, con Auto Loader y carga incremental.
""")
else:
    print("""
  ⚠️  Falta algo. Revisa los ❌ de arriba.

  Lo más común: no ejecutaste el UPDATE del paso 5. Sin ese cambio la
  tabla tiene una sola versión, y no hay nada anterior que recuperar.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Lo que te llevas de este módulo
# MAGIC
# MAGIC | Concepto | Dónde lo viviste |
# MAGIC |---|---|
# MAGIC | **Delta Lake es el formato por defecto** | Hiciste `CREATE TABLE` sin pedir nada especial |
# MAGIC | **Cada cambio queda registrado** | `DESCRIBE HISTORY`, sin configurar nada |
# MAGIC | **Puedes volver atrás** | `VERSION AS OF` y `TIMESTAMP AS OF` |
# MAGIC | **Se puede actualizar y borrar filas** | El `UPDATE` sobre archivos en la nube |
# MAGIC | **Unity Catalog gobierna todo igual** | Tablas y archivos en el mismo árbol |
# MAGIC | **Python y SQL en el mismo notebook** | `%sql` cuando conviene |
# MAGIC
# MAGIC ## 🌍 Esto no es solo para retail
# MAGIC
# MAGIC | Industria | El mismo patrón, aplicado |
# MAGIC |---|---|
# MAGIC | **Banca** | Transacciones → detección de fraude |
# MAGIC | **Manufactura** | Sensores → detectar fallas antes de que ocurran |
# MAGIC | **Telecomunicaciones** | Uso y reclamos → anticipar deserción |
# MAGIC | **Salud** | Historias clínicas → riesgo de reingreso |
# MAGIC | **Energía** | Consumo histórico → pronóstico de demanda *(como hoy)* |
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC
# MAGIC ```
# MAGIC   archivos crudos  →  🥉 bronze  →  🥈 silver  →  🥇 gold
# MAGIC        (ya están)      (Módulo 2)   (Módulo 3)   (Módulo 3)
# MAGIC
# MAGIC   ✋ estás acá: hiciste una tabla de PRÁCTICA con 1.000 filas.
# MAGIC      Todavía no construiste bronze.
# MAGIC ```
# MAGIC
# MAGIC En el **Módulo 2** construyes bronze de verdad, con Auto Loader, y compruebas lo que lo
# MAGIC hace útil: cuando lleguen archivos nuevos, **no reprocesa los que ya leyó**.
