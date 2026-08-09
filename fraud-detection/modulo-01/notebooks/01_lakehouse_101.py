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
# MAGIC Un **notebook** es un documento donde se mezclan explicaciones y código. Está dividido
# MAGIC en **celdas**: las de texto (como esta) explican, y las de código se ejecutan.
# MAGIC
# MAGIC | Para hacer esto | Usa |
# MAGIC |---|---|
# MAGIC | Ejecutar la celda donde estás y pasar a la siguiente | `Shift + Enter` |
# MAGIC | Ejecutar sin avanzar | `Ctrl + Enter` |
# MAGIC
# MAGIC **Ve celda por celda, en orden.** No uses "Run all": la idea es que veas qué pasa en
# MAGIC cada paso, no que llegues al final rápido.
# MAGIC
# MAGIC Cada paso tiene tres partes: **qué vas a hacer**, **por qué importa** y **el código**.
# MAGIC Donde dice **📝 TU TURNO** te toca escribir algo — y justo debajo está la solución, por
# MAGIC si te trabas. Usarla no es trampa.
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
# MAGIC | 7 | Le pides ayuda al asistente de IA | Cómo se trabaja hoy en Databricks |
# MAGIC | 8 | Encuentras tu tabla en el explorador | Dónde vive todo lo que creas |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## El caso: detección de fraude en un banco
# MAGIC
# MAGIC Trabajas en el área de datos de un banco. Llegaron archivos con transacciones de
# MAGIC tarjetas y el equipo de riesgo necesita detectar las fraudulentas.
# MAGIC
# MAGIC Hoy empiezas por el principio: **convertir esos archivos en algo consultable y
# MAGIC confiable**.
# MAGIC
# MAGIC > 💡 **El caso es un banco, pero lo que vas a aprender no es bancario.**
# MAGIC > Ingerir archivos, versionar cambios, poder volver atrás y gobernar quién ve qué son
# MAGIC > problemas de **cualquier industria**. El mismo flujo sirve para detectar fallas en
# MAGIC > equipos industriales, anticipar deserción de clientes en telecomunicaciones, controlar
# MAGIC > inventario en retail o analizar historias clínicas en salud. Cambia el dato, no el
# MAGIC > método.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Importante: esto es material de aprendizaje
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | 🎓 **Objetivo** | Enseñar conceptos. Está optimizado para que se entienda, no para producción |
# MAGIC | 🔒 **Los datos son 100% sintéticos** | Generados por el notebook anterior. Ninguna persona ni institución real |
# MAGIC | 🚫 **No copies este código a producción tal cual** | Le falta manejo de errores, pruebas, control de costos, parametrización y CI/CD |
# MAGIC | ✅ **Los conceptos sí son reales** | Delta Lake, Unity Catalog y time travel son lo que usan las empresas de verdad |
# MAGIC
# MAGIC Un notebook para aprender y una implementación productiva se parecen poco. Este
# MAGIC muestra **el concepto en su forma más clara**; en producción ese mismo concepto viene
# MAGIC acompañado de pruebas automatizadas, control de versiones, monitoreo y despliegue
# MAGIC controlado.
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
# MAGIC    │           │          └── mis_transacciones
# MAGIC    │           └── fin_tu_usuario          (tu espacio personal)
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
# MAGIC
# MAGIC **Celda A** crea los dos campos y te muestra qué catálogos puedes usar.
# MAGIC **Celda B** los lee y comprueba que todo esté en su lugar.
# MAGIC
# MAGIC Van separadas a propósito: los campos **no existen** hasta que corres la celda A, así
# MAGIC que no podrías escribir nada en ellos antes.

# COMMAND ----------

# ═══ CELDA A · crear los campos ═══════════════════════════════════════════
# Ejecuta esta celda. Después mira los dos campos que aparecen ARRIBA del notebook,
# ajústalos si hace falta, y sigue con la celda B.

import re

# current_user() devuelve el correo con el que iniciaste sesión.
# Se usa para proponer un nombre de schema único tuyo, así nadie tiene que inventarse uno.
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
_schema_sugerido = "fin_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())

# se descartan los catálogos del sistema: no son para trabajar en ellos
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

# los widgets son los campos que aparecen arriba del notebook
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
# se recalcula acá para que esta celda sea autosuficiente (re-ejecutable sola)
usuario = spark.sql("SELECT current_user()").collect()[0][0]

# ── Comprobaciones. Este notebook NO crea nada: eso lo hizo 00_generar_datos.
# Si algo falta, es mejor decirlo acá con claridad que fallar raro tres celdas después.
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
    # se buscan schemas que parezcan del taller, para sugerirlos
    parecidos = [s for s in schemas if s.startswith("fin_")]
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

# USE fija el schema por defecto: a partir de acá puedes escribir 'mis_transacciones'
# en vez del nombre completo 'catalogo.schema.mis_transacciones'
spark.sql(f"USE `{catalogo}`.`{schema}`")

# Un VOLUME es el lugar donde Unity Catalog guarda ARCHIVOS (no tablas).
# Sirve para lo que llega crudo: JSON, CSV, imágenes, PDFs.
RAW = f"/Volumes/{catalogo}/{schema}/raw"

try:
    n = len([f for f in dbutils.fs.ls(f"{RAW}/transacciones") if f.name.endswith(".json")])
    if n == 0:
        raise Exception("sin archivos")
except Exception:
    raise Exception(
        f"\n❌ No encuentro los archivos en {RAW}/transacciones\n\n"
        f"   Corre primero el notebook 00_generar_datos con estos mismos valores de\n"
        f"   catálogo y schema.\n"
    )

print(f"👤 Tu usuario   : {usuario}")
print(f"📦 Tu catálogo  : {catalogo}")
print(f"📁 Tu schema    : {schema}")
print(f"🗂️  Tus archivos : {RAW}  ({n} archivos)")
print()
print("✅ Todo listo. Este es tu espacio: nadie más lo toca.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · ¿Qué llegó del banco?
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Mirar los archivos crudos **antes** de construir cualquier cosa con ellos.
# MAGIC
# MAGIC ## Por qué importa
# MAGIC Es el primer hábito de cualquier trabajo con datos: **nunca construyas sobre algo que
# MAGIC no viste**. Cuántos archivos hay, qué columnas traen, si vienen valores raros.
# MAGIC
# MAGIC Los datos llegaron como **archivos JSON**, igual que llegan en la vida real desde un
# MAGIC sistema de pagos: un proceso los deja ahí cada cierto tiempo y alguien tiene que
# MAGIC convertirlos en información útil. Ese alguien eres tú hoy.

# COMMAND ----------

# dbutils.fs es la utilidad para explorar archivos. ls = "list", como en la terminal.
archivos = [f for f in dbutils.fs.ls(f"{RAW}/transacciones") if f.name.endswith(".json")]

print(f"📁 {len(archivos)} archivos de transacciones\n")
for f in archivos[:3]:
    print(f"   {f.name[:40]}…  ({f.size / 1024 / 1024:.1f} MB)")
print(f"   … y {len(archivos) - 3} más")

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora **el contenido**. Fíjate en tres columnas, porque las vas a volver a ver:
# MAGIC
# MAGIC | Columna | Qué es | Cuándo la usarás |
# MAGIC |---|---|---|
# MAGIC | `numero_tarjeta` | Dato sensible (**PII**) | **Módulo 4** — vas a enmascararlo |
# MAGIC | `region` | Norte / centro / sur | **Módulo 4** — vas a filtrar por seguridad |
# MAGIC | `es_fraude` | Si la transacción fue fraude | **Módulo 6** — el modelo lo va a predecir |
# MAGIC
# MAGIC > 💡 **`display()` es de Databricks**, no de Python. Muestra los datos en una tabla
# MAGIC > navegable, y con el botón **+** de arriba puedes convertirla en gráfico sin escribir
# MAGIC > código. Pruébalo cuando tengas el resultado.

# COMMAND ----------

display(spark.read.json(f"{RAW}/transacciones").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · Tu primera tabla Delta
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Convertir esos archivos en una **tabla** que puedas consultar con SQL.
# MAGIC
# MAGIC Vas a usar solo **1.000 transacciones** de muestra, a propósito: así todo responde al
# MAGIC instante y puedes experimentar sin esperar.
# MAGIC
# MAGIC > ℹ️ **Esta es una tabla de práctica, no tu capa bronze.** La bronze — completa e
# MAGIC > incremental — la construyes en el Módulo 2. Acá el objetivo es entender qué es Delta
# MAGIC > Lake con una tabla pequeña y rápida.
# MAGIC
# MAGIC ## Por qué importa: qué es Delta Lake
# MAGIC
# MAGIC En Databricks, `CREATE TABLE` crea una **tabla Delta** por defecto. No hay que pedirlo.
# MAGIC Y eso trae cuatro cosas que un archivo suelto **no** tiene:
# MAGIC
# MAGIC | | Un archivo suelto (CSV, JSON) | Una tabla Delta |
# MAGIC |---|---|---|
# MAGIC | **Transacciones** | Si el proceso se cae a mitad, quedan datos corruptos | O todo se escribe, o nada. Nunca a medias |
# MAGIC | **Historial** | Se sobrescribe y el dato anterior se pierde | Cada cambio queda registrado |
# MAGIC | **Lecturas concurrentes** | Quien lee mientras alguien escribe puede ver basura | Cada lector ve una versión consistente |
# MAGIC | **Rendimiento** | Recorre todo el archivo siempre | Estadísticas que le permiten saltarse datos |
# MAGIC
# MAGIC ### 🔷 Por qué esto es un diferencial de Databricks
# MAGIC
# MAGIC Antes había que elegir: un **data warehouse** (rápido y confiable, pero cerrado y caro)
# MAGIC o un **data lake** (barato y flexible, pero sin garantías). Mantener los dos, con datos
# MAGIC copiándose entre ellos, era el estado normal de la industria.
# MAGIC
# MAGIC Delta Lake trajo las garantías del warehouse **a archivos en formato abierto**. Eso es
# MAGIC lo que hace posible el **lakehouse**: un solo lugar, sin copias.
# MAGIC
# MAGIC Y **Delta Lake es open source** — donado a la Linux Foundation en 2019. Tus datos
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
    CREATE OR REPLACE TABLE mis_transacciones AS
    SELECT
        transaccion_id,
        cliente_id,
        monto,
        comercio,
        categoria_comercio,
        canal,
        pais,
        region,
        fecha,
        es_fraude
    FROM FORMATO.`{RAW}/transacciones`
    LIMIT 1000
""")

print(f"✅ Tabla creada con {spark.table('mis_transacciones').count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC <details>
# MAGIC <summary>💡 <b>Solución del paso 3</b> — ábrela si te trabaste</summary>
# MAGIC
# MAGIC El formato es <code>json</code>. La línea queda:
# MAGIC
# MAGIC <pre>FROM json.`{RAW}/transacciones`</pre>
# MAGIC
# MAGIC Si prefieres no escribirlo, pon `USAR_SOLUCION = True` en la celda de abajo y ejecútala.
# MAGIC </details>

# COMMAND ----------

# --- Solución del paso 3 ---
# Si te trabaste, cambia USAR_SOLUCION a True y ejecuta esta celda.
USAR_SOLUCION = False

if USAR_SOLUCION:
    spark.sql(f"""
        CREATE OR REPLACE TABLE mis_transacciones AS
        SELECT transaccion_id, cliente_id, monto, comercio, categoria_comercio,
               canal, pais, region, fecha, es_fraude
        FROM json.`{RAW}/transacciones`
        LIMIT 1000
    """)
    print(f"✅ Tabla creada con {spark.table('mis_transacciones').count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mira tu tabla
# MAGIC
# MAGIC > 💡 **`%sql` al inicio de una celda** cambia el lenguaje de esa celda a SQL. En el
# MAGIC > mismo notebook puedes mezclar Python y SQL, usando cada uno donde conviene. Eso
# MAGIC > también es de Databricks: no tienes que elegir un solo lenguaje.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mis_transacciones LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿De verdad es una tabla Delta?
# MAGIC
# MAGIC No lo pediste en ningún momento. Compruébalo con `DESCRIBE EXTENDED`, que muestra
# MAGIC **todo lo que Databricks sabe** sobre tu tabla.
# MAGIC
# MAGIC Vas a ver unas 26 filas. **Baja hasta `# Detailed Table Information`**: ahí empieza lo
# MAGIC interesante. La siguiente celda te explica qué es cada cosa.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED mis_transacciones

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Qué acabas de ver, línea por línea
# MAGIC
# MAGIC El resultado tiene **dos partes**.
# MAGIC
# MAGIC ## Parte 1 · Arriba: las columnas y sus tipos
# MAGIC
# MAGIC Las primeras 10 filas son tus columnas con el tipo de dato que Databricks **infirió
# MAGIC solo**, leyendo los archivos JSON:
# MAGIC
# MAGIC | Columna | Tipo | Por qué ese |
# MAGIC |---|---|---|
# MAGIC | `transaccion_id`, `cliente_id` | `string` | Texto: tienen letras y números |
# MAGIC | `monto` | `double` | Número con decimales |
# MAGIC | `es_fraude` | `boolean` | Verdadero o falso |
# MAGIC | `fecha` | **`string`** | 👀 **Fíjate en esta** |
# MAGIC
# MAGIC ### 👀 `fecha` quedó como texto, no como fecha
# MAGIC
# MAGIC **Eso es un problema real, y es a propósito que lo veas ahora.**
# MAGIC
# MAGIC En JSON no existe el tipo "fecha": las fechas viajan como texto (`"2026-03-14 15:27:00"`).
# MAGIC Databricks leyó texto y guardó texto. No adivinó, y **hace bien en no adivinar**: si
# MAGIC interpretara mal el formato — ¿`03/14` es 14 de marzo o 3 de febrero? — corrompería tus
# MAGIC datos en silencio.
# MAGIC
# MAGIC La consecuencia práctica: mientras sea `string` **no puedes** preguntar *"transacciones
# MAGIC de la última semana"* ni *"agrupa por mes"*. Ordenar tampoco es confiable.
# MAGIC
# MAGIC 👉 **Déjala como está.** Convertir tipos y limpiar datos es el trabajo del **Módulo 3**.
# MAGIC Guardar el dato **tal como llegó del origen**, sin transformar, es una decisión
# MAGIC deliberada: si más adelante descubres que la limpieza estaba mal, todavía tienes el
# MAGIC original para rehacerla.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Parte 2 · Abajo: los metadatos de la tabla
# MAGIC
# MAGIC Después de `# Detailed Table Information`, las líneas que vale la pena entender:
# MAGIC
# MAGIC | Línea | Qué dice | Por qué importa |
# MAGIC |---|---|---|
# MAGIC | **`Provider`** | `delta` | ✅ **La respuesta a la pregunta.** Es Delta sin que lo pidieras |
# MAGIC | **`Catalog`** / **`Database`** / **`Table`** | tu catálogo, schema y tabla | Los tres niveles de Unity Catalog, ahora concretos |
# MAGIC | **`Type`** | `MANAGED` | Databricks administra los archivos por ti (ver abajo) |
# MAGIC | **`Location`** | una ruta `s3://…` o `abfss://…` | 👀 **Tus datos están en tu propia nube** |
# MAGIC | **`Owner`** | tu correo | Quién es dueño, base de los permisos del Módulo 4 |
# MAGIC | **`Statistics`** | *N* bytes, 1000 rows | Estadísticas que Delta guarda para saltarse datos al consultar |
# MAGIC | **`Created Time`** | fecha y hora | Cuándo se creó |
# MAGIC | **`Table Properties`** | `delta.*` | La configuración interna de Delta |
# MAGIC | **`Predictive Optimization`** | `ENABLE` | Databricks optimiza la tabla sola (ver abajo) |
# MAGIC
# MAGIC ### 🔷 Tres cosas de esa lista que son diferenciales reales
# MAGIC
# MAGIC **1 · `Location` apunta a tu propio almacenamiento en la nube**
# MAGIC
# MAGIC Tus datos **no están dentro de Databricks**: están en tu cuenta de S3, ADLS o GCS, en
# MAGIC formato Delta, que es abierto. Databricks administra y consulta, pero los archivos son
# MAGIC tuyos y otras herramientas pueden leerlos.
# MAGIC
# MAGIC Compáralo con un data warehouse tradicional, donde los datos viven en un formato
# MAGIC propietario al que solo ese producto entra. **Si mañana quisieras irte, podrías.** Esa es
# MAGIC la diferencia entre arquitectura abierta y quedar encerrado.
# MAGIC
# MAGIC **2 · `Type: MANAGED` — y qué sería lo contrario**
# MAGIC
# MAGIC | | `MANAGED` (lo que tienes) | `EXTERNAL` |
# MAGIC |---|---|---|
# MAGIC | Quién decide dónde van los archivos | Databricks | Tú, con una ruta explícita |
# MAGIC | Al hacer `DROP TABLE` | Borra los archivos también | Borra solo el registro; los archivos quedan |
# MAGIC | Optimización automática | Sí | Limitada |
# MAGIC
# MAGIC Para lo que creas dentro de la plataforma, `MANAGED` es lo recomendado: Databricks se
# MAGIC encarga del mantenimiento. `EXTERNAL` sirve cuando los archivos ya existían o los
# MAGIC comparten otros sistemas.
# MAGIC
# MAGIC **3 · `Predictive Optimization: ENABLE`**
# MAGIC
# MAGIC Con el tiempo una tabla se llena de archivos pequeños y se vuelve lenta. La solución
# MAGIC clásica es programar mantenimientos (`OPTIMIZE`, `VACUUM`) y acordarse de ajustarlos.
# MAGIC
# MAGIC Acá Databricks **observa cómo se consulta la tabla y la optimiza solo**, cuando conviene.
# MAGIC Es trabajo de ingeniería que no tienes que hacer — y que en otras plataformas se paga en
# MAGIC horas de alguien.
# MAGIC
# MAGIC > 💡 **Un hábito que vale adoptar:** cuando llegues a una tabla que no conoces, corre
# MAGIC > `DESCRIBE EXTENDED`. En 5 segundos sabes de qué tipo es, dónde vive, quién la creó,
# MAGIC > cuántas filas tiene y de quién es. Es la primera pregunta que hay que hacerle a una
# MAGIC > tabla ajena.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❓ ¿Esta tabla ya es mi capa bronze?
# MAGIC
# MAGIC **No todavía.** Y conviene tener esto claro para no perderse en los módulos siguientes.
# MAGIC
# MAGIC Databricks recomienda organizar los datos en **tres capas** — la llamada **arquitectura
# MAGIC medallion**:
# MAGIC
# MAGIC | Capa | Qué contiene | Cuándo la construyes |
# MAGIC |---|---|---|
# MAGIC | 🥉 **Bronze** | Todo el dato crudo, tal como llegó del origen | **Módulo 2** |
# MAGIC | 🥈 **Silver** | Limpio, con tipos correctos, sin duplicados ni filas inválidas | **Módulo 3** |
# MAGIC | 🥇 **Gold** | Agregado y listo para consumir: reportes, dashboards, modelos | **Módulo 3** |
# MAGIC
# MAGIC ### Entonces, ¿qué es lo que acabas de crear?
# MAGIC
# MAGIC Una **tabla de práctica**: 1.000 filas de muestra para aprender Delta Lake sin esperar.
# MAGIC No es bronze porque le faltan dos cosas que bronze sí tiene:
# MAGIC
# MAGIC | | Tu `mis_transacciones` | La bronze del Módulo 2 |
# MAGIC |---|---|---|
# MAGIC | **Filas** | 1.000 (una muestra con `LIMIT`) | **375.000** — todas |
# MAGIC | **Columnas** | 10 (dejaste fuera `numero_tarjeta`, `moneda`, `dispositivo_id`) | Todas las del origen |
# MAGIC | **Cómo se carga** | Una vez, a mano | **Incremental**: si llegan archivos nuevos, ingiere solo esos |
# MAGIC | **Para qué sirve** | Aprender los conceptos | Ser la base real de todo el resto del día |
# MAGIC
# MAGIC Esa tercera diferencia — **carga incremental** — es la importante. Un proceso que
# MAGIC reprocesa todo cada vez que llega un archivo nuevo no sirve en producción: cuesta más
# MAGIC cada día que pasa. En el Módulo 2 vas a usar **Auto Loader**, que lleva la cuenta de qué
# MAGIC archivos ya leyó y solo procesa los nuevos.
# MAGIC
# MAGIC > 💡 **Lo que sí llevas al Módulo 2** son los conceptos de este módulo: qué es una tabla
# MAGIC > Delta, que cada cambio queda versionado y que puedes volver atrás. Bronze será también
# MAGIC > una tabla Delta, con el mismo historial y el mismo time travel — solo que completa y
# MAGIC > cargada por un proceso repetible.
# MAGIC
# MAGIC ### 🔷 Por qué Databricks recomienda separar en capas
# MAGIC
# MAGIC Podría parecer trabajo de más: ¿por qué no limpiar los datos de una vez, al ingerirlos?
# MAGIC
# MAGIC Porque **la limpieza se hace con reglas, y las reglas cambian**. Si transformas al
# MAGIC ingerir y descartas el original, cuando descubras que una regla estaba mal ya no tienes
# MAGIC de dónde rehacerla — el dato crudo se perdió. Guardar bronze intacto cuesta almacenamiento
# MAGIC (que es barato) y te compra la posibilidad de reprocesar (que no tiene precio cuando algo
# MAGIC sale mal).
# MAGIC
# MAGIC > ℹ️ Es una **recomendación, no una regla**: la documentación oficial de Databricks lo
# MAGIC > plantea como buena práctica, no como requisito. Hay casos donde dos capas alcanzan.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · El historial de versiones
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Ver el registro de cambios de tu tabla.
# MAGIC
# MAGIC ## Por qué importa
# MAGIC Cada vez que una tabla Delta cambia, **el cambio queda anotado** en un registro llamado
# MAGIC *transaction log*. No configuraste nada para que eso pasara.
# MAGIC
# MAGIC Todavía no modificaste nada, así que deberías ver **una sola versión: la 0**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY mis_transacciones

# COMMAND ----------

# MAGIC %md
# MAGIC 👀 **Mira estas cuatro columnas del resultado:**
# MAGIC
# MAGIC | Columna | Qué dice |
# MAGIC |---|---|
# MAGIC | `version` | El número de versión. Ahora está en **0** |
# MAGIC | `timestamp` | Cuándo pasó |
# MAGIC | `operation` | Qué operación fue (`CREATE OR REPLACE TABLE AS SELECT`) |
# MAGIC | `userName` | **Quién** lo hizo |
# MAGIC
# MAGIC ### 🔷 Por qué esto importa más de lo que parece
# MAGIC
# MAGIC Eso es **auditoría**: quién cambió qué y cuándo. En muchas plataformas hay que
# MAGIC construirla — tablas de log, *triggers*, procesos que registren los cambios. Acá viene
# MAGIC por defecto.
# MAGIC
# MAGIC Para un banco, un hospital o cualquier empresa regulada, poder responder *"¿quién
# MAGIC modificó este dato y cuándo?"* no es una comodidad: es un **requisito de cumplimiento**.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 5 · Te equivocas a propósito
# MAGIC
# MAGIC ## La situación
# MAGIC
# MAGIC Llega un mensaje urgente del equipo de riesgo:
# MAGIC
# MAGIC > *"Detectamos un patrón. Marca como fraude todas las transacciones de más de un
# MAGIC > millón. Necesitamos el reporte en 10 minutos."*
# MAGIC
# MAGIC Lo haces. Es lo que pidieron.
# MAGIC
# MAGIC ## Paso 5.1 · Antes de cambiar nada, mide
# MAGIC
# MAGIC Vas a modificar datos. **Siempre cuenta primero**: sin un número de referencia, después
# MAGIC no puedes saber si tu cambio hizo lo que esperabas o algo distinto.
# MAGIC
# MAGIC Ejecuta las dos celdas de abajo y **anota los dos números** — los vas a comparar en un
# MAGIC minuto.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Cuántas transacciones están marcadas como fraude AHORA?
# MAGIC SELECT COUNT(*) AS fraudes_antes
# MAGIC FROM mis_transacciones
# MAGIC WHERE es_fraude = true

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Y cuántas transacciones pasan de un millón? Estas son las que vas a tocar.
# MAGIC SELECT
# MAGIC     COUNT(*)                                          AS pasan_de_un_millon,
# MAGIC     SUM(CASE WHEN es_fraude THEN 1 ELSE 0 END)        AS de_esas_ya_eran_fraude,
# MAGIC     SUM(CASE WHEN NOT es_fraude THEN 1 ELSE 0 END)    AS de_esas_van_a_cambiar
# MAGIC FROM mis_transacciones
# MAGIC WHERE monto > 1000000

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 TU TURNO · escribe el UPDATE
# MAGIC
# MAGIC ### Qué tienes que lograr
# MAGIC
# MAGIC Que las transacciones de **más de 1.000.000** queden marcadas como fraude. O sea: en
# MAGIC esas filas, la columna `es_fraude` debe pasar de `false` a `true`.
# MAGIC
# MAGIC ### Los tres datos que necesitas
# MAGIC
# MAGIC | | Valor | De dónde sale |
# MAGIC |---|---|---|
# MAGIC | **Qué tabla** | `mis_transacciones` | La que creaste en el paso 3 |
# MAGIC | **Qué columna cambiar, y a qué valor** | `es_fraude` → `true` | Lo que pide el equipo de riesgo |
# MAGIC | **En qué filas** | las que tengan `monto > 1000000` | El umbral del mensaje |
# MAGIC
# MAGIC ### La estructura de un `UPDATE`
# MAGIC
# MAGIC ```sql
# MAGIC UPDATE  <la tabla>
# MAGIC SET     <columna> = <valor nuevo>     -- qué cambiar
# MAGIC WHERE   <condición>                   -- en cuáles filas
# MAGIC ```
# MAGIC
# MAGIC Escríbelo en la celda de abajo, reemplazando las tres partes, y ejecútala.
# MAGIC
# MAGIC > ⚠️ **El `WHERE` no es opcional.** Un `UPDATE` sin `WHERE` cambia **todas** las filas de
# MAGIC > la tabla — en este caso marcaría las 1.000 transacciones como fraude. Es uno de los
# MAGIC > errores más clásicos y más caros en bases de datos. Acá no pasa nada grave porque tus
# MAGIC > datos son sintéticos y además tienes time travel, pero vale tomarse el hábito de
# MAGIC > escribir el `WHERE` primero.
# MAGIC
# MAGIC ### Cómo saber si funcionó
# MAGIC
# MAGIC Databricks te devuelve **`num_affected_rows`**: cuántas filas tocó el `UPDATE`.
# MAGIC
# MAGIC Debe coincidir con **`pasan_de_un_millon`** de la celda anterior (unas 29 filas), **no**
# MAGIC con `de_esas_van_a_cambiar`. Y eso tiene una explicación que vale entender:
# MAGIC
# MAGIC > `UPDATE` toca **todas** las filas que cumplen el `WHERE`, sin revisar si el valor ya
# MAGIC > era el que le estás poniendo. Las 4 filas que ya tenían `es_fraude = true` se vuelven a
# MAGIC > escribir con `true`. El resultado final es correcto, pero el contador incluye trabajo
# MAGIC > que no hacía falta.
# MAGIC
# MAGIC Con 29 filas no importa. Con millones sí: por eso en producción se suele agregar la
# MAGIC condición al `WHERE` para tocar solo lo que de verdad cambia — `WHERE monto > 1000000 AND
# MAGIC es_fraude = false`. Menos filas reescritas, menos tiempo, menos costo.

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
# MAGIC <pre>UPDATE mis_transacciones
# MAGIC SET es_fraude = true
# MAGIC WHERE monto > 1000000</pre>
# MAGIC
# MAGIC Debería reportar <b>num_affected_rows ≈ 29</b>.
# MAGIC <br><br>
# MAGIC Nota que <code>1000000</code> se escribe <b>sin puntos ni comas</b>. En SQL los números
# MAGIC no llevan separador de miles: <code>1.000.000</code> daría error porque el punto es
# MAGIC decimal.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔷 Lo que acabas de hacer es menos trivial de lo que parece
# MAGIC
# MAGIC Acabas de actualizar filas sueltas de datos que viven como **archivos en almacenamiento
# MAGIC de objetos** en la nube (lo viste en el `Location` del paso 3).
# MAGIC
# MAGIC **Antes de Delta Lake eso no se podía hacer.** El almacenamiento de objetos no permite
# MAGIC modificar parte de un archivo: para cambiar 29 filas había que **reescribir el conjunto
# MAGIC completo** — leer todo, cambiar lo poco que hacía falta, escribir todo de nuevo. Con
# MAGIC tablas grandes eso es horas de proceso, y si algo falla a mitad quedan datos corruptos.
# MAGIC
# MAGIC Por eso los data lakes eran, en la práctica, de solo lectura. Y por eso hacían falta dos
# MAGIC sistemas: el lake para guardar mucho y barato, el warehouse para poder modificar.
# MAGIC
# MAGIC ### Por qué esto tiene consecuencias legales, no solo técnicas
# MAGIC
# MAGIC Las leyes de privacidad — GDPR en Europa, la Ley 1581 en Colombia — le dan a las
# MAGIC personas el derecho a pedir que **borren sus datos**. Para cumplirlo hay que poder
# MAGIC ejecutar un `DELETE` sobre filas específicas.
# MAGIC
# MAGIC Sin la capacidad de modificar filas sueltas, la respuesta a *"bórrenme del sistema"* era
# MAGIC reescribir petabytes o no cumplir. **`UPDATE` y `DELETE` sobre formato abierto** es lo
# MAGIC que hizo viable tener los datos en un lake y aun así cumplir la regulación.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cuenta de nuevo — ahora hay más fraudes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS fraudes_ahora
# MAGIC FROM mis_transacciones
# MAGIC WHERE es_fraude = true

# COMMAND ----------

# MAGIC %md
# MAGIC ### Y mira el historial otra vez
# MAGIC
# MAGIC **Ahora hay una versión 1.** La tabla cambió — pero fíjate en que **la versión 0 sigue
# MAGIC ahí**. No desapareció.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY mis_transacciones

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · Time travel ⭐
# MAGIC ## El momento clave del módulo
# MAGIC
# MAGIC ## La situación, continuación
# MAGIC
# MAGIC Vuelve el equipo de riesgo:
# MAGIC
# MAGIC > *"Nos equivocamos. El umbral era otro y ya mandamos tu reporte al comité.
# MAGIC > ¿Puedes decirnos cómo estaba la tabla antes del cambio?"*
# MAGIC
# MAGIC **Detente un segundo y piensa cómo responderías** eso en una base de datos tradicional,
# MAGIC o con archivos CSV en un servidor.
# MAGIC
# MAGIC La respuesta honesta sería *"tendría que pedir que restauren un backup"* — si es que hay
# MAGIC backup de hace 10 minutos, si alguien tiene permiso, y si se puede restaurar sin tumbar
# MAGIC lo demás. Horas, en el mejor caso.
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Responder con **una consulta**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- La misma tabla, consultada en dos momentos distintos, en una sola consulta
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM mis_transacciones VERSION AS OF 0 WHERE es_fraude = true)
# MAGIC         AS fraudes_en_version_0,
# MAGIC     (SELECT COUNT(*) FROM mis_transacciones WHERE es_fraude = true)
# MAGIC         AS fraudes_ahora

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Dos números distintos, de la misma tabla
# MAGIC
# MAGIC Eso es **time travel**, y acabas de resolver en segundos algo que normalmente es un
# MAGIC incidente.
# MAGIC
# MAGIC Piensa en lo que **no** tuviste que hacer:
# MAGIC
# MAGIC - no hiciste un backup
# MAGIC - no activaste ninguna opción de versionado
# MAGIC - no pagaste almacenamiento extra por guardar copias
# MAGIC - no pediste permisos a nadie
# MAGIC
# MAGIC **Delta Lake lo hace por defecto.** Guarda solo lo que cambió, no una copia completa de
# MAGIC la tabla por cada versión.
# MAGIC
# MAGIC ### 🔷 Para qué se usa esto en la vida real
# MAGIC
# MAGIC | Situación | Cómo ayuda |
# MAGIC |---|---|
# MAGIC | Un proceso escribió datos mal y ya corrió | Vuelves a la versión anterior |
# MAGIC | *"Este reporte del mes pasado no me cuadra"* | Consultas los datos **como estaban** entonces |
# MAGIC | Un modelo de ML dio un resultado extraño | Reproduces **exactamente** los datos con que se entrenó |
# MAGIC | Auditoría regulatoria | Muestras el estado de una fecha específica |
# MAGIC
# MAGIC Ese tercer caso — **reproducibilidad** — es de los más valiosos. Sin él, "¿por qué el
# MAGIC modelo predijo esto en marzo?" es una pregunta sin respuesta.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Puedes consultar cualquier versión, no solo contar
# MAGIC
# MAGIC Acá están las transacciones grandes **como estaban antes** de tu cambio. Mira la columna
# MAGIC `es_fraude`: en la versión 0 muchas estaban en `false`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT transaccion_id, monto, es_fraude
# MAGIC FROM mis_transacciones VERSION AS OF 0
# MAGIC WHERE monto > 1000000
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### También por fecha, no solo por número de versión
# MAGIC
# MAGIC `VERSION AS OF 0` sirve cuando sabes el número. Pero la pregunta real suele ser
# MAGIC *"¿cómo estaba esto ayer a las 6 de la tarde?"*. Para eso está `TIMESTAMP AS OF`.
# MAGIC
# MAGIC La celda de abajo busca la fecha exacta de tu versión 0 y consulta con ella.
# MAGIC
# MAGIC > 💡 **Solo puedes viajar a un momento en que la tabla ya existía.** Si pides una fecha
# MAGIC > anterior a su creación, Delta te devuelve un error claro en vez de datos inventados —
# MAGIC > que es exactamente el comportamiento que quieres de un sistema de datos.

# COMMAND ----------

# DESCRIBE HISTORY se puede consultar como si fuera una tabla. Acá se saca el timestamp
# real de la versión 0 y se usa en la consulta siguiente.
ts_v0 = spark.sql("""
    SELECT timestamp FROM (DESCRIBE HISTORY mis_transacciones) WHERE version = 0
""").collect()[0][0]

print(f"🕐 Tu versión 0 se creó a las: {ts_v0}\n")

display(spark.sql(f"""
    SELECT COUNT(*) AS filas,
           SUM(CASE WHEN es_fraude THEN 1 ELSE 0 END) AS fraudes
    FROM mis_transacciones TIMESTAMP AS OF '{ts_v0}'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🏆 Reto opcional — deshacer el cambio de verdad
# MAGIC
# MAGIC Consultar el pasado es una cosa; **volver** a él es otra. Si vas rápido, prueba esto en
# MAGIC una celda nueva:
# MAGIC
# MAGIC ```sql
# MAGIC RESTORE TABLE mis_transacciones TO VERSION AS OF 0;
# MAGIC DESCRIBE HISTORY mis_transacciones;
# MAGIC ```
# MAGIC
# MAGIC **Pregunta:** después del `RESTORE`, ¿en qué versión quedó la tabla?
# MAGIC
# MAGIC <details>
# MAGIC <summary>Ver respuesta</summary>
# MAGIC
# MAGIC En la <b>versión 2</b>. El restore no borra el historial: <b>agrega</b> una versión
# MAGIC nueva cuyo contenido es igual al de la 0.
# MAGIC <br><br>
# MAGIC Delta nunca destruye historial, solo suma. Así queda registrado que hubo un cambio
# MAGIC <i>y</i> que después se deshizo — que es justamente lo que una auditoría necesita ver.
# MAGIC </details>
# MAGIC
# MAGIC > ⚠️ Si haces el restore, el checkpoint del paso 9 seguirá funcionando: compara la
# MAGIC > versión 0 con el estado actual, y el historial ya tiene más de una versión.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · El asistente de IA
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Pedirle a la IA integrada en Databricks que te explique código y que escriba SQL por ti.
# MAGIC
# MAGIC ## Por qué importa
# MAGIC El asistente **conoce tus tablas**: sus nombres, columnas y tipos. No es un chat genérico
# MAGIC al que hay que explicarle todo — sabe en qué esquema estás trabajando.
# MAGIC
# MAGIC ## Qué hacer
# MAGIC Busca el ícono del **asistente** (arriba a la derecha del notebook) y pídele dos cosas:
# MAGIC
# MAGIC **1 · Que te explique algo**
# MAGIC
# MAGIC > *«Explícame qué hace esta consulta: SELECT COUNT(\*) FROM mis_transacciones VERSION AS
# MAGIC > OF 0»*
# MAGIC
# MAGIC **2 · Que escriba SQL por ti**
# MAGIC
# MAGIC > *«Escribe una consulta que cuente transacciones y monto total por categoría de
# MAGIC > comercio, ordenado de mayor a menor, sobre la tabla mis_transacciones»*
# MAGIC
# MAGIC Pega lo que te dé en la celda de abajo y ejecútalo.
# MAGIC
# MAGIC > ⚠️ **Lee lo que te propone antes de ejecutarlo.** No siempre acierta, y en datos un
# MAGIC > error silencioso es peor que un error ruidoso. Revisar la respuesta de la IA es parte
# MAGIC > del trabajo, no un paso opcional.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pega acá el SQL que te dio el asistente y ejecútalo
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 8 · Encuentra tu tabla en el explorador
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Ver tu tabla desde la interfaz gráfica, no desde código.
# MAGIC
# MAGIC ## Qué hacer
# MAGIC En el menú de la izquierda abre **Catalog** y navega:
# MAGIC
# MAGIC ```
# MAGIC <tu catálogo>  →  <tu schema>  →  mis_transacciones
# MAGIC ```
# MAGIC
# MAGIC Esa es la jerarquía de **Unity Catalog** del paso 1, ahora visible.
# MAGIC
# MAGIC ### Mira estas tres pestañas
# MAGIC
# MAGIC | Pestaña | Qué vas a ver | Cuándo la usarás |
# MAGIC |---|---|---|
# MAGIC | **Overview** | Las columnas y sus tipos | Siempre |
# MAGIC | **Permissions** | Quién tiene acceso | **Módulo 4** |
# MAGIC | **History** | El mismo historial que viste con SQL | Ahora |
# MAGIC
# MAGIC 👀 **Fíjate en un detalle:** el volumen `raw` con los archivos JSON aparece **al lado**
# MAGIC de tu tabla, en el mismo árbol.
# MAGIC
# MAGIC ### 🔷 Por qué ese detalle importa
# MAGIC
# MAGIC Unity Catalog gobierna **tablas y archivos con las mismas reglas**. En la mayoría de las
# MAGIC arquitecturas son dos mundos separados: permisos de base de datos por un lado, permisos
# MAGIC de almacenamiento por otro, y nadie tiene la vista completa de quién puede ver qué.
# MAGIC
# MAGIC Acá los permisos, el linaje y la auditoría son los mismos para una tabla, un archivo, un
# MAGIC modelo de ML o un dashboard. **Un solo lugar donde definir el gobierno.**

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 9 · Verifica tu checkpoint
# MAGIC
# MAGIC Esta celda comprueba sola que completaste el módulo. Ejecútala.

# COMMAND ----------

resultados = []

# 1 · la tabla existe y tiene datos
try:
    filas = spark.table("mis_transacciones").count()
    resultados.append((filas > 0, f"Tu tabla existe y tiene {filas} filas"))
except Exception as e:
    resultados.append((False, f"No encuentro la tabla mis_transacciones — ¿hiciste el paso 3?"))
    filas = 0

# 2 · tiene al menos 2 versiones: la creaste (v0) y la modificaste (v1)
try:
    versiones = spark.sql("DESCRIBE HISTORY mis_transacciones").count()
    resultados.append((versiones >= 2,
                       f"Tiene {versiones} versión(es) en el historial — se necesitan al menos 2"))
except Exception:
    versiones = 0
    resultados.append((False, "No pude leer el historial de la tabla"))

# 3 · time travel devuelve algo distinto de lo actual
try:
    v0 = spark.sql("SELECT COUNT(*) c FROM mis_transacciones VERSION AS OF 0 "
                   "WHERE es_fraude = true").collect()[0][0]
    ahora = spark.sql("SELECT COUNT(*) c FROM mis_transacciones "
                      "WHERE es_fraude = true").collect()[0][0]
    resultados.append((True, f"Time travel funciona · versión 0: {v0} fraudes · "
                             f"ahora: {ahora} fraudes"))
    if versiones >= 2 and v0 == ahora:
        resultados.append((False,
            "Los dos conteos son iguales — ¿ejecutaste el UPDATE del paso 5?"))
except Exception as e:
    resultados.append((False, f"Time travel falló: {str(e)[:60]}"))

# ---- resultado ----
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

  Eso último — time travel — es lo que separa una tabla Delta de un
  archivo cualquiera. Y no configuraste nada para tenerlo.

  Ojo: esta tabla es de PRÁCTICA, no es tu capa bronze todavía.
  Son 1.000 filas de muestra, cargadas a mano.

  👉 En el Módulo 2 construyes la capa bronze de verdad: las 375.000
     transacciones completas, con Auto Loader y carga incremental.
""")
else:
    print("""
  ⚠️  Falta algo. Revisa los ❌ de arriba.

  Lo más común: no ejecutaste el UPDATE del paso 5. Sin ese cambio la
  tabla tiene una sola versión, y no hay nada anterior que recuperar.

  Si no logras resolverlo, levanta la mano.
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
# MAGIC | **El historial es auditoría** | Quién, qué y cuándo, gratis |
# MAGIC | **Se puede actualizar y borrar filas** | El `UPDATE` sobre archivos en la nube |
# MAGIC | **Unity Catalog gobierna todo igual** | Tablas y archivos en el mismo árbol |
# MAGIC | **Python y SQL en el mismo notebook** | `%sql` cuando conviene |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌍 Esto no es solo para bancos
# MAGIC
# MAGIC Cambia el dato y el mismo flujo resuelve otro problema:
# MAGIC
# MAGIC | Industria | El mismo patrón, aplicado |
# MAGIC |---|---|
# MAGIC | **Manufactura** | Lecturas de sensores → detectar fallas antes de que ocurran |
# MAGIC | **Retail** | Ventas y stock → pronóstico de demanda |
# MAGIC | **Telecomunicaciones** | Uso y reclamos → anticipar deserción de clientes |
# MAGIC | **Salud** | Historias clínicas → riesgo de reingreso |
# MAGIC | **Logística** | Rastreo de envíos → predecir retrasos |
# MAGIC | **Seguros** | Reclamaciones → detección de fraude *(igual que hoy)* |
# MAGIC
# MAGIC Y el **time travel** resuelve lo mismo en todas: *"¿cómo estaban los datos cuando se
# MAGIC tomó esta decisión?"*.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚠️ Recordatorio antes de seguir
# MAGIC
# MAGIC Este notebook es **material de aprendizaje**. Para llevar algo así a producción falta:
# MAGIC
# MAGIC | | En este notebook | En producción |
# MAGIC |---|---|---|
# MAGIC | **Errores** | Se ven en pantalla | Se capturan, registran y alertan |
# MAGIC | **Pruebas** | Ninguna | Pruebas de datos y de código, automatizadas |
# MAGIC | **Despliegue** | Se ejecuta a mano | Control de versiones en Git y CI/CD |
# MAGIC | **Configuración** | Valores escritos en el código | Parametrizada por ambiente |
# MAGIC | **Datos** | Sintéticos | Reales, con reglas de calidad y privacidad |
# MAGIC | **Costos** | Sin control | Presupuestos y monitoreo |
# MAGIC
# MAGIC **Los conceptos sí se trasladan tal cual.** Delta Lake, time travel y Unity Catalog son
# MAGIC exactamente lo que usan las empresas en producción — lo que cambia es la ingeniería
# MAGIC alrededor.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC
# MAGIC ### Dónde estás en la arquitectura medallion
# MAGIC
# MAGIC ```
# MAGIC   archivos crudos  →  🥉 bronze  →  🥈 silver  →  🥇 gold
# MAGIC        (ya están)      (Módulo 2)   (Módulo 3)   (Módulo 3)
# MAGIC
# MAGIC   ✋ estás acá: hiciste una tabla de PRÁCTICA con 1.000 filas
# MAGIC      para aprender Delta Lake. Todavía no construiste bronze.
# MAGIC ```
# MAGIC
# MAGIC ### Módulo 2 · ahí sí construyes bronze
# MAGIC
# MAGIC | | Tu tabla de práctica | La bronze del Módulo 2 |
# MAGIC |---|---|---|
# MAGIC | Filas | 1.000 | **375.000** |
# MAGIC | Columnas | 10 | **las 13** del origen |
# MAGIC | Carga | manual, una vez | **incremental**, con Auto Loader |
# MAGIC
# MAGIC Y vas a comprobar lo que hace útil a Auto Loader: cuando lleguen archivos nuevos,
# MAGIC **no reprocesa los que ya leyó**.
# MAGIC
# MAGIC Todo lo que aprendiste hoy sigue aplicando: bronze también será una tabla Delta, con su
# MAGIC historial de versiones y su time travel.
