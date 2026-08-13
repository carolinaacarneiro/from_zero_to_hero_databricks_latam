# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # ✅ Verificación de ambiente
# MAGIC
# MAGIC ### ¡Hola! Y gracias por acompañarnos en el taller.
# MAGIC
# MAGIC Este es el **notebook de verificación**: su único trabajo es confirmar que tu ambiente
# MAGIC de Databricks está listo para el día del entrenamiento.
# MAGIC
# MAGIC Durante el taller vas a construir un proyecto de datos e IA **de punta a punta**:
# MAGIC ingesta, transformación, gobierno, machine learning y una capa de consumo con
# MAGIC dashboard, Genie y una app. Cada uno de esos pasos necesita ciertos permisos y
# MAGIC funcionalidades en tu workspace.
# MAGIC
# MAGIC Este notebook los prueba **uno por uno**, en el mismo orden en que los vas a usar, y
# MAGIC te dice exactamente qué hacer si algo falta.
# MAGIC
# MAGIC > 🔒 **No modifica ni borra nada tuyo.** Crea unos objetos mínimos de prueba
# MAGIC > (una tabla, un volumen, un dashboard vacío, un espacio Genie vacío) y **los elimina
# MAGIC > al terminar**. Todo lo de prueba lleva un prefijo (`_verificacion`, `_z2h_probe`).
# MAGIC > **Tu schema sí queda creado** — es tu espacio de trabajo para todo el taller.
# MAGIC >
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Los notebooks del taller
# MAGIC > enseñan conceptos con datos 100% sintéticos; no están pensados para copiarse a
# MAGIC > producción tal cual (les falta manejo de errores, pruebas, control de costos y CI/CD).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📍 ¿Dónde debo correr este notebook?
# MAGIC
# MAGIC **En el mismo workspace que vas a usar durante el taller.** Verificar en un lugar y
# MAGIC trabajar en otro no sirve de nada.
# MAGIC
# MAGIC ### Opción 1 — Tu propio workspace ⭐ *la mejor opción*
# MAGIC
# MAGIC Si tu empresa ya usa Databricks, **es el mejor escenario**: trabajas en tu ambiente
# MAGIC real, con tus propios datos y permisos, y lo que aprendas se aplica directo el lunes.
# MAGIC
# MAGIC > ⚠️ **Por favor usa un workspace de desarrollo, de pruebas o uno dedicado al
# MAGIC > entrenamiento.**
# MAGIC >
# MAGIC > **Nunca uses un workspace de producción.** Durante el taller vas a crear schemas,
# MAGIC > tablas, pipelines, jobs y modelos, y vas a aplicar políticas de gobierno. Nada de
# MAGIC > eso debería tocar un ambiente productivo.
# MAGIC
# MAGIC Necesitas permisos para crear **schemas, tablas, pipelines y jobs**. Si no estás
# MAGIC seguro, corre este notebook: te lo dice en 2 minutos.
# MAGIC
# MAGIC ### Opción 2 — Databricks Trial *(si aún no tienes cuenta)*
# MAGIC
# MAGIC Si tu empresa todavía no usa Databricks, o no consigues acceso a un workspace de
# MAGIC desarrollo, crea una cuenta de prueba:
# MAGIC
# MAGIC ### 👉 https://www.databricks.com/try-databricks
# MAGIC
# MAGIC Es **gratuita por 14 días**, no necesita tarjeta de crédito, y **todos los módulos del
# MAGIC taller funcionan completos**. Es la opción que recomendamos si no tienes workspace
# MAGIC propio.
# MAGIC
# MAGIC > 💡 Créala **cerca de la fecha del taller** para que los 14 días cubran el día del
# MAGIC > evento.
# MAGIC
# MAGIC ### Opción 3 — Databricks Free Edition *(última alternativa)*
# MAGIC
# MAGIC En el mismo enlace también existe la **Free Edition**: gratuita y sin vencimiento,
# MAGIC pensada para *"estudiantes, educadores, hobbyistas y cualquiera que quiera aprender o
# MAGIC experimentar con datos e IA"*.
# MAGIC
# MAGIC Tiene algunas limitaciones oficiales:
# MAGIC
# MAGIC | Limitación oficial |
# MAGIC |---|
# MAGIC | Solo **compute serverless** (sin configuración propia) |
# MAGIC | **1 SQL warehouse**, tamaño `2X-Small` |
# MAGIC | **Máx. 5 tareas de job concurrentes** por cuenta |
# MAGIC | **Hasta 3 apps**, que se apagan tras 24 h |
# MAGIC | 1 workspace y 1 metastore por cuenta |
# MAGIC
# MAGIC > ⚠️ **Importante:** la documentación oficial dice que *"las cuentas Free Edition no
# MAGIC > pueden usarse para propósitos comerciales"*. Si vas a trabajar con datos o casos de
# MAGIC > tu empresa, usa un **trial** o el **workspace de tu organización**, no la Free
# MAGIC > Edition.
# MAGIC
# MAGIC 📄 Detalle oficial:
# MAGIC https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
# MAGIC
# MAGIC **Corre este notebook para verificar qué permisos y funcionalidades tienes
# MAGIC disponibles en tu cuenta.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 IMPORTANTE — aquí decides DÓNDE vas a trabajar todo el día
# MAGIC
# MAGIC Arriba de este notebook hay **dos campos** (widgets) que tienes que llenar **antes de
# MAGIC correr nada**:
# MAGIC
# MAGIC | Campo | Qué es | Ejemplo |
# MAGIC |---|---|---|
# MAGIC | **1 · Catálogo** | El catálogo de Unity Catalog donde vas a trabajar. En **Free Edition** casi siempre es `workspace`. En un **workspace corporativo**, alínealo internamente con tu equipo: puedes **usar un catálogo que ya exista** (muchas veces es el mejor camino) o **crear uno nuevo** dedicado al taller. **Crear un catálogo NO es obligatorio.** | `workspace` |
# MAGIC | **2 · Schema** | El nombre de **tu** espacio de trabajo, dentro de ese catálogo. Lo eliges tú, con el nombre que quieras (sin espacios ni acentos). | `z2h_ana` |
# MAGIC
# MAGIC ### 🔑 Esta decisión te acompaña TODO el taller
# MAGIC
# MAGIC El catálogo y el schema que escribas aquí **son los mismos que vas a usar en cada uno
# MAGIC de los 7 módulos**. Todo lo que construyas durante el día —tablas, pipeline, modelo,
# MAGIC dashboard— va a vivir en `catálogo.schema`. Por eso:
# MAGIC
# MAGIC > ⭐ **Escríbelos con calma, y ANÓTALOS** (en un papel, en tus notas, donde sea).
# MAGIC > Cada módulo te va a pedir estos dos nombres otra vez, y tienes que escribir
# MAGIC > **exactamente los mismos**. La causa #1 de errores en el taller es escribir un
# MAGIC > nombre distinto en cada módulo — así que aquí lo defines una vez y lo repites igual.
# MAGIC
# MAGIC No se infiere ni se adivina nada a partir de tu correo: **tú tienes el control** de
# MAGIC dónde trabajas.
# MAGIC
# MAGIC > 💡 ¿Los widgets no aparecen arriba? Corre la **celda del Paso 0** una vez: los crea
# MAGIC > automáticamente. Luego llénalos y haz `Run all`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ▶️ Cómo usarlo
# MAGIC
# MAGIC 1. **Conecta el notebook a un compute** — arriba a la derecha. Si no tienes uno,
# MAGIC    créalo (serverless sirve perfecto).
# MAGIC 2. **Corre la celda del `Paso 0`** para que aparezcan los dos campos arriba, y
# MAGIC    **llénalos**: `Catálogo` y `Schema`. (Ver la sección anterior — es la decisión
# MAGIC    más importante de este notebook.)
# MAGIC 3. **Menú `Run` → `Run all`** y espera. Cada paso imprime ✅, ⚠️ o ❌.
# MAGIC 4. Al final aparece un **resumen** con tu veredicto:
# MAGIC
# MAGIC | | Significa |
# MAGIC |---|---|
# MAGIC | 🟢 **Listo** | Tienes todo. No hay nada que hacer. |
# MAGIC | 🟡 **Listo con limitaciones** | Puedes participar; algunos ejercicios tendrán alcance reducido. |
# MAGIC | 🔴 **Falta algo** | Hay que resolver un permiso antes del taller. |
# MAGIC
# MAGIC 5. **Copia el bloque de resumen y respóndenos el correo con eso pegado** — incluso si
# MAGIC    todo sale en verde. Así sabemos que estás listo.
# MAGIC
# MAGIC ### ¿Por qué te pedimos esto?
# MAGIC
# MAGIC Porque el taller es **acumulativo**: lo que construyes en un módulo lo usas en el
# MAGIC siguiente. Un problema de permisos descubierto a la 1 de la tarde te saca del ejercicio
# MAGIC por el resto del día. Descubierto hoy, lo resolvemos con calma.
# MAGIC
# MAGIC > **¿Algo falla y no sabes cómo resolverlo?** Respóndenos el correo con el resumen
# MAGIC > pegado y te ayudamos a encontrar el camino: puede ser un permiso que le pidas a tu
# MAGIC > administrador, o crear una cuenta de prueba. Lo importante es **resolverlo antes del
# MAGIC > taller**, no el mismo día.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0 · Define tu catálogo y tu schema
# MAGIC
# MAGIC **Corre esta celda primero.** Crea los dos campos (widgets) arriba del notebook.
# MAGIC Luego **escribe en ellos tu catálogo y tu schema**, y sigue con el resto (`Run all`).
# MAGIC
# MAGIC Estos dos nombres son los que vas a usar durante **todo** el taller — anótalos y
# MAGIC repítelos idénticos en cada módulo. **No se infiere ni se adivina nada** a partir de tu
# MAGIC correo: tú eliges dónde trabajas.

# COMMAND ----------

# ── Crea los widgets de catálogo y schema (solo definición; escribe los valores arriba) ──
dbutils.widgets.text("catalogo", "", "1 · Catálogo (escríbelo)")
dbutils.widgets.text("schema",   "", "2 · Schema (escríbelo)")

print("✅ Widgets listos arriba. Escribe tu CATÁLOGO y tu SCHEMA, y luego haz 'Run all'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 · ¿Quién eres y hay compute?
# MAGIC
# MAGIC Identifica tu usuario y comprueba que el notebook puede ejecutar código. Si este
# MAGIC paso falla, es porque **no hay un compute conectado** — mira arriba a la derecha.
# MAGIC
# MAGIC También lee y valida el **catálogo** y el **schema** que escribiste en los widgets del
# MAGIC Paso 0. Esos dos nombres son los que usarás durante **todo** el taller — anótalos y
# MAGIC repítelos idénticos en cada módulo.

# COMMAND ----------

import re
from datetime import datetime, timezone

# Lee los valores que escribiste en los widgets del Paso 0.
CATALOGO = dbutils.widgets.get("catalogo").strip()
SCHEMA   = dbutils.widgets.get("schema").strip()

resultados = []   # (clave, etiqueta, estado, detalle, arreglo)
OK, PARCIAL, FALLA, NA = "OK", "PARCIAL", "FALLA", "N/D"


def registrar(clave, etiqueta, estado, detalle="", arreglo=""):
    resultados.append(
        {"clave": clave, "etiqueta": etiqueta, "estado": estado,
         "detalle": str(detalle)[:300], "arreglo": arreglo}
    )
    icono = {OK: "✅", PARCIAL: "⚠️", FALLA: "❌", NA: "➖"}[estado]
    print(f"{icono}  {etiqueta}")
    if detalle:
        print(f"     {str(detalle)[:200]}")


def probar(clave, etiqueta, fn, arreglo="", critico=True):
    """Ejecuta una comprobación y registra el resultado sin abortar el notebook."""
    try:
        salida = fn()
        if salida is False:
            registrar(clave, etiqueta, FALLA if critico else PARCIAL, "", arreglo)
        else:
            registrar(clave, etiqueta, OK, "" if salida is True else salida)
        return salida
    except Exception as e:
        msg = str(e).split("\n")[0]
        registrar(clave, etiqueta, FALLA if critico else PARCIAL, msg, arreglo)
        return None


# ---- identidad ----
usuario = spark.sql("SELECT current_user()").collect()[0][0]


def _nombre_valido(v):
    """Un identificador de UC válido: letras, números y guión bajo, empezando por letra o _."""
    return bool(re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", v))


# Validación de los nombres que escribiste en los widgets. Si están vacíos o mal formados,
# el notebook lo dice acá y no tres pasos más adelante con un error críptico de SQL.
NOMBRES_OK = True
if not CATALOGO or not SCHEMA:
    NOMBRES_OK = False
    print("⚠️  Faltan nombres. Escribe el CATÁLOGO y el SCHEMA en los widgets de arriba y "
          "vuelve a correr.\n"
          "    · Catálogo: dónde vas a trabajar. En Free Edition suele ser 'workspace'. En un "
          "workspace corporativo, alínealo con tu equipo: sirve un catálogo existente o uno "
          "nuevo dedicado al taller (crear catálogo NO es obligatorio).\n"
          "    · Schema: el nombre de tu espacio de trabajo, el que quieras "
          "(p. ej. z2h_ana, taller_ana…).")
else:
    for etiqueta, valor in (("catálogo", CATALOGO), ("schema", SCHEMA)):
        if not _nombre_valido(valor):
            NOMBRES_OK = False
            print(f"⚠️  '{valor}' no es un nombre válido para el {etiqueta}. Usa solo letras, "
                  f"números y guión bajo, empezando por una letra. Corrígelo arriba y vuelve "
                  f"a correr.")

print(f"Usuario   : {usuario}")
print(f"Catálogo  : {CATALOGO or '(vacío — escríbelo arriba)'}")
print(f"Schema    : {SCHEMA or '(vacío — escríbelo arriba)'}")
if NOMBRES_OK:
    print(f"→ Trabajarás en: {CATALOGO}.{SCHEMA}")
    print("  ⭐ ANOTA estos dos nombres. Los vas a escribir IGUALES en cada módulo del")
    print("     taller (M1 a M7). Aquí es donde eliges tu espacio de trabajo para todo el día.")

# En compute serverless las clusterUsageTags no existen: hay que detectarlo, no
# reportar "desconocida", que parece un error cuando no lo es.
def _describir_compute():
    for clave in ("spark.databricks.clusterUsageTags.sparkVersion",
                  "spark.databricks.clusterUsageTags.effectiveSparkVersion"):
        try:
            v = spark.conf.get(clave)
            if v:
                return v
        except Exception:
            pass
    try:
        if spark.conf.get("spark.databricks.clusterUsageTags.clusterId", ""):
            return "compute clásico (versión no reportada)"
    except Exception:
        pass
    return "serverless"


dbr = _describir_compute()
print(f"Compute     : {dbr}")
print(f"Fecha       : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

# ---- ¿qué tipo de cuenta es? ----
# Free Edition tiene cuotas y algunas features limitadas. Detectarlo ahora cambia el plan
# de esa persona para la tarde, en vez de descubrirlo en la sala.
def detectar_edicion():
    try:
        org = spark.conf.get("spark.databricks.clusterUsageTags.orgId", "")
    except Exception:
        org = ""
    try:
        # en Free Edition el listado de catálogos es mínimo y no hay metastore configurable
        cats = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
        return f"{len(cats)} catálogo(s) visibles: {', '.join(cats[:6])}"
    except Exception as e:
        return f"no se pudo listar catálogos: {e}"

probar("edicion", "Compute activo y consulta básica",
       lambda: spark.sql("SELECT 1 AS ok").collect()[0][0] == 1,
       arreglo="Conecta el notebook a un compute (arriba a la derecha) y vuelve a correr.")

print()
print("Catálogos visibles:", detectar_edicion())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 · Unity Catalog: tu catálogo y tu schema
# MAGIC
# MAGIC Acá se comprueba que puedes trabajar en el **catálogo** y el **schema** que escribiste
# MAGIC en los widgets de arriba. **No se infiere ni se adivina nada**: se usan exactamente los
# MAGIC nombres que tú pusiste, que son los mismos que repetirás en cada módulo.
# MAGIC
# MAGIC > 💡 **¿Qué escribir?**
# MAGIC > - **Catálogo:** dónde vas a trabajar. En Free Edition suele ser `workspace`. En un
# MAGIC >   workspace corporativo, **alínealo internamente con tu equipo**: puedes reutilizar un
# MAGIC >   catálogo existente (a menudo es el mejor camino) o crear uno nuevo para el taller.
# MAGIC >   **Crear un catálogo no es obligatorio.**
# MAGIC > - **Schema:** el nombre que quieras para tu espacio (p. ej. `z2h_ana`). Si no existe,
# MAGIC >   el notebook intenta crearlo.
# MAGIC >
# MAGIC > ⭐ **Anota los dos nombres y repítelos idénticos en todos los notebooks.** La mayoría
# MAGIC > de los errores del taller vienen de escribir un nombre distinto en cada módulo.

# COMMAND ----------

def usar_catalogo():
    """Comprueba que el catálogo que escribiste existe y puedes entrar en él."""
    if not NOMBRES_OK:
        raise Exception("faltan o son inválidos los nombres del catálogo/schema (arriba)")
    cats = [r[0].lower() for r in spark.sql("SHOW CATALOGS").collect()]
    if CATALOGO.lower() not in cats:
        raise Exception(
            f"el catálogo '{CATALOGO}' no aparece entre los que ves. "
            f"Revisa el nombre o el acceso a ese catálogo.")
    spark.sql(f"USE CATALOG {CATALOGO}")
    return f"'{CATALOGO}' visible y accesible"


probar("uc_catalogo", "Tu catálogo existe y puedes entrar en él", usar_catalogo,
       arreglo="Escribe en el widget 'catalogo' un catálogo donde tengas acceso. En Free "
               "Edition suele ser 'workspace'. En un workspace corporativo, alínealo con tu "
               "equipo: sirve un catálogo existente o uno nuevo dedicado al taller (crear "
               "catálogo no es obligatorio). Si no consigues ninguno, crea una cuenta de "
               "prueba gratuita en https://www.databricks.com/try-databricks y corre el "
               "notebook ahí.")


def crear_schema():
    """Crea (o reutiliza) el schema que escribiste. Es lo que harás en cada módulo."""
    if not NOMBRES_OK:
        raise Exception("faltan o son inválidos los nombres del catálogo/schema (arriba)")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
    spark.sql(f"USE {CATALOGO}.{SCHEMA}")
    return f"{CATALOGO}.{SCHEMA} listo para trabajar"


probar("uc_schema", "Puedes crear/usar tu schema", crear_schema,
       arreglo="Necesitas CREATE SCHEMA en ese catálogo. Prueba con otro catálogo en el "
               "widget (p. ej. 'workspace'), o alínealo con tu equipo / administrador para "
               "usar un catálogo donde sí puedas crear tu schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 · Delta Lake: crear tabla, escribir y viajar en el tiempo
# MAGIC
# MAGIC **Esto es exactamente el ejercicio del Módulo 1.** Se crea una tabla Delta, se
# MAGIC escriben unas filas, se modifican, y se consulta **cómo estaba la tabla antes** del
# MAGIC cambio (*time travel*).
# MAGIC
# MAGIC Si funciona, tienes lo mínimo para todo el taller: los 7 módulos escriben tablas.

# COMMAND ----------


def _requiere_catalogo():
    """Los pasos que siguen no tienen sentido sin un catálogo donde escribir."""
    if not CATALOGO:
        raise Exception("sin catálogo disponible — resuelve el paso 2 primero")


TABLA = f"{CATALOGO}.{SCHEMA}._verificacion_tmp"


def crear_tabla():
    _requiere_catalogo()
    # Tabla de prueba NEUTRA, agnóstica al escenario de capstone: columnas genéricas que
    # sirven para probar Delta, time travel, row filter (por 'region') y column masking
    # (sobre 'dato_sensible'). El paso 5 necesita 'dato_sensible' desde el inicio, porque
    # agregarla después con ALTER TABLE es un paso frágil de más.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLA} (
            id INT, valor DOUBLE, region STRING, marcado BOOLEAN, dato_sensible STRING
        )
    """)
    spark.sql(f"""
        INSERT INTO {TABLA} VALUES
        (1, 150.0, 'norte', false, 'AAAA-BBBB-CCCC-1234'),
        (2, 9800.0, 'centro', true, 'AAAA-BBBB-CCCC-5678'),
        (3, 42.5, 'sur', false, 'AAAA-BBBB-CCCC-9012'),
        (4, 15200.0, 'norte', false, 'AAAA-BBBB-CCCC-3456')
    """)
    return f"{spark.table(TABLA).count()} filas insertadas"


probar("delta_crear", "Crear tabla Delta y escribir datos", crear_tabla,
       arreglo="Necesitas MODIFY en tu schema. Verifica con tu administrador.")


def historial_y_time_travel():
    """UPDATE + DESCRIBE HISTORY + VERSION AS OF: el ejercicio del Módulo 1."""
    _requiere_catalogo()
    spark.sql(f"UPDATE {TABLA} SET marcado = true WHERE valor > 10000")
    versiones = spark.sql(f"DESCRIBE HISTORY {TABLA}").count()
    antes = spark.sql(f"SELECT COUNT(*) c FROM {TABLA} VERSION AS OF 0 "
                      f"WHERE marcado = true").collect()[0][0]
    ahora = spark.sql(f"SELECT COUNT(*) c FROM {TABLA} "
                      f"WHERE marcado = true").collect()[0][0]
    if versiones < 2:
        return False
    return f"{versiones} versiones · marcados antes={antes}, ahora={ahora}"


probar("delta_time_travel", "Historial de versiones y time travel",
       historial_y_time_travel,
       arreglo="Si falla, avísanos: el ejercicio del Módulo 1 depende de esto.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 · Volúmenes: leer archivos crudos
# MAGIC
# MAGIC Los datos del taller llegan como **archivos** (JSON y CSV) que viven en un
# MAGIC **volumen** de Unity Catalog. En el **Módulo 2** los vas a ingerir con Auto Loader.
# MAGIC
# MAGIC Acá se crea un volumen, se escribe un archivo pequeño y se lee. Si falla, el Módulo 2
# MAGIC no es bloqueante para el resto del taller.

# COMMAND ----------

VOLUMEN = f"{CATALOGO}.{SCHEMA}._verificacion_vol"


def crear_volumen():
    _requiere_catalogo()
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUMEN}")
    ruta = f"/Volumes/{CATALOGO}/{SCHEMA}/_verificacion_vol/prueba.json"
    dbutils.fs.put(ruta, '{"id": 1, "monto": 150.0}\n{"id": 2, "monto": 300.0}',
                   overwrite=True)
    n = spark.read.json(f"/Volumes/{CATALOGO}/{SCHEMA}/_verificacion_vol/").count()
    return f"volumen creado, {n} filas leídas desde JSON"


probar("volumen", "Crear volumen y leer archivos", crear_volumen,
       arreglo=f"Necesitas CREATE VOLUME en tu schema. Sin esto, el Módulo 2 se hace en "
               f"pareja. Avísanos.", critico=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 · Gobierno: proteger filas y columnas
# MAGIC
# MAGIC **Es el corazón del Módulo 4**, el módulo de gobierno y seguridad. Ahí vas a hacer
# MAGIC que la *misma consulta* devuelva *distintos resultados* según quién pregunta:
# MAGIC
# MAGIC - **Column masking** — un dato sensible que se ve enmascarado, p. ej. `****-****-****-1234`
# MAGIC - **Row-level security** — cada persona ve solo las filas de su región
# MAGIC
# MAGIC > 💡 Estas dos funcionaron en las pruebas que hicimos, **incluso en Free Edition**.
# MAGIC > Si en tu caso fallan, **no es un error tuyo**: suele ser un permiso del workspace.
# MAGIC > Avísanos con el resultado y lo revisamos antes del taller.

# COMMAND ----------

FN_MASCARA = f"{CATALOGO}.{SCHEMA}._mascara_tmp"
FN_FILTRO = f"{CATALOGO}.{SCHEMA}._filtro_tmp"


def probar_masking():
    """Aplica una máscara y comprueba que el valor realmente sale enmascarado."""
    _requiere_catalogo()
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {FN_MASCARA}(valor STRING)
        RETURN CASE WHEN is_account_group_member('z2h_pii_readers') THEN valor
                    ELSE CONCAT('****-****-****-', SUBSTRING(valor, -4)) END
    """)
    spark.sql(f"ALTER TABLE {TABLA} ALTER COLUMN dato_sensible SET MASK {FN_MASCARA}")
    visto = spark.sql(f"SELECT dato_sensible FROM {TABLA} LIMIT 1").collect()[0][0]
    spark.sql(f"ALTER TABLE {TABLA} ALTER COLUMN dato_sensible DROP MASK")
    # no basta con que el ALTER no falle: el dato tiene que salir enmascarado
    if not str(visto).startswith("****"):
        return False
    return f"soportado · se vio '{visto}' en vez del número completo"


probar("masking", "Column masking (Módulo 4)", probar_masking,
       arreglo="Suele ser un permiso del workspace, no de la edición: pídele a tu "
               "administrador permiso para crear funciones y aplicar máscaras, o avísanos. "
               "Avísanos con el resultado del notebook.",
       critico=False)


def probar_row_filter():
    _requiere_catalogo()
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {FN_FILTRO}(reg STRING)
        RETURN reg IS NOT NULL
    """)
    spark.sql(f"ALTER TABLE {TABLA} SET ROW FILTER {FN_FILTRO} ON (region)")
    spark.sql(f"SELECT COUNT(*) FROM {TABLA}").collect()
    spark.sql(f"ALTER TABLE {TABLA} DROP ROW FILTER")
    return "row filter soportado"


probar("row_filter", "Row-level security (Módulo 4)", probar_row_filter,
       arreglo="Mismo caso que column masking: suele ser un permiso del workspace. "
               "Avísanos y lo revisamos antes del taller.",
       critico=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 · Machine Learning
# MAGIC
# MAGIC En el **Módulo 6** vas a generar una predicción sobre tus datos y registrarla en
# MAGIC Unity Catalog. Según el escenario de capstone, eso puede ser un modelo entrenado con
# MAGIC MLflow o una predicción hecha con funciones de la plataforma. Acá se comprueban las
# MAGIC piezas comunes: que **MLflow** esté disponible y que puedas usar el **registry de
# MAGIC Unity Catalog**.

# COMMAND ----------

def probar_mlflow():
    import mlflow
    return f"MLflow {mlflow.__version__}"


probar("mlflow", "MLflow disponible", probar_mlflow,
       arreglo="Avísanos: el Módulo 6 depende de MLflow.", critico=False)


def probar_registro_modelos():
    """Registrar objetos de ML en UC es parte del Módulo 6."""
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    return "registry de Unity Catalog configurado"


probar("modelos_uc", "Registro de modelos en Unity Catalog", probar_registro_modelos,
       arreglo="", critico=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 · Capa de consumo: warehouse, dashboard, Genie y Apps
# MAGIC
# MAGIC Es el **Módulo 7**, donde cierras el proyecto con tres entregables. Acá no basta
# MAGIC con listar: se **crea un dashboard y un espacio Genie de prueba y se borran**, que
# MAGIC es la única forma de saber que tienes permiso de crearlos.

# COMMAND ----------

MARCA = "_z2h_probe"          # prefijo de todo lo que se crea para probar
_a_borrar = {"dashboards": [], "genie": []}


def _cliente():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


# ---- 1 · SQL Warehouse ----
def probar_warehouse():
    """Sin warehouse no hay dashboard ni Genie: es el prerrequisito de los otros tres."""
    global WAREHOUSE_ID
    whs = list(_cliente().warehouses.list())
    if not whs:
        WAREHOUSE_ID = None
        return False
    # se prefiere uno que ya esté corriendo, para no esperar el arranque
    corriendo = [x for x in whs if str(getattr(x, "state", "")).endswith("RUNNING")]
    elegido = (corriendo or whs)[0]
    WAREHOUSE_ID = elegido.id
    return f"{len(whs)} warehouse(s) · se usará '{elegido.name}'"


WAREHOUSE_ID = None
probar("warehouse", "SQL Warehouse disponible", probar_warehouse,
       arreglo="Sin SQL Warehouse no puedes crear dashboards ni espacios Genie. "
               "Pídele uno a tu administrador o avísanos.",
       critico=False)


# ---- 2 · crear un dashboard AI/BI ----
def probar_dashboard():
    if not WAREHOUSE_ID:
        raise Exception("sin SQL Warehouse: no se puede probar")
    w = _cliente()
    r = w.api_client.do(
        "POST", "/api/2.0/lakeview/dashboards",
        body={"display_name": f"{MARCA}_dash",
              "warehouse_id": WAREHOUSE_ID,
              "serialized_dashboard": '{"datasets":[],"pages":[]}'},
    )
    did = r.get("dashboard_id")
    if did:
        _a_borrar["dashboards"].append(did)
    return "puedes crear dashboards AI/BI"


probar("dashboard", "Crear dashboard AI/BI (Módulo 7)", probar_dashboard,
       arreglo="No puedes crear dashboards. Pídele a tu administrador permiso de "
               "creación de dashboards. Avísanos con el resultado del notebook.",
       critico=False)


# ---- 3 · crear un espacio Genie ----
def probar_genie():
    if not WAREHOUSE_ID:
        raise Exception("sin SQL Warehouse: no se puede probar")
    w = _cliente()
    r = w.api_client.do(
        "POST", "/api/2.0/genie/spaces",
        body={"title": f"{MARCA}_genie",
              "warehouse_id": WAREHOUSE_ID,
              "serialized_space": '{"version":1}'},
    )
    sid = r.get("space_id")
    if sid:
        _a_borrar["genie"].append(sid)
    return "puedes crear espacios Genie"


probar("genie", "Crear espacio Genie (Módulo 7)", probar_genie,
       arreglo="No puedes crear espacios Genie. Suele ser permiso del workspace o falta "
               "de Genie habilitado. Avísanos con el resultado del notebook.",
       critico=False)


# ---- 4 · Databricks Apps ----
def probar_apps():
    """No se despliega una app de prueba: tarda minutos. Solo se comprueba el acceso
    a la API, que es lo que falla cuando Apps no está disponible."""
    w = _cliente()
    apps = list(w.apps.list())
    return f"API de Apps accesible ({len(apps)} app(s) en el workspace)"


probar("apps", "Databricks Apps accesible (Módulo 7)", probar_apps,
       arreglo="Apps no está disponible en tu workspace. En el Módulo 7 verás la demo "
               "Avísanos con el resultado del notebook.",
       critico=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 8 · Limpieza
# MAGIC
# MAGIC Se borra **todo** lo que este notebook creó para probar: el schema, la tabla, las
# MAGIC funciones de gobierno, el volumen, el dashboard y el espacio Genie.
# MAGIC
# MAGIC Tu workspace queda **exactamente como estaba**. Los objetos se borran por
# MAGIC identificador, nunca por nombre, así que es imposible que toque algo tuyo.

# COMMAND ----------

# ---- objetos de Unity Catalog ----
for sentencia in (
    f"ALTER TABLE {TABLA} DROP ROW FILTER",
    f"ALTER TABLE {TABLA} ALTER COLUMN dato_sensible DROP MASK",
    f"DROP TABLE IF EXISTS {TABLA}",
    f"DROP FUNCTION IF EXISTS {FN_MASCARA}",
    f"DROP FUNCTION IF EXISTS {FN_FILTRO}",
    f"DROP VOLUME IF EXISTS {VOLUMEN}",
):
    try:
        spark.sql(sentencia)
    except Exception:
        pass   # si no existe o no aplica, no importa

# ⚠️ El SCHEMA no se borra: es TU espacio de trabajo del taller (lo usarás en cada módulo).
# Solo se eliminan los objetos de prueba que este notebook creó (todos con prefijo temporal).

# ---- dashboard y espacio Genie de prueba ----
# Se borran por id, no por nombre: así nunca se toca algo del participante.
try:
    _w = _cliente()
    for did in _a_borrar["dashboards"]:
        try:
            _w.api_client.do("DELETE", f"/api/2.0/lakeview/dashboards/{did}")
        except Exception:
            print(f"   ⚠️  no se pudo borrar el dashboard de prueba {did} — bórralo a mano")
    for sid in _a_borrar["genie"]:
        try:
            _w.api_client.do("DELETE", f"/api/2.0/genie/spaces/{sid}")
        except Exception:
            print(f"   ⚠️  no se pudo borrar el espacio Genie de prueba {sid} — bórralo a mano")
except Exception:
    pass

print("🧹 Limpieza terminada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado
# MAGIC
# MAGIC ### 👇 Copia TODO el bloque de abajo y pégalo en tu respuesta al correo

# COMMAND ----------

criticos = {"edicion", "uc_catalogo", "uc_schema", "delta_crear", "delta_time_travel"}
por_clave = {r["clave"]: r for r in resultados}

fallas_criticas = [r for r in resultados if r["clave"] in criticos and r["estado"] == FALLA]
limitaciones = [r for r in resultados if r["clave"] not in criticos
                and r["estado"] in (FALLA, PARCIAL)]

# ---- veredicto ----
if fallas_criticas:
    veredicto = "🔴 NO LISTO — hay que resolver algo antes del taller"
elif limitaciones:
    veredicto = "🟡 LISTO CON LIMITACIONES — puedes participar, con alcance reducido en algo"
else:
    veredicto = "🟢 LISTO — tienes todo lo necesario"

ancho = 78
L = []
L.append("=" * ancho)
L.append("  DATABRICKS: FROM ZERO TO HERO — VERIFICACIÓN DE AMBIENTE")
L.append("=" * ancho)
L.append(f"  Usuario : {usuario}")
L.append(f"  Catálogo: {CATALOGO or '(no escrito)'}")
L.append(f"  Schema  : {SCHEMA or '(no escrito)'}   ← úsalo IGUAL en todos los módulos")
L.append(f"  Compute : {dbr}")
L.append(f"  Fecha   : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
L.append("-" * ancho)
L.append(f"  {veredicto}")
L.append("-" * ancho)
for r in resultados:
    icono = {OK: "OK  ", PARCIAL: "WARN", FALLA: "FALLA", NA: "--  "}[r["estado"]]
    L.append(f"  [{icono}] {r['etiqueta']}")
    if r["detalle"] and r["estado"] != OK:
        L.append(f"           {r['detalle'][:150]}")
L.append("=" * ancho)

print("\n".join(L))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué hacer ahora

# COMMAND ----------

print()
if fallas_criticas:
    print("🔴 HAY QUE RESOLVER ESTO ANTES DEL TALLER")
    print()
    for r in fallas_criticas:
        print(f"  ✗ {r['etiqueta']}")
        if r["detalle"]:
            print(f"    Error: {r['detalle'][:160]}")
        if r["arreglo"]:
            print(f"    → {r['arreglo']}")
        print()
    print("  TIENES DOS CAMINOS:")
    print()
    print("  1. Pídele a tu administrador los permisos que faltan, en un workspace de")
    print("     DESARROLLO o de pruebas (nunca de producción).")
    print()
    print("  2. Crea una cuenta de prueba gratuita de 14 días — no necesita tarjeta de")
    print("     crédito y todos los módulos funcionan completos:")
    print("        https://www.databricks.com/try-databricks")
    print()
    print("  Cualquiera de los dos sirve. Si no estás seguro de cuál te conviene,")
    print("  respóndenos el correo con el bloque de arriba y te orientamos.")
    print()

if limitaciones:
    print("🟡 LIMITACIONES DETECTADAS — puedes participar, con estos ajustes:")
    print()
    for r in limitaciones:
        print(f"  • {r['etiqueta']}")
        if r["arreglo"]:
            print(f"    → {r['arreglo']}")
        print()
    print("  Nada de esto es un error tuyo y no te bloquea el taller. Avísanos con el")
    print("  resultado para que lo tengamos en cuenta.")
    print()
    print("  ¿Quieres el taller sin ninguna limitación? Crea una cuenta de prueba")
    print("  gratuita de 14 días (sin tarjeta de crédito):")
    print("        https://www.databricks.com/try-databricks")
    print()

if not fallas_criticas and not limitaciones:
    print("🟢 TODO LISTO. No tienes que hacer nada más.")
    print()
    print("  Nos vemos en el taller. Trae tu laptop y tu cargador.")
    print()

print("-" * 78)
print("RECORDATORIO: respóndenos el correo con el bloque de resultado pegado,")
print("aunque todo esté en verde. Así sabemos que estás listo.")
print("-" * 78)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📬 Último paso
# MAGIC
# MAGIC **Copia el bloque de resumen de arriba y respóndenos el correo con eso pegado** —
# MAGIC incluso si todo salió en verde. Es la única forma que tenemos de saber que estás listo.
# MAGIC
# MAGIC > 📝 **Y lo más importante para el día del taller:** anota tu **catálogo** y tu
# MAGIC > **schema** (aparecen en el resumen). Son los que definiste aquí y **los vas a escribir
# MAGIC > iguales en los widgets de cada módulo, del M1 al M7.** Todo tu proyecto vive ahí.
# MAGIC
# MAGIC ### Recordatorio de los tres caminos
# MAGIC
# MAGIC | | Opción | Cuándo |
# MAGIC |---|---|---|
# MAGIC | ⭐ | **Tu workspace de desarrollo o pruebas** | Si tu empresa ya usa Databricks. **Nunca producción.** |
# MAGIC | ✅ | **Trial de 14 días** → https://www.databricks.com/try-databricks | Si no tienes cuenta. Todos los módulos completos. |
# MAGIC | 🟡 | **Free Edition** (mismo enlace) | Última alternativa. Puede limitar el gobierno del Módulo 4. |
# MAGIC
# MAGIC ### ¿Dudas?
# MAGIC
# MAGIC Responde el correo de la invitación o escríbenos por WhatsApp. **Preferimos resolverlo
# MAGIC hoy que el día del taller.**
# MAGIC
# MAGIC **Nos vemos pronto. 🚀**
