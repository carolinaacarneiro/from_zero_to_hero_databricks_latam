# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🤖 Módulo 6 · Machine Learning con MLflow
# MAGIC
# MAGIC Vas a entrenar un modelo que predice el fraude, registrarlo, y usarlo.
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - **Ejecuta el Paso 1 primero de todo.** El entrenamiento corre **mientras** ves la
# MAGIC   teoría. Nadie mira una barra de progreso.
# MAGIC - Donde dice **📝 TU TURNO**, completas código.
# MAGIC - 🙋 Trabado más de 3 minutos: levanta la mano.
# MAGIC
# MAGIC ## De dónde venimos
# MAGIC Tienes datos confiables (silver, M3) y gobernados (M4). Hoy entrenas un modelo sobre
# MAGIC ellos y lo dejas **registrado con un alias**, listo para que la **App del Módulo 7** lo
# MAGIC consulte.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Requiere **ML Runtime** (no serverless básico, no Free
# MAGIC > Edition). Usamos **MLflow con scikit-learn** — el camino directo y transparente. En un
# MAGIC > entorno con AutoML disponible, este mismo problema se puede acelerar con AutoML.
# MAGIC >
# MAGIC > 💡 Predecir un evento raro con datos históricos es de cualquier industria: fallas de
# MAGIC > máquinas, deserción de clientes, riesgo de reingreso hospitalario. Cambia el dato.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 · Lanza el entrenamiento (minuto 0)
# MAGIC
# MAGIC **Ejecuta esta celda y las dos siguientes ahora.** Construyen las features y entrenan el
# MAGIC modelo. Toma unos minutos — y ahí empieza la teoría, mientras esto corre.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 · Declara tu catálogo y tu schema
# MAGIC
# MAGIC **Corre la celda de abajo una vez** para que aparezcan dos campos arriba del notebook.
# MAGIC Luego **escribe en ellos el catálogo y el schema donde estás trabajando** — los
# MAGIC **mismos** que declaraste en el Módulo 1 y que vienes usando en todos los módulos.
# MAGIC
# MAGIC > ⚠️ **No adivinamos nada por ti.** Tú declaras explícitamente dónde trabajas, para que
# MAGIC > no haya duda de en qué catálogo y schema estás. Si los dejas vacíos, el notebook se
# MAGIC > detiene y te lo pide.

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
_usuario = spark.sql("SELECT current_user()").collect()[0][0]

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

# 3 · el schema tiene que existir (lo creaste en el Módulo 1)
schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{catalogo}`").collect()]
if schema not in schemas:
    raise Exception(
        f"\n❌ El schema '{catalogo}.{schema}' no existe.\n"
        f"   Corre los módulos anteriores con estos mismos valores, o corrige el nombre "
        f"arriba.\n"
    )

spark.sql(f"USE `{catalogo}`.`{schema}`")

print(f"👤 Usuario       : {_usuario}")
print(f"📁 Trabajarás en : {catalogo}.{schema}")
print("✅ Espacio declarado y verificado. Nadie más toca este schema.")

# COMMAND ----------

import mlflow

# MLflow registra los modelos en Unity Catalog
mlflow.set_registry_uri("databricks-uc")
NOMBRE_MODELO = f"{catalogo}.{schema}.modelo_fraude"

print(f"📁 {catalogo}.{schema}")
print(f"🤖 El modelo se registrará como: {NOMBRE_MODELO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construye la tabla de features
# MAGIC
# MAGIC Las señales con las que el modelo aprende. Fíjate en `desviacion_monto`: no es un dato
# MAGIC crudo, lo **derivamos** — cuánto se aleja el monto del promedio de su categoría. Las
# MAGIC features derivadas suelen ser las más predictivas.
# MAGIC
# MAGIC > 🔒 **Sin fuga de información:** todas estas columnas existen **en el momento en que
# MAGIC > llega la transacción**. No usamos nada que solo se sepa después de confirmar el fraude.

# COMMAND ----------

from pyspark.sql import functions as F

# promedio de monto por categoría (para la desviación)
prom_cat = (spark.table("silver_transacciones")
            .groupBy("categoria_comercio")
            .agg(F.avg("monto").alias("_prom_cat")))

features = (
    spark.table("silver_transacciones")
    .join(prom_cat, "categoria_comercio", "left")
    .withColumn("desviacion_monto", F.col("monto") / F.col("_prom_cat"))
    # las columnas numéricas se declaran como double (no int): así el modelo no se queja
    # si en producción llega un valor nulo. Es la recomendación de MLflow para evitar el
    # warning de "integer columns cannot represent missing values".
    .withColumn("hora", F.hour("fecha").cast("double"))
    .withColumn("es_exterior", (F.col("pais") != "CO").cast("double"))
    .select(
        F.col("monto").cast("double").alias("monto"),
        F.col("desviacion_monto").cast("double").alias("desviacion_monto"),
        "hora", "es_exterior",
        "categoria_comercio", "canal", "segmento",
        F.col("antiguedad_meses").cast("double").alias("antiguedad_meses"),
        F.col("score_crediticio").cast("double").alias("score_crediticio"),
        F.col("es_fraude").cast("int").alias("es_fraude"),
    )
    .na.drop()
)

# overwriteSchema: si features_fraude ya existía con otros tipos (por una corrida previa),
# reemplaza el esquema en vez de intentar fusionarlo — evita el error DELTA_FAILED_TO_MERGE_FIELDS.
(features.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("features_fraude"))
print(f"✅ features_fraude: {spark.table('features_fraude').count():,} filas")
display(spark.table("features_fraude").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entrena el modelo (corre mientras ves la teoría)
# MAGIC
# MAGIC Un Gradient Boosting sobre las features. MLflow registra la corrida sola: parámetros,
# MAGIC métricas y el modelo. **Mide AUC, no accuracy** — ya sabes por qué.

# COMMAND ----------

import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import mlflow.sklearn

df = spark.table("features_fraude").toPandas()
X = df.drop(columns=["es_fraude"])
y = df["es_fraude"]
X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

cat_cols = ["categoria_comercio", "canal", "segmento"]
num_cols = [c for c in X.columns if c not in cat_cols]
pre = ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols)],
                        remainder="passthrough")
modelo = Pipeline([("pre", pre),
                   ("clf", GradientBoostingClassifier(n_estimators=120, max_depth=3,
                                                      random_state=42))])

mlflow.set_experiment(f"/Users/{_usuario}/z2h_fraude")
with mlflow.start_run(run_name="gbt_fraude") as run:
    modelo.fit(X_tr, y_tr)
    proba = modelo.predict_proba(X_te)[:, 1]
    auc = roc_auc_score(y_te, proba)

    mlflow.log_param("algoritmo", "GradientBoosting")
    mlflow.log_param("n_estimators", 120)
    mlflow.log_metric("auc", auc)
    mlflow.sklearn.log_model(modelo, "modelo", input_example=X_tr.head(3))
    run_id = run.info.run_id

print(f"✅ Entrenamiento terminado")
print(f"📊 AUC = {auc:.4f}   (objetivo: ≥ 0.85)")
print(f"🔖 run_id = {run_id}")
print()
print("👉 Ahora empieza la teoría. Este modelo ya está entrenado y registrado en MLflow.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · Abre el experimento de MLflow
# MAGIC
# MAGIC *(Después de la teoría.)* En el ícono de **Experiments** (probeta, a la derecha) o en el
# MAGIC menú **Experiments**, abre `z2h_fraude`.
# MAGIC
# MAGIC | Qué mirar | Por qué |
# MAGIC |---|---|
# MAGIC | La columna **`auc`** | Tu métrica real. Ordena por ella |
# MAGIC | Los **parámetros** de cada run | Qué algoritmo, qué configuración |
# MAGIC | El **modelo** guardado como artefacto | Reproducible: cualquiera puede recargarlo |
# MAGIC
# MAGIC Si entrenaste varias veces, verías **varios runs** para comparar lado a lado. Esa es la
# MAGIC gracia: no eliges el mejor "de memoria", lo eliges por su AUC.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · 📝 TU TURNO — registra el modelo con alias
# MAGIC
# MAGIC Un modelo en un experimento todavía es un resultado de laboratorio. **Registrarlo en
# MAGIC Unity Catalog** lo convierte en un objeto gobernado: con permisos, versiones y alias.
# MAGIC
# MAGIC Completa las dos líneas: registrar el run como modelo, y ponerle el alias `champion`.
# MAGIC
# MAGIC <details>
# MAGIC <summary>💡 Solución</summary>
# MAGIC
# MAGIC <pre>version = mlflow.register_model(f"runs:/{run_id}/modelo", NOMBRE_MODELO).version
# MAGIC from mlflow.tracking import MlflowClient
# MAGIC MlflowClient().set_registered_model_alias(NOMBRE_MODELO, "champion", version)</pre>
# MAGIC </details>

# COMMAND ----------

from mlflow.tracking import MlflowClient

# 📝 TODO: registra el modelo y ponle el alias 'champion'
# version = mlflow.register_model(..., NOMBRE_MODELO).version
# MlflowClient().set_registered_model_alias(NOMBRE_MODELO, "champion", version)

# COMMAND ----------

# MAGIC %md
# MAGIC 🎉 **Tu modelo ahora es `modelo_fraude@champion`.**
# MAGIC
# MAGIC En vez de `modelo_final_v3_bueno.pkl`, tu código pedirá siempre `@champion` y obtendrá
# MAGIC la versión vigente. Cuando re-entrenes y promuevas una versión nueva, **el código que lo
# MAGIC usa no cambia**.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · 📝 TU TURNO — carga por alias y predice
# MAGIC
# MAGIC Carga el modelo **por su alias** (no por versión ni ruta) y predice sobre unas
# MAGIC transacciones. Verás la `probabilidad_fraude` junto a los datos reales.
# MAGIC
# MAGIC <details>
# MAGIC <summary>💡 Solución</summary>
# MAGIC
# MAGIC <pre>modelo_cargado = mlflow.sklearn.load_model(f"models:/{NOMBRE_MODELO}@champion")
# MAGIC muestra = spark.table("features_fraude").limit(200).toPandas()
# MAGIC muestra["probabilidad_fraude"] = modelo_cargado.predict_proba(
# MAGIC     muestra.drop(columns=["es_fraude"]))[:, 1]
# MAGIC display(muestra[["monto","categoria_comercio","pais" if "pais" in muestra else "canal",
# MAGIC                  "es_fraude","probabilidad_fraude"]])</pre>
# MAGIC </details>

# COMMAND ----------

# 📝 TODO: carga models:/{NOMBRE_MODELO}@champion y predice sobre una muestra
# modelo_cargado = mlflow.sklearn.load_model(f"models:/{NOMBRE_MODELO}@champion")
# muestra = spark.table("features_fraude").limit(200).toPandas()
# muestra["probabilidad_fraude"] = modelo_cargado.predict_proba(
#     muestra.drop(columns=["es_fraude"]))[:, 1]
# display(muestra[["monto", "categoria_comercio", "canal", "es_fraude", "probabilidad_fraude"]])

# COMMAND ----------

# MAGIC %md
# MAGIC 👀 **Mira la columna `probabilidad_fraude`.** Ordénala de mayor a menor: las
# MAGIC transacciones que el modelo considera más sospechosas suben arriba. Compara con
# MAGIC `es_fraude` real — el modelo no acierta siempre, pero **distingue**. Eso es el AUC que
# MAGIC viste, hecho tangible.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 5 · El linaje del modelo
# MAGIC
# MAGIC En **Catalog** → tu schema → **Models** → `modelo_fraude` → pestaña **Lineage**.
# MAGIC
# MAGIC Ves de qué tablas salió: `features_fraude` ← `silver_transacciones` ← bronze ← archivos.
# MAGIC **El modelo es un objeto gobernado más**, con su linaje, igual que una tabla.
# MAGIC
# MAGIC > 💡 Esto responde una pregunta de auditoría real: *«¿con qué datos se entrenó el modelo
# MAGIC > que rechazó esta transacción?»*. Con linaje, es una consulta. Sin él, una investigación.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · Verifica tu checkpoint

# COMMAND ----------

resultados = []
try:
    m = mlflow.sklearn.load_model(f"models:/{NOMBRE_MODELO}@champion")
    resultados.append((True, f"El modelo {NOMBRE_MODELO}@champion existe y se puede cargar"))
except Exception as e:
    resultados.append((False, f"No pude cargar el modelo @champion: {str(e)[:60]}"))

try:
    resultados.append((auc >= 0.80, f"AUC = {auc:.4f} (objetivo ≥ 0.85; ≥ 0.80 aceptable)"))
except Exception:
    resultados.append((False, "No encontré el AUC — ¿corriste el paso 1?"))

print("=" * 68)
print("  MÓDULO 6 · CHECKPOINT")
print("=" * 68)
for ok, msg in resultados:
    print(f"  {'✅' if ok else '❌'}  {msg}")
print("=" * 68)
if all(ok for ok, _ in resultados):
    print("""
  🎉 ¡Módulo 6 completo!
    · Construiste features (sin fuga de información)
    · Entrenaste un modelo y mediste AUC, no accuracy
    · Lo registraste en UC como modelo_fraude@champion
    · Predijiste sobre datos reales

  👉 En el Módulo 7 una App va a consultar ESTE modelo, por su alias,
     para calificar transacciones en vivo. El ciclo se cierra.
""")
else:
    print("  ⚠️  Revisa los ❌. Lo más común: falta completar el registro (paso 3).")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · Hospeda tu modelo como un Serving Endpoint ⭐
# MAGIC
# MAGIC Registrar el modelo lo deja **guardado**. Para que una App (o cualquier sistema) lo use en
# MAGIC vivo, hay que **servirlo como un endpoint**: una API REST a la que le mandas una transacción
# MAGIC y te devuelve la predicción. **La App del Módulo 7 consulta este endpoint** — así que este
# MAGIC paso es necesario para cerrar el capstone.
# MAGIC
# MAGIC > 💡 **¿Por qué un endpoint y no cargar el modelo en la App?** Porque el endpoint se encarga
# MAGIC > de las versiones de librerías (numpy, scikit-learn…) por ti. La App solo hace una llamada
# MAGIC > HTTP — no necesita tener instaladas las mismas versiones con que entrenaste. Es más simple
# MAGIC > y es como se consume un modelo en producción.
# MAGIC
# MAGIC ## 📝 TU TURNO — créalo por la UI (manual)
# MAGIC
# MAGIC 1. Menú izquierdo → **Serving** → **Create serving endpoint**.
# MAGIC 2. **Name**: ponle un nombre fácil de recordar, p. ej. **`zerotohero`** *(lo vas a usar en
# MAGIC    la App del M7)*.
# MAGIC 3. **Entity**: elige **tu modelo** → `<catálogo>.<schema>.modelo_fraude`, versión la que
# MAGIC    tiene el alias **`@champion`**.
# MAGIC 4. **Compute**: deja el tamaño más pequeño (**Small**) y activa **Scale to zero** *(así no
# MAGIC    gasta cuando nadie lo usa)*.
# MAGIC 5. **Create**. Tarda **unos minutos** en quedar **Ready** — mientras tanto, sigue con el
# MAGIC    Módulo 7 (dashboard y Genie) y vuelve cuando esté listo.
# MAGIC
# MAGIC ## Cuando esté Ready, pruébalo
# MAGIC En la página del endpoint, pestaña **Use** (o **Query**), manda esta entrada de ejemplo y
# MAGIC mira la respuesta:
# MAGIC
# MAGIC ```json
# MAGIC {"dataframe_split": {
# MAGIC   "columns": ["monto","desviacion_monto","hora","es_exterior","categoria_comercio","canal","segmento","antiguedad_meses","score_crediticio"],
# MAGIC   "data": [[150000.0, 1.0, 14.0, 0.0, "supermercado", "presencial", "retail", 36.0, 650.0]]
# MAGIC }}
# MAGIC ```
# MAGIC
# MAGIC Debe devolver algo como `{"predictions": [0]}`. **Anota el nombre del endpoint** — lo
# MAGIC necesitas para la App del Módulo 7.
# MAGIC
# MAGIC > 💡 Encima del serving se puede poner **Mosaic AI Gateway**: control de acceso, rate limits,
# MAGIC > logging de payloads y control de costo. Es la capa de gobierno para modelos servidos.
# MAGIC
# MAGIC ### 🏆 Reto opcional
# MAGIC Entrena una segunda variante (cambia `n_estimators` o `max_depth`), regístrala como una
# MAGIC versión nueva, y compárala con la champion en el experimento. Si es mejor, muévele el
# MAGIC alias `champion`. El endpoint que sirve `@champion` **usará la nueva versión sin que toques
# MAGIC la App**. Así se promueve un modelo en la vida real.

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Lo que te llevas
# MAGIC
# MAGIC | Concepto | Dónde lo viviste |
# MAGIC |---|---|
# MAGIC | **Features, sin fuga de información** | Todas existen al momento de predecir |
# MAGIC | **La trampa del accuracy** | Medimos AUC porque el fraude es 1,5% |
# MAGIC | **MLflow registra cada corrida** | Reproducible y comparable |
# MAGIC | **El modelo es objeto gobernado de UC** | Permisos, linaje, versiones, alias |
# MAGIC | **`@champion`** | El código pide el alias, no un archivo |
# MAGIC | **Serving endpoint** | El modelo servido como API REST, listo para consumir |
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC Tienes un modelo que predice fraude, registrado, gobernado y **servido como endpoint**. En
# MAGIC el **Módulo 7** —el último— lo pones frente a un usuario: un dashboard, un espacio Genie, y
# MAGIC una **App** que consulta **tu serving endpoint** para calificar transacciones en vivo.
# MAGIC
# MAGIC > ⏱️ **Deja el endpoint creándose ahora** (Paso 7) y sigue al Módulo 7. Estará **Ready**
# MAGIC > para cuando llegues al Bloque C (la App).
