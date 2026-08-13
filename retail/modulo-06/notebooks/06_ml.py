# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🔮 Módulo 6 · Pronóstico de ventas con Machine Learning
# MAGIC
# MAGIC Vas a **entrenar tu propio modelo** para pronosticar las ventas de los próximos 90 días
# MAGIC —en dólares y en unidades— por país, y lo vas a **gobernar con MLflow y Unity Catalog**.
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - Ejecuta celda por celda. El código ya está listo — el foco es **entender el flujo de ML**.
# MAGIC - 🙋 Trabado más de 3 minutos: levanta la mano.
# MAGIC
# MAGIC ## De dónde venimos
# MAGIC Tienes la serie de tiempo `gold_ventas_diarias` (M3): ventas por día, país y categoría, con
# MAGIC tendencia y estacionalidad reales. Hoy la proyectas al futuro con un modelo entrenado por ti.
# MAGIC
# MAGIC ## El flujo de este módulo — igual que un proyecto de ML real
# MAGIC Pronosticar es un problema de **machine learning**: le enseñas a un modelo el patrón
# MAGIC histórico (tendencia + estacionalidad) y le pides que lo proyecte. El camino:
# MAGIC
# MAGIC | Paso | Qué haces |
# MAGIC |---|---|
# MAGIC | **Features** | conviertes cada fecha en señales que el modelo entiende (tendencia, estacionalidad) |
# MAGIC | **Entrenar** | un modelo `scikit-learn` aprende el patrón; **MLflow** registra la corrida |
# MAGIC | **Registrar** | el modelo queda en **Unity Catalog** con alias `@champion` |
# MAGIC | **Pronosticar** | cargas el modelo por su alias y proyectas 90 días → tabla gold |
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos.
# MAGIC >
# MAGIC > 💡 **Solo usa librerías estándar** (`scikit-learn`, `mlflow`, `pandas`) — corre en
# MAGIC > cualquier ambiente, incluida la Free Edition. No necesitas instalar nada.
# MAGIC >
# MAGIC > 💡 Pronosticar una serie de tiempo es de cualquier industria: demanda de energía, tickets
# MAGIC > de soporte, tráfico web, ocupación hotelera. Cambia el dato, no el método.

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
        f"   Corre los módulos anteriores con estos mismos valores primero, o corrige el nombre "
        f"arriba.\n"
    )

spark.sql(f"USE `{catalogo}`.`{schema}`")

print(f"👤 Usuario       : {_usuario}")
print(f"📁 Trabajarás en : {catalogo}.{schema}")
print("✅ Espacio declarado y verificado. Nadie más toca este schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · Mira la serie que vas a pronosticar
# MAGIC
# MAGIC El pronóstico se hace **por país**. Prepara la serie por día y país (la usaremos como
# MAGIC datos de entrenamiento) y también la total, para verla.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- la serie por día y país: es lo que el modelo va a aprender
# MAGIC CREATE OR REPLACE VIEW serie_pais_diaria AS
# MAGIC SELECT dia, pais, SUM(monto) AS monto, SUM(unidades) AS unidades
# MAGIC FROM gold_ventas_diarias
# MAGIC GROUP BY dia, pais

# COMMAND ----------

# MAGIC %sql
# MAGIC -- míralas como gráfico de líneas (botón + → Visualization → Line, eje X dia, eje Y monto):
# MAGIC -- vas a ver la tendencia (sube con el tiempo) y los picos de fin de año (estacionalidad)
# MAGIC SELECT dia, SUM(monto) AS monto_total FROM serie_pais_diaria GROUP BY dia ORDER BY dia

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · De fechas a *features*
# MAGIC
# MAGIC Un modelo **no entiende una fecha** — entiende números. Así que convertimos cada día en
# MAGIC **features** que capturan lo que mueve las ventas:
# MAGIC
# MAGIC | Feature | Qué captura |
# MAGIC |---|---|
# MAGIC | `tendencia` | los días transcurridos desde el inicio → el crecimiento en el tiempo |
# MAGIC | `sin_anual`, `cos_anual` | la **estacionalidad anual** (los picos de fin de año) |
# MAGIC | `dia_semana` | el patrón semanal (fines de semana vs. entre semana) |
# MAGIC | `mes`, `dia_mes` | detalle del calendario |
# MAGIC | `pais` | cada país tiene su nivel de ventas (lo codificamos con one-hot) |
# MAGIC
# MAGIC > 💡 Con estas features, para pronosticar un día futuro **no necesitamos conocer el pasado
# MAGIC > reciente**: basta la fecha y el país. Eso hace el pronóstico simple y robusto.

# COMMAND ----------

import numpy as np
import pandas as pd

# la serie histórica, a pandas (es pequeña: ~18 meses × países)
serie = spark.table("serie_pais_diaria").toPandas()
serie["dia"] = pd.to_datetime(serie["dia"])
serie = serie.sort_values("dia").reset_index(drop=True)

# origen fijo de la tendencia: el primer día de la serie. Se usa igual para entrenar y para
# pronosticar, así el número 'tendencia' significa lo mismo en ambos momentos.
ORIGEN = serie["dia"].min()


def construir_features(df):
    """Convierte (dia, pais) en las señales numéricas que el modelo entiende."""
    d = pd.to_datetime(df["dia"])
    doy = d.dt.dayofyear
    return pd.DataFrame({
        "tendencia": (d - ORIGEN).dt.days,
        "sin_anual": np.sin(2 * np.pi * doy / 365.25),
        "cos_anual": np.cos(2 * np.pi * doy / 365.25),
        "dia_semana": d.dt.dayofweek,
        "mes": d.dt.month,
        "dia_mes": d.dt.day,
        "pais": df["pais"].values,
    })


X = construir_features(serie)
y = serie[["monto", "unidades"]]
print(f"✅ {len(X):,} filas de entrenamiento · features: {list(X.columns)}")
display(X.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · Entrena el modelo y regístralo con MLflow
# MAGIC
# MAGIC Un **Random Forest** aprende el patrón de las features. Pronostica **monto y unidades a la
# MAGIC vez** (es multi-salida). El flujo es el de cualquier proyecto de ML:
# MAGIC
# MAGIC 1. Partimos la serie en **train / test por tiempo** (los últimos 60 días son el test) para
# MAGIC    medir honestamente qué tan bueno es — con datos que el modelo **no** vio.
# MAGIC 2. **MLflow** registra la corrida sola: parámetros, la métrica **MAPE** (error porcentual
# MAGIC    medio) y el modelo.
# MAGIC 3. Reentrenamos con **toda** la serie y **registramos el modelo en Unity Catalog** con el
# MAGIC    alias `@champion` — queda como objeto gobernado, con versiones y linaje.

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

mlflow.set_registry_uri("databricks-uc")
NOMBRE_MODELO = f"{catalogo}.{schema}.modelo_forecast"

# el pipeline: one-hot al país, el resto de features pasa directo, y un Random Forest multi-salida
pre = ColumnTransformer([("pais", OneHotEncoder(handle_unknown="ignore"), ["pais"])],
                        remainder="passthrough")
modelo = Pipeline([("pre", pre),
                   ("rf", RandomForestRegressor(n_estimators=200, random_state=42))])

# 1 · train/test por tiempo (últimos 60 días = test)
corte = serie["dia"].max() - pd.Timedelta(days=60)
tr = serie[serie["dia"] <= corte]
te = serie[serie["dia"] > corte]

mlflow.set_experiment(f"/Users/{_usuario}/z2h_retail_forecast")
with mlflow.start_run(run_name="rf_forecast") as run:
    modelo.fit(construir_features(tr), tr[["monto", "unidades"]])
    pred_te = modelo.predict(construir_features(te))

    # MAPE del monto: error porcentual medio (0.10 = 10% de error)
    real = te["monto"].values
    mape = float(np.mean(np.abs(real - pred_te[:, 0]) / np.clip(np.abs(real), 1, None)))
    # desviación de los residuos: sirve para el intervalo del pronóstico
    sd_monto = float(np.std(real - pred_te[:, 0]))

    # 3 · reentrena con TODA la serie (para que el pronóstico use todo el histórico) y registra
    modelo.fit(X, y)
    mlflow.log_param("algoritmo", "RandomForest multi-salida (monto + unidades)")
    mlflow.log_param("n_estimators", 200)
    mlflow.log_metric("mape_monto", mape)
    mlflow.sklearn.log_model(modelo, "modelo", input_example=X.head(3))
    run_id = run.info.run_id

# registra el modelo en Unity Catalog y ponle el alias 'champion'
version = mlflow.register_model(f"runs:/{run_id}/modelo", NOMBRE_MODELO).version
MlflowClient().set_registered_model_alias(NOMBRE_MODELO, "champion", version)

print(f"✅ Entrenamiento terminado")
print(f"📊 MAPE del monto = {mape:.1%}   (más bajo es mejor)")
print(f"🔖 Modelo registrado como {NOMBRE_MODELO}@champion (versión {version})")
print()
print("👉 Ábrelo en el ícono de Experiments (probeta) y en Catalog → Models.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 5 · Pronostica 90 días y guárdalo para el dashboard
# MAGIC
# MAGIC Cargamos el modelo **por su alias** `@champion` (no por versión ni ruta: así el código no
# MAGIC cambia cuando reentrenas). Generamos las fechas futuras × país, predecimos, y guardamos el
# MAGIC resultado en `gold_pronostico_pais` — la tabla que consume el **Módulo 7** (dashboard,
# MAGIC Genie y app).

# COMMAND ----------

modelo_champion = mlflow.sklearn.load_model(f"models:/{NOMBRE_MODELO}@champion")

# fechas futuras: 90 días después del último dato, para cada país
ultimo_dia = serie["dia"].max()
paises = sorted(serie["pais"].unique())
fechas = pd.date_range(ultimo_dia + pd.Timedelta(days=1), periods=90, freq="D")
futuro = pd.DataFrame([(f, p) for f in fechas for p in paises], columns=["dia", "pais"])

pred = modelo_champion.predict(construir_features(futuro))
# nunca proyectar negativo (las ventas no lo son); el intervalo usa la desviación de los residuos
futuro["monto_pronostico"] = np.clip(pred[:, 0], 0, None).round(2)
futuro["monto_min"] = np.clip(pred[:, 0] - 1.28 * sd_monto, 0, None).round(2)
futuro["monto_max"] = (pred[:, 0] + 1.28 * sd_monto).round(2)
futuro["unidades_pronostico"] = np.clip(pred[:, 1], 0, None).round(0)
futuro["dia"] = futuro["dia"].dt.date

(spark.createDataFrame(
    futuro[["dia", "pais", "monto_pronostico", "monto_min", "monto_max", "unidades_pronostico"]])
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("gold_pronostico_pais"))

print(f"✅ Pronóstico guardado en gold_pronostico_pais ({len(futuro):,} filas · "
      f"{len(paises)} países × 90 días)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- comprueba: el pronóstico por país, listo para el dashboard
# MAGIC SELECT pais,
# MAGIC        ROUND(SUM(monto_pronostico), 0)    AS monto_90dias,
# MAGIC        ROUND(SUM(unidades_pronostico), 0) AS unidades_90dias
# MAGIC FROM gold_pronostico_pais
# MAGIC GROUP BY pais
# MAGIC ORDER BY monto_90dias DESC

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · Dos caminos para llegar a una predicción
# MAGIC
# MAGIC Hoy **entrenaste tu propio modelo** con MLflow. Es el camino cuando necesitas control: tus
# MAGIC features, tu lógica, el modelo gobernado en Unity Catalog con alias, versiones y linaje.
# MAGIC
# MAGIC Databricks también tiene un **camino rápido** para problemas comunes: las **AI Functions**,
# MAGIC funciones de SQL que traen IA a cualquiera que sepa consultar.
# MAGIC
# MAGIC | Función | Qué hace |
# MAGIC |---|---|
# MAGIC | `ai_forecast` | Pronostica una serie de tiempo con una sola consulta SQL |
# MAGIC | `ai_query` | Le manda un prompt a un LLM y devuelve la respuesta |
# MAGIC | `ai_classify` | Clasifica texto en categorías que tú das |
# MAGIC | `ai_analyze_sentiment` | Sentimiento de un texto (reseñas, comentarios) |
# MAGIC | `ai_translate`, `ai_summarize` | Traduce, resume |
# MAGIC
# MAGIC | | Modelo propio (hoy · MLflow) | AI Functions |
# MAGIC |---|---|---|
# MAGIC | Cuándo | Necesitas control y un modelo a tu medida | Problema común, quieres velocidad |
# MAGIC | Quién | Científico/ingeniero de ML | Cualquiera con SQL |
# MAGIC | Gobierno | Registrado en UC con alias `@champion`, versiones y linaje | La función corre en la plataforma |
# MAGIC
# MAGIC > 🎓 **La regla práctica:** si un modelo a medida te da control y lo necesitas, entrénalo y
# MAGIC > gobiérnalo con MLflow — es lo que hiciste. Si un problema común se resuelve con una
# MAGIC > función de SQL y está disponible en tu workspace, úsala. Ambos caminos conviven.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · Verifica tu checkpoint

# COMMAND ----------

resultados = []

try:
    n = spark.table("gold_pronostico_pais").count()
    resultados.append((n > 0, f"gold_pronostico_pais existe y tiene {n:,} filas de pronóstico"))
except Exception as e:
    resultados.append((False, f"No encuentro gold_pronostico_pais: {str(e)[:50]}"))

try:
    cols = spark.table("gold_pronostico_pais").columns
    tiene = "monto_pronostico" in cols and "unidades_pronostico" in cols
    resultados.append((tiene, "El pronóstico trae monto y unidades (dólares y cantidad)"))
except Exception:
    resultados.append((False, "No pude leer las columnas del pronóstico"))

try:
    fut = spark.sql("""
        SELECT COUNT(*) c FROM gold_pronostico_pais
        WHERE dia > (SELECT MAX(dia) FROM gold_ventas_diarias)
    """).collect()[0][0]
    resultados.append((fut > 0, f"El pronóstico proyecta {fut:,} filas hacia el futuro"))
except Exception:
    resultados.append((False, "No pude comprobar las fechas futuras"))

try:
    MlflowClient().get_model_version_by_alias(f"{catalogo}.{schema}.modelo_forecast", "champion")
    resultados.append((True, "El modelo está registrado en Unity Catalog como modelo_forecast@champion"))
except Exception:
    resultados.append((False, "No encuentro el modelo modelo_forecast@champion en UC"))

print("=" * 68)
print("  MÓDULO 6 · CHECKPOINT")
print("=" * 68)
for ok, msg in resultados:
    print(f"  {'✅' if ok else '❌'}  {msg}")
print("=" * 68)
if all(ok for ok, _ in resultados):
    print("""
  🎉 ¡Módulo 6 completo!
    · Convertiste una serie de tiempo en features
    · Entrenaste un modelo de forecast y mediste su error (MAPE) con MLflow
    · Lo registraste en Unity Catalog con alias @champion
    · Proyectaste 90 días por país y lo guardaste como tabla gold para el dashboard

  👉 En el Módulo 7 pones todo frente a quien decide: un dashboard con
     ventas + pronóstico + inventario, un Genie que responde preguntas,
     y una app de reabastecimiento.
""")
else:
    print("  ⚠️  Revisa los ❌. Lo más común: no corriste el Paso 4/5, o el schema no es el "
          "mismo donde generaste gold_ventas_diarias.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Lo que te llevas
# MAGIC
# MAGIC | Concepto | Dónde lo viviste |
# MAGIC |---|---|
# MAGIC | **De fecha a features** | tendencia + estacionalidad (sin/cos anual) |
# MAGIC | **Entrenar y evaluar** | train/test por tiempo, métrica **MAPE** con MLflow |
# MAGIC | **Modelo gobernado** | registrado en Unity Catalog con alias `@champion` |
# MAGIC | **Cargar por alias y predecir** | `models:/…@champion` → pronóstico 90 días |
# MAGIC | **Modelo propio vs AI Functions** | control a medida vs velocidad en SQL |
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC Tienes el pronóstico como tabla gold. En el **Módulo 7** —el último— lo pones frente a un
# MAGIC humano: un **dashboard** con ventas, pronóstico e inventario; un **Genie** que responde en
# MAGIC español; y una **app** que recomienda qué reabastecer.
