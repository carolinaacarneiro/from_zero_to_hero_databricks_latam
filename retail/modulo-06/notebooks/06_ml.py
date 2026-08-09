# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🔮 Módulo 6 · Pronóstico de ventas con AI Functions
# MAGIC
# MAGIC Vas a pronosticar las ventas de los próximos 90 días —en dólares y en
# MAGIC unidades— **sin entrenar ningún modelo a mano**. Solo SQL.
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - Ejecuta celda por celda. Donde dice **📝 TU TURNO**, completas una línea.
# MAGIC - 🙋 Trabado más de 3 minutos: levanta la mano.
# MAGIC
# MAGIC ## De dónde venimos
# MAGIC Tienes la serie de tiempo `gold_ventas_diarias` (M3): ventas por día, país y categoría, con
# MAGIC tendencia y estacionalidad reales. Hoy la proyectas al futuro.
# MAGIC
# MAGIC ## El diferencial de este módulo: **AI Functions**
# MAGIC Pronosticar solía requerir un científico de datos, una librería, entrenar y evaluar un
# MAGIC modelo. Databricks tiene **`ai_forecast`**: una **función de SQL** que hace el pronóstico
# MAGIC por ti. Le das una serie de tiempo y te devuelve la proyección con intervalos de
# MAGIC confianza. **Cualquiera que sepa un `SELECT` puede pronosticar.**
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos.
# MAGIC >
# MAGIC > ⚙️ **Requisitos de `ai_forecast` (léelo antes de correr el Paso 2):**
# MAGIC > 1. Un **SQL warehouse Pro o Serverless** (no un warehouse Classic, no un cluster común).
# MAGIC > 2. Usamos la **versión 1** de la función (`version => '1'`), que es la disponible por defecto.
# MAGIC >    La *versión 2* añade cosas (feriados, covariables) pero exige el preview «Predictive AI
# MAGIC >    Functions»; para el taller, la v1 basta y evita ese requisito.
# MAGIC >
# MAGIC > Si aun así ves el error
# MAGIC > `UNSUPPORTED_FEATURE.AI_FUNCTION_PREVIEW ... ai_forecast is in preview and currently
# MAGIC > disabled`, tu workspace tiene la función deshabilitada del todo. Pídele a un admin/TA que
# MAGIC > active «Predictive AI Functions» en **Settings → Previews** (toma segundos), o usa el
# MAGIC > **Plan B sin `ai_forecast`** del Paso 4B, que produce el mismo resultado para que puedas
# MAGIC > seguir con el dashboard y la app del Módulo 7.
# MAGIC >
# MAGIC > 💡 Pronosticar una serie de tiempo es de cualquier industria: demanda de energía, tickets
# MAGIC > de soporte, tráfico web, ocupación hotelera. Cambia el dato, no la función.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 0 · Tu espacio

# COMMAND ----------

import re
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
schema = "retail_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())
_ocultos = ("system", "samples", "__databricks_internal", "hive_metastore")
catalogo = None
for c in [r[0] for r in spark.sql("SHOW CATALOGS").collect() if r[0].lower() not in _ocultos]:
    try:
        if schema in [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{c}`").collect()]:
            catalogo = c
            break
    except Exception:
        continue
if catalogo is None:
    raise Exception("No encontré tu schema del taller. ¿Corriste los módulos 2 y 3?")
spark.sql(f"USE `{catalogo}`.`{schema}`")
print(f"📁 {catalogo}.{schema}")
print("   Serie base del pronóstico: gold_ventas_diarias")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 · Mira la serie que vas a pronosticar
# MAGIC
# MAGIC Primero, prepara la serie **total por día** (sumando todos los países y categorías). Un
# MAGIC pronóstico necesita **un valor por día**. Esta vista la usaremos varias veces.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW serie_total_diaria AS
# MAGIC SELECT
# MAGIC     dia,
# MAGIC     SUM(monto)    AS monto,
# MAGIC     SUM(unidades) AS unidades
# MAGIC FROM gold_ventas_diarias
# MAGIC GROUP BY dia

# COMMAND ----------

# MAGIC %md
# MAGIC Mírala como gráfico de líneas: **botón + → Visualization → Line**, eje X `dia`, eje Y
# MAGIC `monto`. Vas a ver la tendencia (sube con el tiempo) y los picos de fin de año.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM serie_total_diaria ORDER BY dia

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · El pronóstico total ⭐
# MAGIC ## El momento clave del módulo
# MAGIC
# MAGIC ## Qué hace `ai_forecast`
# MAGIC Le pasas una tabla con una serie de tiempo y le dices:
# MAGIC
# MAGIC | Argumento | Qué es | Nuestro valor |
# MAGIC |---|---|---|
# MAGIC | `TABLE(...)` | la serie histórica | `serie_total_diaria` |
# MAGIC | `horizon` | hasta qué fecha proyectar | 90 días después del último dato |
# MAGIC | `time_col` | la columna de fecha | `'dia'` |
# MAGIC | `value_col` | qué pronosticar (¡puede ser varias!) | `'monto'` y `'unidades'` |
# MAGIC | `parameters` | ajustes del pronóstico (JSON) | `'{"global_floor": 0}'` — nunca proyectar negativo |
# MAGIC | `version` | qué versión de la función usar | `'1'` (la disponible por defecto) |
# MAGIC
# MAGIC Por cada columna que pronosticas, devuelve tres: `{columna}_forecast` (la proyección),
# MAGIC `{columna}_upper` y `{columna}_lower` (el rango probable). Así el negocio ve no solo *«se
# MAGIC venderá X»* sino *«entre X y Y»*.
# MAGIC
# MAGIC > 💡 **¿Por qué `global_floor: 0`?** Las ventas y las unidades **nunca son negativas**. Sin
# MAGIC > ese piso, el modelo podría proyectar un valor bajo cero en un día flojo — y un *«vamos a
# MAGIC > vender -200 dólares»* no tiene sentido. `global_floor` le pone el piso en 0. *(En la versión
# MAGIC > 2 esto se llama `positive_only`; en la v1 se hace con este parámetro.)*
# MAGIC
# MAGIC Ejecuta la celda. **Con una sola función pronosticas dólares y unidades a la vez.**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM AI_FORECAST(
# MAGIC     TABLE(serie_total_diaria),
# MAGIC     horizon    => (SELECT DATE_ADD(MAX(dia), 90) FROM serie_total_diaria),
# MAGIC     time_col   => 'dia',
# MAGIC     value_col  => ARRAY('monto', 'unidades'),
# MAGIC     parameters => '{"global_floor": 0}',
# MAGIC     version    => '1'
# MAGIC )
# MAGIC ORDER BY dia

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Acabas de pronosticar 90 días con una función de SQL
# MAGIC
# MAGIC Mira el resultado como gráfico: eje X `dia`, eje Y `monto_forecast`. Vas a ver la
# MAGIC proyección continuar la tendencia y la estacionalidad que la serie traía.
# MAGIC
# MAGIC Piensa en lo que **no** tuviste que hacer: elegir un algoritmo, separar train/test,
# MAGIC ajustar hiperparámetros, evaluar métricas. `ai_forecast` se encarga. Eso es una **AI
# MAGIC Function**: capacidad de IA empaquetada como una función de SQL.
# MAGIC
# MAGIC > 💡 `monto_upper` y `monto_lower` son el **intervalo de confianza**: el rango dentro del
# MAGIC > cual es probable que caiga la venta real. Un pronóstico honesto no da un número mágico,
# MAGIC > da un rango — y planeas el inventario para el escenario que te convenga.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · 📝 TU TURNO — pronóstico por país
# MAGIC
# MAGIC El pronóstico total está bien, pero el negocio quiere saber **cuánto venderá cada país**.
# MAGIC `ai_forecast` lo hace con **un argumento más**: `group_col`. Le dices por qué columna
# MAGIC agrupar y calcula **una serie independiente por grupo**.
# MAGIC
# MAGIC Primero, la serie por día **y país**:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW serie_pais_diaria AS
# MAGIC SELECT dia, pais, SUM(monto) AS monto, SUM(unidades) AS unidades
# MAGIC FROM gold_ventas_diarias
# MAGIC GROUP BY dia, pais

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora completa el `ai_forecast` agregando `group_col => 'pais'`. Reemplaza el `TODO`.
# MAGIC
# MAGIC <details>
# MAGIC <summary>💡 Solución</summary>
# MAGIC
# MAGIC <pre>SELECT * FROM AI_FORECAST(
# MAGIC     TABLE(serie_pais_diaria),
# MAGIC     horizon   => (SELECT DATE_ADD(MAX(dia), 90) FROM serie_pais_diaria),
# MAGIC     time_col  => 'dia',
# MAGIC     value_col => ARRAY('monto', 'unidades'),
# MAGIC     group_col => 'pais',
# MAGIC     parameters => '{"global_floor": 0}',
# MAGIC     version   => '1'
# MAGIC )
# MAGIC ORDER BY pais, dia</pre>
# MAGIC </details>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: agrega la línea  group_col => 'pais',  (con su coma) donde se indica, y ejecuta
# MAGIC SELECT * FROM AI_FORECAST(
# MAGIC     TABLE(serie_pais_diaria),
# MAGIC     horizon   => (SELECT DATE_ADD(MAX(dia), 90) FROM serie_pais_diaria),
# MAGIC     time_col  => 'dia',
# MAGIC     value_col => ARRAY('monto', 'unidades'),
# MAGIC     -- TODO: group_col => 'pais',
# MAGIC     parameters => '{"global_floor": 0}',
# MAGIC     version   => '1'
# MAGIC )
# MAGIC ORDER BY dia

# COMMAND ----------

# MAGIC %md
# MAGIC 👀 Con `group_col`, el resultado trae la columna `pais` y una serie proyectada **por cada
# MAGIC país**. La misma función, escalada de 1 serie a 6, sin escribir un bucle. Cambiando
# MAGIC `group_col` por `'categoria'` tendrías el pronóstico por categoría; por `'producto_id'`,
# MAGIC por producto. Es un argumento, no un proyecto.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · Guarda el pronóstico para el dashboard
# MAGIC
# MAGIC El Módulo 7 (dashboard, Genie y app) necesita el pronóstico **como tabla**. Guardamos el
# MAGIC pronóstico por país en `gold_pronostico_pais`. Ejecuta la celda (ya está resuelta).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_pronostico_pais AS
# MAGIC SELECT
# MAGIC     dia,
# MAGIC     pais,
# MAGIC     ROUND(monto_forecast, 2)    AS monto_pronostico,
# MAGIC     ROUND(monto_lower, 2)       AS monto_min,
# MAGIC     ROUND(monto_upper, 2)       AS monto_max,
# MAGIC     ROUND(unidades_forecast, 0) AS unidades_pronostico
# MAGIC FROM AI_FORECAST(
# MAGIC     TABLE(serie_pais_diaria),
# MAGIC     horizon   => (SELECT DATE_ADD(MAX(dia), 90) FROM serie_pais_diaria),
# MAGIC     time_col  => 'dia',
# MAGIC     value_col => ARRAY('monto', 'unidades'),
# MAGIC     group_col => 'pais',
# MAGIC     parameters => '{"global_floor": 0}',
# MAGIC     version   => '1'
# MAGIC )

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
# MAGIC # 🅱️ Paso 4B · Plan B — SOLO si `ai_forecast` te dio error de preview
# MAGIC
# MAGIC ¿Viste el error `AI_FUNCTION_PREVIEW ... ai_forecast is in preview and currently disabled`?
# MAGIC Es que **falta activar el preview** «Predictive AI Functions» (un admin lo hace en
# MAGIC **Settings → Previews**). Si no puedes activarlo ahora, **corre esta celda**: genera el mismo
# MAGIC `gold_pronostico_pais` con un pronóstico **estacional simple en SQL puro**, para que el
# MAGIC dashboard y la app del Módulo 7 funcionen igual.
# MAGIC
# MAGIC > 💡 **Qué hace:** para cada país y cada día futuro, reutiliza la venta del **mismo día hace
# MAGIC > un año** (captura la estacionalidad anual que tienen los datos) y la ajusta por un **factor
# MAGIC > de crecimiento** (últimos 90 días vs. los mismos 90 días del año anterior). Es un
# MAGIC > *seasonal-naive*: no es tan bueno como `ai_forecast`, pero es honesto y funciona sin preview.
# MAGIC >
# MAGIC > 👉 Si `ai_forecast` **sí** funcionó, **no corras esta celda** — ya tienes el pronóstico bueno.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_pronostico_pais AS
# MAGIC WITH ultimo AS (SELECT MAX(dia) AS max_dia FROM gold_ventas_diarias),
# MAGIC serie AS (
# MAGIC   SELECT dia, pais, SUM(monto) AS monto, SUM(unidades) AS unidades
# MAGIC   FROM gold_ventas_diarias GROUP BY dia, pais
# MAGIC ),
# MAGIC -- factor de crecimiento por país: últimos 90 días vs los mismos 90 días de hace un año
# MAGIC crec AS (
# MAGIC   SELECT s.pais,
# MAGIC     SUM(CASE WHEN s.dia > date_sub(u.max_dia, 90) THEN s.monto ELSE 0 END) AS reciente,
# MAGIC     SUM(CASE WHEN s.dia > date_sub(u.max_dia, 454)
# MAGIC              AND s.dia <= date_sub(u.max_dia, 364) THEN s.monto ELSE 0 END) AS hace_un_anio
# MAGIC   FROM serie s CROSS JOIN ultimo u GROUP BY s.pais
# MAGIC ),
# MAGIC futuras AS (
# MAGIC   SELECT explode(sequence(date_add(u.max_dia, 1), date_add(u.max_dia, 90), interval 1 day)) AS dia
# MAGIC   FROM ultimo u
# MAGIC ),
# MAGIC paises AS (SELECT DISTINCT pais FROM serie)
# MAGIC SELECT
# MAGIC   f.dia,
# MAGIC   p.pais,
# MAGIC   ROUND(COALESCE(h.monto, 0) * COALESCE(NULLIF(c.reciente,0)/NULLIF(c.hace_un_anio,0), 1.0), 2)
# MAGIC     AS monto_pronostico,
# MAGIC   ROUND(COALESCE(h.monto, 0) * COALESCE(NULLIF(c.reciente,0)/NULLIF(c.hace_un_anio,0), 1.0) * 0.88, 2)
# MAGIC     AS monto_min,
# MAGIC   ROUND(COALESCE(h.monto, 0) * COALESCE(NULLIF(c.reciente,0)/NULLIF(c.hace_un_anio,0), 1.0) * 1.12, 2)
# MAGIC     AS monto_max,
# MAGIC   ROUND(COALESCE(h.unidades, 0) * COALESCE(NULLIF(c.reciente,0)/NULLIF(c.hace_un_anio,0), 1.0), 0)
# MAGIC     AS unidades_pronostico
# MAGIC FROM futuras f
# MAGIC CROSS JOIN paises p
# MAGIC LEFT JOIN serie h ON h.pais = p.pais AND h.dia = date_sub(f.dia, 364)
# MAGIC LEFT JOIN crec c ON c.pais = p.pais

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 5 · AI Functions y MLflow — cuándo cada uno
# MAGIC
# MAGIC `ai_forecast` es **una** de la familia de **AI Functions**: funciones de SQL que traen IA a
# MAGIC cualquiera que sepa consultar. Otras que vale conocer:
# MAGIC
# MAGIC | Función | Qué hace |
# MAGIC |---|---|
# MAGIC | `ai_forecast` | Pronostica una serie de tiempo *(lo que hiciste)* |
# MAGIC | `ai_query` | Le manda un prompt a un LLM y devuelve la respuesta |
# MAGIC | `ai_classify` | Clasifica texto en categorías que tú das |
# MAGIC | `ai_analyze_sentiment` | Sentimiento de un texto (reseñas, comentarios) |
# MAGIC | `ai_translate`, `ai_summarize` | Traduce, resume |
# MAGIC
# MAGIC > 💡 Ejemplo con `ai_query`: podrías pedirle *«clasifica esta reseña de producto como
# MAGIC > positiva/negativa»* sobre miles de reseñas, en una sola consulta SQL. La IA deja de ser
# MAGIC > un proyecto aparte y se vuelve una columna más.
# MAGIC
# MAGIC ### ¿Y MLflow? ¿Cuándo entrenar un modelo propio?
# MAGIC
# MAGIC Las AI Functions son el **camino rápido**: resuelven problemas comunes (pronóstico,
# MAGIC clasificación, texto) sin entrenar nada. Pero cuando necesitas un modelo **a tu medida**
# MAGIC —con tus propias features, tu propia lógica de negocio— entrenas uno y lo gestionas con
# MAGIC **MLflow**:
# MAGIC
# MAGIC | | AI Functions (hoy) | MLflow (modelo propio) |
# MAGIC |---|---|---|
# MAGIC | Cuándo | Problema común, quieres velocidad | Necesitas un modelo a medida |
# MAGIC | Quién | Cualquiera con SQL | Científico/ingeniero de ML |
# MAGIC | Esfuerzo | Una función | Entrenar, evaluar, registrar, versionar |
# MAGIC | Gobierno | La función corre en la plataforma | El modelo se registra en Unity Catalog con alias `@champion`, versiones y linaje |
# MAGIC
# MAGIC Con **MLflow** registras cada corrida (parámetros, métricas, el modelo), comparas
# MAGIC versiones, y promueves la mejor con un alias. El modelo queda como **objeto gobernado de
# MAGIC Unity Catalog**, con permisos y linaje igual que una tabla — y una app o un job lo consulta
# MAGIC por su alias sin saber qué versión es.
# MAGIC
# MAGIC > 🎓 **La regla práctica:** empieza por la AI Function. Si resuelve el problema, listo. Si
# MAGIC > necesitas más control, entrenas con MLflow. Hoy, para pronosticar ventas, `ai_forecast`
# MAGIC > sobra — y por eso es el camino correcto.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · Verifica tu checkpoint

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

print("=" * 68)
print("  MÓDULO 6 · CHECKPOINT")
print("=" * 68)
for ok, msg in resultados:
    print(f"  {'✅' if ok else '❌'}  {msg}")
print("=" * 68)
if all(ok for ok, _ in resultados):
    print("""
  🎉 ¡Módulo 6 completo!
    · Pronosticaste las ventas totales con ai_forecast — solo SQL
    · Lo escalaste a un pronóstico por país con un argumento
    · Proyectaste dólares y unidades a la vez
    · Guardaste el pronóstico como tabla gold para el dashboard

  👉 En el Módulo 7 pones todo frente a quien decide: un dashboard con
     ventas + pronóstico + inventario, un Genie que responde preguntas,
     y una app de reabastecimiento.
""")
else:
    print("  ⚠️  Revisa los ❌. Si ai_forecast dio error de compute, usa un SQL warehouse Pro/Serverless.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Lo que te llevas
# MAGIC
# MAGIC | Concepto | Dónde lo viviste |
# MAGIC |---|---|
# MAGIC | **AI Functions**: IA como función de SQL | `ai_forecast` en dos consultas |
# MAGIC | **Pronóstico con intervalo** | `_forecast`, `_upper`, `_lower` |
# MAGIC | **Varias métricas a la vez** | dólares y unidades en una llamada |
# MAGIC | **Escala con `group_col`** | de 1 serie a 6 países, sin bucles |
# MAGIC | **AI Functions vs MLflow** | rápido vs a medida — cuándo cada uno |
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC Tienes el pronóstico como tabla gold. En el **Módulo 7** —el último— lo pones frente a un
# MAGIC humano: un **dashboard** con ventas, pronóstico e inventario; un **Genie** que responde en
# MAGIC español; y una **app** que recomienda qué reabastecer.
