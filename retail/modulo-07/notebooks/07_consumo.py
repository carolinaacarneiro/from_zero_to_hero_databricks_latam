# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 📊 Módulo 7 · Consumo: Dashboard, Genie y App
# MAGIC
# MAGIC El último módulo — y el más divertido. Pones tus ventas, tu pronóstico y
# MAGIC tu inventario **frente a quien decide**. Es el módulo con **más tiempo para explorar**:
# MAGIC construyes lo esencial y luego juegas con la plataforma.
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - Tres bloques: **A** Dashboard · **B** Genie · **C** App.
# MAGIC - Casi todo se hace en la **UI**, no en este notebook. Acá está la guía y los prompts.
# MAGIC - 🙋 Trabado más de 3 minutos: levanta la mano.
# MAGIC
# MAGIC ## De dónde venimos
# MAGIC Tienes datos gold (M3), gobernados (M4) y un **pronóstico** por país (M6). Hoy los pones a
# MAGIC trabajar para un humano: gerencia, un analista comercial, el equipo de compras.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos. El último kilómetro —dashboard,
# MAGIC > lenguaje natural, app— es igual en cualquier industria.

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
spark.sql(f"USE `{catalogo}`.`{schema}`")
print(f"📁 Trabajarás sobre: {catalogo}.{schema}")
print("   Tablas clave:")
print("     · gold_ventas_diarias      (ventas históricas por día/país/categoría)")
print("     · gold_ventas_producto     (ranking de productos)")
print("     · gold_inventario_estado   (qué reabastecer)")
print("     · gold_pronostico_pais     (pronóstico de 90 días)")

# COMMAND ----------

# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # BLOQUE A · Dashboard
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC
# MAGIC ## Paso A1 · Crea el dashboard y sus datasets
# MAGIC
# MAGIC Menú izquierdo → **Dashboards → Create dashboard**. Nómbralo `Retail · <tu_usuario>`.
# MAGIC
# MAGIC En la pestaña **Data**, agrega estos datasets (uno por consulta). Son la base de todos los
# MAGIC gráficos — ajusta catálogo/schema si hace falta.

# COMMAND ----------

# MAGIC %md
# MAGIC **Dataset 1 · ventas mensuales (histórico)**
# MAGIC ```sql
# MAGIC SELECT date_trunc('month', dia) AS mes,
# MAGIC        SUM(monto) AS monto, SUM(unidades) AS unidades, SUM(utilidad) AS utilidad
# MAGIC FROM gold_ventas_diarias GROUP BY 1 ORDER BY 1
# MAGIC ```
# MAGIC
# MAGIC **Dataset 2 · pronóstico vs histórico (para la línea con proyección)**
# MAGIC ```sql
# MAGIC SELECT dia, SUM(monto) AS monto, NULL AS monto_pronostico FROM gold_ventas_diarias GROUP BY dia
# MAGIC UNION ALL
# MAGIC SELECT dia, NULL AS monto, SUM(monto_pronostico) AS monto_pronostico FROM gold_pronostico_pais GROUP BY dia
# MAGIC ```
# MAGIC
# MAGIC **Dataset 3 · top productos**
# MAGIC ```sql
# MAGIC SELECT nombre_producto, categoria, monto_total, unidades_vendidas, margen_pct
# MAGIC FROM gold_ventas_producto ORDER BY monto_total DESC LIMIT 20
# MAGIC ```
# MAGIC
# MAGIC **Dataset 4 · ventas por país y categoría**
# MAGIC ```sql
# MAGIC SELECT pais, categoria, SUM(monto) AS monto FROM gold_ventas_diarias GROUP BY pais, categoria
# MAGIC ```
# MAGIC
# MAGIC **Dataset 5 · inventario a reabastecer**
# MAGIC ```sql
# MAGIC SELECT categoria, pais, COUNT(*) AS productos_a_reabastecer
# MAGIC FROM gold_inventario_estado WHERE necesita_reabastecer GROUP BY categoria, pais
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso A2 · Arma el dashboard — dos caminos
# MAGIC
# MAGIC Vas a construir un dashboard de retail con estas visualizaciones. Elige el camino que
# MAGIC prefieras — el resultado es el mismo.
# MAGIC
# MAGIC | # | Visualización | Tipo | Dataset · campos |
# MAGIC |---|---|---|---|
# MAGIC | 1 | **Ventas totales** (KPI) | Counter | D1 · suma de `monto` |
# MAGIC | 2 | **Utilidad total** (KPI) | Counter | D1 · suma de `utilidad` |
# MAGIC | 3 | **Productos a reabastecer** (KPI) | Counter | D5 · suma de `productos_a_reabastecer` |
# MAGIC | 4 | **Ventas por mes** | Barras / línea | D1 · X `mes`, Y `monto` |
# MAGIC | 5 | **Histórico + pronóstico** ⭐ | Línea | D2 · X `dia`, Y `monto` y `monto_pronostico` |
# MAGIC | 6 | **Ventas por país y categoría** | Barras apiladas | D4 · X `pais`, Y `monto`, color `categoria` |
# MAGIC | 7 | **Top 20 productos** | Tabla | D3 · ordenada por `monto_total` |
# MAGIC | 8 | **Reabastecimiento por categoría** | Barras | D5 · X `categoria`, Y `productos_a_reabastecer` |
# MAGIC
# MAGIC ### 🅰️ A mano (arrastrando campos)
# MAGIC En la pestaña **Canvas**, agrega cada widget, elige el tipo y arrastra los campos.
# MAGIC
# MAGIC ### 🅱️ Con IA (pídeselo al asistente del dashboard)
# MAGIC En el editor hay un **asistente de IA** (ícono ✨/Assistant). En vez de arrastrar,
# MAGIC **describe lo que quieres** — incluyendo **de qué tablas** salen los datos.
# MAGIC
# MAGIC 👉 **Primero corre la celda de abajo:** te arma el prompt con **tu catálogo y schema
# MAGIC reales** ya escritos. Copia lo que imprime y pégalo en el asistente del dashboard.
# MAGIC
# MAGIC El prompt nombra las **4 tablas gold** que construiste, todas en tu schema
# MAGIC `<catálogo>.<schema>`:
# MAGIC
# MAGIC | Tabla | Qué tiene | La usan las visualizaciones |
# MAGIC |---|---|---|
# MAGIC | `gold_ventas_diarias` | ventas por día, país y categoría | 1, 2, 4, 5, 6 |
# MAGIC | `gold_ventas_producto` | totales por producto (monto, margen) | 7 |
# MAGIC | `gold_inventario_estado` | qué reabastecer, por producto y país | 3, 8 |
# MAGIC | `gold_pronostico_pais` | pronóstico de 90 días por país | 5 |
# MAGIC
# MAGIC Revisa lo que propone antes de aceptarlo: la IA acelera, pero **tú validas**. Si algo no
# MAGIC quedó como esperabas, ajústalo a mano o pídele el cambio en lenguaje natural.
# MAGIC
# MAGIC > 💡 La visualización 5 es la más interesante: en una sola línea de tiempo ves lo que
# MAGIC > **pasó** (histórico) y lo que **va a pasar** (pronóstico del M6). Ese empalme es el que
# MAGIC > el negocio quiere ver.

# COMMAND ----------

# Arma el prompt con TU catálogo y schema reales, y las tablas totalmente calificadas.
# Copia TODO lo que imprime abajo y pégalo en el asistente de IA del dashboard.
prompt = f"""Crea un dashboard de retail. Usa estas tablas de Unity Catalog (catálogo.schema.tabla):
- {catalogo}.{schema}.gold_ventas_diarias  (columnas: dia, pais, categoria, monto, unidades, utilidad)
- {catalogo}.{schema}.gold_ventas_producto (columnas: nombre_producto, categoria, marca, monto_total, unidades_vendidas, margen_pct)
- {catalogo}.{schema}.gold_inventario_estado (columnas: producto_id, nombre_producto, categoria, pais, stock_actual, necesita_reabastecer)
- {catalogo}.{schema}.gold_pronostico_pais (columnas: dia, pais, monto_pronostico, unidades_pronostico)

Crea estas visualizaciones:
1. Un contador con la suma total de monto (de gold_ventas_diarias).
2. Un contador con la suma total de utilidad (de gold_ventas_diarias).
3. Un contador con el total de productos donde necesita_reabastecer = true (de gold_inventario_estado).
4. Un gráfico de barras de monto por mes, usando la fecha dia (de gold_ventas_diarias).
5. Un gráfico de líneas que muestre, por dia, el monto histórico (de gold_ventas_diarias) y el monto_pronostico (de gold_pronostico_pais) en la misma gráfica.
6. Un gráfico de barras apiladas de monto por pais, con color por categoria (de gold_ventas_diarias).
7. Una tabla con los 20 productos de mayor monto_total (de gold_ventas_producto), mostrando nombre_producto, categoria, monto_total y margen_pct.
8. Un gráfico de barras del conteo de productos con necesita_reabastecer = true por categoria (de gold_inventario_estado).
Agrega un filtro por pais que afecte a todas las visualizaciones."""

print("📋 Copia el prompt de abajo y pégalo en el asistente de IA del dashboard:\n")
print("=" * 78)
print(prompt)
print("=" * 78)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso A3 · Un filtro por país, y publica
# MAGIC
# MAGIC Agrega un widget de **filtro** sobre `pais` y conéctalo a los gráficos. Ahora el usuario de
# MAGIC negocio explora sin tocar SQL: elige México y todo se actualiza.
# MAGIC
# MAGIC Botón **Publish**. Ábrelo como lo vería un gerente comercial: sin código, solo las ventas,
# MAGIC el pronóstico y qué hay que reabastecer. **Ese es el punto** — el dato llegó a quien decide.
# MAGIC
# MAGIC ### 🎈 Tiempo de explorar
# MAGIC Te sobra tiempo en este bloque a propósito. Prueba: cambia tipos de gráfico, agrega un
# MAGIC gráfico de **margen por categoría**, o uno de **días de cobertura** de inventario. Rompe
# MAGIC cosas, es tu dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # BLOQUE B · Genie — el de más aprendizaje
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC
# MAGIC ## Paso B1 · Crea el espacio Genie
# MAGIC
# MAGIC Menú → **Genie → New**. Agrégale tres tablas: `gold_ventas_diarias`,
# MAGIC `gold_ventas_producto` y `gold_pronostico_pais`. Nómbralo `Retail · <tu_usuario>`.
# MAGIC
# MAGIC Genie deja hacer preguntas en **lenguaje natural** sobre esas tablas — en español.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso B2 · Haz las 3 preguntas — en español
# MAGIC
# MAGIC Escríbelas tal cual en el chat de Genie:
# MAGIC
# MAGIC | # | Pregunta | Qué esperar |
# MAGIC |---|---|---|
# MAGIC | 1 | *¿Cuál fue el monto total de ventas por país?* | ✅ responde bien |
# MAGIC | 2 | *¿Qué categoría vendió más el último trimestre?* | ✅ responde bien |
# MAGIC | 3 | *¿Cuáles son mis productos estrella?* | ❌ **falla o inventa** |
# MAGIC
# MAGIC Las dos primeras funcionan porque preguntan por columnas que **existen**. La tercera falla
# MAGIC — y eso es lo que vamos a aprender.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso B3 · Diagnostica por qué falló
# MAGIC
# MAGIC **«Producto estrella» no existe en el esquema.** No hay ninguna columna que diga si un
# MAGIC producto es estrella. Genie no puede **adivinar** una definición de negocio: o responde
# MAGIC cualquier cosa, o inventa una interpretación.
# MAGIC
# MAGIC 👀 Esto es lo importante del módulo: **Genie es tan bueno como el contexto que le das.** No
# MAGIC es un oráculo — es un asistente que necesita que le enseñes tu negocio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso B4 · 📝 TU TURNO — enséñale con una instruction
# MAGIC
# MAGIC En el espacio Genie, abre **Instructions** y agrega esta definición:
# MAGIC
# MAGIC > *Un producto estrella es aquel que está entre el 10% de mayor monto_total y además tiene
# MAGIC > un margen_pct por encima de 0.35. Usa la tabla gold_ventas_producto.*
# MAGIC
# MAGIC Y agrega una **consulta de ejemplo**:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT nombre_producto, monto_total, margen_pct
# MAGIC FROM gold_ventas_producto
# MAGIC WHERE margen_pct > 0.35
# MAGIC ORDER BY monto_total DESC
# MAGIC LIMIT 20
# MAGIC ```
# MAGIC
# MAGIC **Vuelve a hacer la pregunta 3.** Ahora Genie tiene una definición y responde bien.
# MAGIC
# MAGIC 🎉 Acabas de **cultivar** a Genie: pasó de fallar a acertar, no porque cambió el modelo,
# MAGIC sino porque le diste el contexto de tu negocio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso B5 · Comentarios de columna (opcional, potente)
# MAGIC
# MAGIC Genie también aprovecha los **comentarios** de las columnas. Ejecuta esto y vuelve a
# MAGIC preguntar algo sobre el margen: Genie usa el comentario para entender mejor.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON COLUMN gold_ventas_producto.margen_pct IS
# MAGIC   'Proporción de utilidad sobre el monto vendido (0 a 1). Mide qué tan rentable es el producto';
# MAGIC COMMENT ON COLUMN gold_pronostico_pais.monto_pronostico IS
# MAGIC   'Venta proyectada en dólares para ese día y país, calculada con ai_forecast';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso B6 · 🎈 Explora Genie
# MAGIC Pregúntale lo que se te ocurra. Algunas ideas:
# MAGIC - *¿Cuánto se pronostica vender en Brasil los próximos 90 días?*
# MAGIC - *¿Qué marca tiene el mejor margen?*
# MAGIC - *Muéstrame las ventas por canal*
# MAGIC
# MAGIC Cuando una falle, piensa **por qué** — y si es una definición de negocio, enséñasela.

# COMMAND ----------

# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # BLOQUE C · App de reabastecimiento
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC
# MAGIC ## Paso C1 · Despliega la App
# MAGIC
# MAGIC La App `asistente_reabastecimiento` ya está lista en la carpeta `app/` de este módulo. Lee
# MAGIC `gold_inventario_estado` y `gold_pronostico_pais`, y recomienda **qué productos comprar**.
# MAGIC
# MAGIC Menú → **Apps → Create app → Deploy from folder**, apunta a la carpeta `app/`.
# MAGIC
# MAGIC **Antes de desplegar**, configura en `app.yaml` (o en la UI de la app) estas variables:
# MAGIC - `Z2H_CATALOGO` → tu catálogo
# MAGIC - `Z2H_SCHEMA` → tu schema (`retail_<tu_usuario>`)
# MAGIC - `Z2H_WAREHOUSE_ID` → el ID de un SQL warehouse (lo ves en **SQL Warehouses**)
# MAGIC
# MAGIC La celda de abajo te imprime los valores de catálogo y schema para copiarlos.

# COMMAND ----------

print("Configura estas variables en app/app.yaml antes de desplegar:\n")
print(f"  Z2H_CATALOGO = {catalogo}")
print(f"  Z2H_SCHEMA   = {schema}")
print(f"  Z2H_WAREHOUSE_ID = <ID de un SQL warehouse — míralo en 'SQL Warehouses'>")
print()
print("💡 Una Databricks App es una web servida por la plataforma, con el gobierno de UC")
print("   detrás. No montas servidores: subes el código y Databricks la sirve.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso C1.5 · ⚠️ Dale permisos a la App (¡esto es clave!)
# MAGIC
# MAGIC Cuando abras la App recién desplegada, es muy probable que veas un error como:
# MAGIC
# MAGIC > `INSUFFICIENT_PERMISSIONS ... User does not have SELECT on Table ... gold_inventario_estado`
# MAGIC
# MAGIC **No es un bug — es gobierno de Unity Catalog funcionando.** La clave:
# MAGIC
# MAGIC > 🔑 **Una Databricks App NO corre como tú.** Corre con la identidad de su propio
# MAGIC > **service principal** (un "usuario robot" que se crea junto con la App). Tú eres dueño de
# MAGIC > tus tablas, pero ese service principal **no tiene acceso** hasta que se lo das. Por eso la
# MAGIC > misma consulta funciona en el notebook (eres tú) y falla en la App (es el robot).
# MAGIC
# MAGIC ### Qué hacer
# MAGIC 1. Busca el **nombre del service principal** de tu App: menú **Apps → tu app →** pestaña
# MAGIC    **Authorization** (o **Configuration**). Es un ID tipo `xxxxxxxx-xxxx-xxxx-...`.
# MAGIC 2. Pégalo en la celda de abajo (`SP_DE_LA_APP`) y ejecútala: te imprime los `GRANT` exactos.
# MAGIC 3. Copia esos `GRANT` a una celda `%sql` (o al editor SQL) y ejecútalos.
# MAGIC 4. Además, dale al mismo service principal permiso **CAN USE** sobre tu **SQL warehouse**:
# MAGIC    **SQL Warehouses → tu warehouse → Permissions → Add → (el service principal) → Can use**.
# MAGIC 5. Recarga la App. Ahora sí lee las tablas.

# COMMAND ----------

# 📝 Pega aquí el service principal de tu App (lo ves en Apps → tu app → Authorization)
SP_DE_LA_APP = "PEGA_AQUI_EL_SERVICE_PRINCIPAL"

if SP_DE_LA_APP == "PEGA_AQUI_EL_SERVICE_PRINCIPAL":
    print("⚠️  Primero pega el service principal de tu App arriba y vuelve a ejecutar.")
    print("    Lo encuentras en: Apps → tu app → pestaña Authorization / Configuration.")
else:
    print("Copia estos GRANT a una celda %sql (o al editor SQL) y ejecútalos:\n")
    print("=" * 78)
    print(f"GRANT USE CATALOG ON CATALOG `{catalogo}` TO `{SP_DE_LA_APP}`;")
    print(f"GRANT USE SCHEMA ON SCHEMA `{catalogo}`.`{schema}` TO `{SP_DE_LA_APP}`;")
    print(f"GRANT SELECT ON SCHEMA `{catalogo}`.`{schema}` TO `{SP_DE_LA_APP}`;")
    print("=" * 78)
    print("\nY no olvides: SQL Warehouses → tu warehouse → Permissions → Can use → ese mismo SP.")

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 **Por qué esto importa más allá del taller:** es el mismo principio para *cualquier*
# MAGIC > automatización (un job, un pipeline, una app, un dashboard programado): corre como una
# MAGIC > identidad de servicio, y esa identidad necesita permisos explícitos. Darle acceso al
# MAGIC > **grupo/rol** correcto (RBAC del Módulo 4), no a personas, es lo que lo hace mantenible.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso C2 · 📝 TU TURNO — personalízala
# MAGIC
# MAGIC En `app/app.py` hay tres cosas para cambiar (búscalas, están marcadas con `# TODO`):
# MAGIC
# MAGIC 1. El **título** de la App
# MAGIC 2. Un **texto** de bienvenida
# MAGIC 3. El **umbral de urgencia**: a partir de cuántos días de cobertura se marca en 🔴 rojo
# MAGIC
# MAGIC Guarda y **vuelve a desplegar** (redeploy).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso C3 · Úsala ⭐
# MAGIC
# MAGIC Abre la App. Elige un **país** y una **categoría**. La App te muestra:
# MAGIC - Los productos que **hay que reabastecer** (stock bajo o que no cubre el lead time)
# MAGIC - Su **demanda diaria** reciente y sus **días de cobertura**
# MAGIC - Cuánto se **pronostica vender** en ese país los próximos 90 días
# MAGIC
# MAGIC 👀 Ese es el ciclo cerrado: del archivo crudo de la mañana a una **decisión de compra**,
# MAGIC apoyada en el pronóstico que tú generaste con `ai_forecast` hace 30 minutos.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🏁 Cierre del capstone
# MAGIC
# MAGIC Ejecuta esta celda: verifica que los artefactos del día existen.

# COMMAND ----------

resultados = []
for tabla, desc in [
    ("gold_ventas_diarias", "ventas históricas (base del dashboard y Genie)"),
    ("gold_pronostico_pais", "pronóstico de 90 días (base de la app)"),
    ("gold_inventario_estado", "estado de inventario (base de la app)"),
]:
    try:
        n = spark.table(tabla).count()
        resultados.append((n > 0, f"{tabla}: {n:,} filas — {desc}"))
    except Exception:
        resultados.append((False, f"No encuentro {tabla}"))

print("=" * 68)
print("  MÓDULO 7 · CIERRE DEL CAPSTONE")
print("=" * 68)
for ok, msg in resultados:
    print(f"  {'✅' if ok else '❌'}  {msg}")
print("""
  Además, confirma tú en la UI:
    ☐ Dashboard publicado con ventas + pronóstico + inventario
    ☐ Genie responde bien la pregunta 3 DESPUÉS de la instruction
    ☐ App desplegada que recomienda qué reabastecer
""")
print("=" * 68)
if all(ok for ok, _ in resultados):
    print("""
  🎉🎉 ¡CAPSTONE COMPLETO! 🎉🎉

  En un día, de punta a punta:
    M1 · tu primera tabla Delta y time travel
    M2 · ingesta con Auto Loader → bronze
    M3 · pipeline declarativo → silver y gold
    M4 · gobierno: row filter por país y máscara del costo
    M5 · orquestación: un job que corre solo
    M6 · pronóstico de ventas con ai_forecast (solo SQL)
    M7 · dashboard + Genie + app de reabastecimiento

  Empezaste con archivos crudos. Terminas con una app que recomienda
  qué comprar, apoyada en tu propio pronóstico. Todo esto lo construiste HOY.
""")
else:
    print("  ⚠️  Revisa los ❌. Para la app y el pronóstico necesitas el Módulo 6.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Lo que te llevas
# MAGIC
# MAGIC | Concepto | Dónde lo viviste |
# MAGIC |---|---|
# MAGIC | **Dashboard** para métricas recurrentes | Ventas + pronóstico + inventario |
# MAGIC | **Genie se cultiva**, no es magia | Lo viste fallar y lo corregiste |
# MAGIC | **App** para una decisión concreta | Qué reabastecer, con el pronóstico detrás |
# MAGIC | **Cada herramienta para su audiencia** | Gerencia · analista · compras |
# MAGIC | **El ciclo se cierra** | Del archivo crudo a la decisión de compra, en un día |
# MAGIC
# MAGIC ## Y con esto terminamos
# MAGIC
# MAGIC Gracias por el día. Guarda los enlaces de tus artefactos — y llévate este notebook: es
# MAGIC tuyo, con todo lo que construiste.
# MAGIC
# MAGIC **De Zero a Hero, de verdad.** 🚀
