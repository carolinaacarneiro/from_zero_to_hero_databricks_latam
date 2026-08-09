# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 📊 Módulo 7 · Consumo: Dashboard, Genie y App
# MAGIC
# MAGIC El último módulo. Pones tus datos y tu modelo **frente a quien decide**.
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - Tres bloques: **A** Dashboard · **B** Genie · **C** App.
# MAGIC - La mayor parte se hace en la **UI**, no en este notebook. Acá está la guía.
# MAGIC - 🙋 Trabado más de 3 minutos: levanta la mano.
# MAGIC
# MAGIC ## De dónde venimos
# MAGIC Tienes datos gold (M3), gobernados (M4), y un modelo `@champion` **servido como endpoint**
# MAGIC (M6). Hoy los pones a trabajar para un humano.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos. El último kilómetro —dashboard,
# MAGIC > lenguaje natural, app— es igual en cualquier industria.

# COMMAND ----------

import re
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
schema = "fin_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())
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
print(f"   Tablas clave: gold_riesgo_diario · silver_transacciones · modelo_fraude@champion")

# COMMAND ----------

# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # BLOQUE A · Dashboard
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC
# MAGIC ## Paso A1 · Crea el dashboard
# MAGIC
# MAGIC Menú izquierdo → **Dashboards → Create dashboard**. Nómbralo `Fraude · <tu_usuario>`.
# MAGIC
# MAGIC En la pestaña **Data**, agrega un dataset con esta consulta (ajusta catálogo/schema):
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM gold_riesgo_diario
# MAGIC ```
# MAGIC
# MAGIC Esto alimenta todos los gráficos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso A2 · 📝 TU TURNO — 4 visualizaciones
# MAGIC
# MAGIC Hay **dos caminos**. Elige el que prefieras — el resultado es el mismo dashboard.
# MAGIC
# MAGIC ### 🅰️ A mano (arrastrando campos)
# MAGIC En la pestaña **Canvas**, agrega cuatro widgets. Para cada uno, elige el tipo y arrastra
# MAGIC los campos:
# MAGIC
# MAGIC | # | Visualización | Tipo | Campos |
# MAGIC |---|---|---|---|
# MAGIC | 1 | **Tasa de fraude** (último mes) | Counter / KPI | promedio de `tasa_fraude` |
# MAGIC | 2 | **Monto de fraude por día** | Línea | X: `dia` · Y: suma de `monto_fraude` |
# MAGIC | 3 | **Fraude por categoría** | Barras | X: `categoria_comercio` · Y: suma de `transacciones_fraude` |
# MAGIC | 4 | **Top 20 días de más fraude** | Tabla | orden por `monto_fraude` desc, límite 20 |
# MAGIC
# MAGIC ### 🅱️ Con IA (pídeselo al asistente del dashboard)
# MAGIC En el editor del dashboard hay un **asistente de IA** (el ícono ✨/Assistant). En vez de
# MAGIC arrastrar, **describe lo que quieres** y él crea la visualización. Copia y pega este
# MAGIC prompt, una petición a la vez:
# MAGIC
# MAGIC > *Sobre el dataset gold_riesgo_diario, crea estas visualizaciones:*
# MAGIC > *1. Un indicador (counter) con el promedio de tasa_fraude.*
# MAGIC > *2. Un gráfico de líneas con la suma de monto_fraude por dia.*
# MAGIC > *3. Un gráfico de barras con la suma de transacciones_fraude por categoria_comercio,*
# MAGIC > *ordenado de mayor a menor.*
# MAGIC > *4. Una tabla con las 20 filas de mayor monto_fraude.*
# MAGIC
# MAGIC Revisa lo que propone antes de aceptarlo — igual que con Genie Code en el Módulo 1: la
# MAGIC IA acelera, pero **tú validas**. Si una visualización no quedó como esperabas, ajústala a
# MAGIC mano o pídele el cambio en lenguaje natural (*«pon el eje Y en millones»*).
# MAGIC
# MAGIC > 💡 Cada widget es una pregunta de negocio. El KPI responde *«¿cómo vamos?»* de un
# MAGIC > vistazo; la serie de tiempo, *«¿está subiendo?»*; las barras, *«¿dónde se concentra?»*.
# MAGIC > Sea a mano o con IA, el objetivo es el mismo — y saber pedirlo bien es una habilidad.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso A3 · Un filtro de fechas
# MAGIC
# MAGIC Agrega un widget de **filtro** de rango de fechas sobre `dia`, y conéctalo a los
# MAGIC gráficos. Ahora el usuario de negocio explora sin tocar SQL: mueve el rango y todo se
# MAGIC actualiza.
# MAGIC
# MAGIC ## Paso A4 · Publica
# MAGIC
# MAGIC Botón **Publish**. Ábrelo como lo vería un gerente: sin código, solo los números y los
# MAGIC gráficos. **Ese es el punto** — el dato llegó a quien decide.

# COMMAND ----------

# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # BLOQUE B · Genie — el de más aprendizaje
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC
# MAGIC ## Paso B1 · Crea el espacio Genie
# MAGIC
# MAGIC Menú → **Genie → New**. Agrégale dos tablas: `gold_riesgo_diario` y
# MAGIC `silver_transacciones`. Nómbralo `Fraude · <tu_usuario>`.
# MAGIC
# MAGIC Genie deja hacer preguntas en **lenguaje natural** sobre esas tablas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso B2 · Haz las 3 preguntas — en español
# MAGIC
# MAGIC Escríbelas tal cual en el chat de Genie:
# MAGIC
# MAGIC | # | Pregunta | Qué esperar |
# MAGIC |---|---|---|
# MAGIC | 1 | *¿Cuál fue la tasa de fraude promedio?* | ✅ responde bien |
# MAGIC | 2 | *¿Qué categoría de comercio tiene más fraude?* | ✅ responde bien |
# MAGIC | 3 | *¿Cuántas transacciones sospechosas hubo esta semana?* | ❌ **falla o inventa** |
# MAGIC
# MAGIC Las dos primeras funcionan porque preguntan por columnas que **existen**. La tercera
# MAGIC falla — y eso es lo que vamos a aprender.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso B3 · Diagnostica por qué falló
# MAGIC
# MAGIC **«Transacción sospechosa» no existe en el esquema.** No hay ninguna columna que diga si
# MAGIC una transacción es sospechosa. Genie no puede **adivinar** una definición de negocio: o
# MAGIC responde cualquier cosa, o inventa una interpretación.
# MAGIC
# MAGIC 👀 Esto es lo importante del módulo: **Genie es tan bueno como el contexto que le das.**
# MAGIC No es un oráculo — es un asistente que necesita que le enseñes tu negocio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso B4 · 📝 TU TURNO — enséñale con una instruction
# MAGIC
# MAGIC En el espacio Genie, abre **Instructions** y agrega esta definición:
# MAGIC
# MAGIC > *Una transacción sospechosa es aquella cuyo monto supera 3 veces el promedio histórico
# MAGIC > de su categoría de comercio, o cuyo país es distinto de CO.*
# MAGIC
# MAGIC Y agrega una **consulta de ejemplo** que muestre cómo se calcula:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(*) FROM silver_transacciones
# MAGIC WHERE pais <> 'CO'
# MAGIC ```
# MAGIC
# MAGIC **Vuelve a hacer la pregunta 3.** Ahora Genie tiene una definición y responde bien.
# MAGIC
# MAGIC 🎉 Acabas de **cultivar** a Genie: pasó de fallar a acertar, no porque cambió el modelo,
# MAGIC sino porque le diste el contexto de tu negocio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso B5 · Comentarios de columna
# MAGIC
# MAGIC Genie también aprovecha los **comentarios** de las columnas. Ejecuta esto y vuelve a
# MAGIC preguntar algo sobre `tasa_fraude`: Genie usa el comentario para entender mejor.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON COLUMN gold_riesgo_diario.tasa_fraude IS
# MAGIC   'Proporción de transacciones fraudulentas sobre el total del grupo (0 a 1)';
# MAGIC COMMENT ON COLUMN gold_riesgo_diario.ticket_promedio IS
# MAGIC   'Monto promedio por transacción del grupo, en pesos colombianos';

# COMMAND ----------

# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC # BLOQUE C · App
# MAGIC # ═══════════════════════════════════════════════════════
# MAGIC
# MAGIC ## Paso C1 · Requisito: tu modelo servido como endpoint
# MAGIC
# MAGIC La App `revisor_transacciones` **consulta tu Serving Endpoint** (el que creaste en el
# MAGIC **Paso 7 del Módulo 6**) para calificar transacciones. Antes de seguir, confirma que ese
# MAGIC endpoint está **Ready** (menú **Serving**).
# MAGIC
# MAGIC > 💡 **¿Por qué un endpoint y no cargar el modelo en la App?** Porque el endpoint se encarga
# MAGIC > de las versiones de librerías (numpy, scikit-learn…) por ti. La App solo hace una **llamada
# MAGIC > HTTP** — no necesita tener instaladas las mismas versiones con que entrenaste el modelo.
# MAGIC > Es más simple, más robusto, y es como se consume un modelo en producción.
# MAGIC
# MAGIC ## Paso C1.1 · Despliega la App
# MAGIC
# MAGIC Menú → **Apps → Create app → Deploy from folder**, apunta a la carpeta `app/`.
# MAGIC
# MAGIC **Antes de desplegar**, configura en `app/app.yaml` (o en la UI de la app) estas variables:
# MAGIC - `Z2H_ENDPOINT` → el **nombre** de tu serving endpoint (ej. `zerotohero`)
# MAGIC - `Z2H_CATALOGO`, `Z2H_SCHEMA`, `Z2H_WAREHOUSE_ID` → para los **KPIs** del panorama
# MAGIC   *(la App lee `gold_riesgo_diario` para mostrar transacciones, fraudes, tasa y monto)*
# MAGIC
# MAGIC La celda de abajo te imprime los valores exactos para copiarlos.
# MAGIC
# MAGIC > 💡 Una Databricks App es una web servida por la plataforma, con el gobierno de UC detrás.
# MAGIC > No montas servidores: subes el código y Databricks la sirve.
# MAGIC >
# MAGIC > 🔁 **Cada vez que cambies `app.yaml` o `requirements.txt`, RE-DESPLIEGA la App** (botón
# MAGIC > **Deploy**). Editar los archivos no basta: solo se aplican en un deploy nuevo.

# COMMAND ----------

print("Configura estas variables en app/app.yaml antes de desplegar:\n")
print(f'  Z2H_ENDPOINT     = "<el nombre de tu serving endpoint, ej. zerotohero>"')
print(f'  Z2H_CATALOGO     = "{catalogo}"')
print(f'  Z2H_SCHEMA       = "{schema}"')
print(f'  Z2H_WAREHOUSE_ID = "<ID de un SQL warehouse — míralo en SQL Warehouses>"')
print()
print("⚠️  El endpoint tiene que estar READY (menú Serving). Lo creaste en el Paso 7 del M6.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso C1.5 · ⚠️ Dale permisos a la App (¡esto es clave!)
# MAGIC
# MAGIC Cuando abras la App recién desplegada, es muy probable que veas errores de permisos: no
# MAGIC puede **consultar el endpoint** o **leer las tablas** de los KPIs. **No es un bug — es
# MAGIC gobierno de Unity Catalog funcionando.** La clave:
# MAGIC
# MAGIC > 🔑 **Una Databricks App NO corre como tú.** Corre con la identidad de su propio
# MAGIC > **service principal** (un "usuario robot" que se crea junto con la App). Tú tienes acceso,
# MAGIC > pero ese service principal **no** — hasta que se lo das. Por eso funciona en el notebook
# MAGIC > (eres tú) y falla en la App (es el robot).
# MAGIC
# MAGIC La App necesita **tres cosas**, y no todas se dan igual:
# MAGIC
# MAGIC | Permiso | Sobre qué | Cómo se da |
# MAGIC |---|---|---|
# MAGIC | **Can Query** | el **serving endpoint** | UI: Serving → tu endpoint → Permissions |
# MAGIC | **Can Use** | el **SQL warehouse** (KPIs) | UI: SQL Warehouses → tu warehouse → Permissions |
# MAGIC | **USE + SELECT** | el **schema** (tablas gold) | la celda `GRANT` de abajo |
# MAGIC
# MAGIC ### Qué hacer
# MAGIC 1. Copia el **App ID** de tu App: menú **Apps → tu app → Overview / Configuration → App ID**
# MAGIC    (un UUID `xxxxxxxx-xxxx-...`; es el mismo valor que su service principal).
# MAGIC 2. **Por la UI**, dale a ese service principal:
# MAGIC    - **Can Query** en tu endpoint (**Serving → tu endpoint → Permissions → Add**)
# MAGIC    - **Can Use** en tu warehouse (**SQL Warehouses → tu warehouse → Permissions → Add**)
# MAGIC 3. Pega el App ID en la celda de abajo (`SP_DE_LA_APP`) y **ejecútala** para el `SELECT` en
# MAGIC    las tablas.
# MAGIC 4. Recarga la App.
# MAGIC
# MAGIC > 💡 La celda ejecuta cada `GRANT` por separado con `spark.sql(...)`. Un solo `%sql` con
# MAGIC > varios `GRANT` pegados da error de sintaxis (una celda SQL corre **un** statement).

# COMMAND ----------

# 📝 Pega aquí el App ID de tu App (Apps → tu app → Overview → App ID).
#    Es el mismo UUID que su service principal.
SP_DE_LA_APP = "PEGA_AQUI_EL_APP_ID"

if SP_DE_LA_APP == "PEGA_AQUI_EL_APP_ID":
    print("⚠️  Primero pega el App ID de tu App arriba y vuelve a ejecutar.")
    print("    Lo encuentras en: Apps → tu app → Overview / Configuration → App ID (un UUID).")
else:
    # SELECT sobre las tablas gold (para los KPIs). El endpoint (Can Query) y el warehouse
    # (Can Use) se dan por la UI — ver la tabla de arriba.
    grants = [
        f"GRANT USE CATALOG ON CATALOG `{catalogo}` TO `{SP_DE_LA_APP}`",
        f"GRANT USE SCHEMA ON SCHEMA `{catalogo}`.`{schema}` TO `{SP_DE_LA_APP}`",
        f"GRANT SELECT ON SCHEMA `{catalogo}`.`{schema}` TO `{SP_DE_LA_APP}`",
    ]
    for g in grants:
        spark.sql(g)
        print(f"✅ {g}")
    print("\n🎉 Permisos de tablas aplicados (para los KPIs).")
    print("👉 Faltan 2 pasos por la UI: Can Query en el endpoint y Can Use en el warehouse.")
    print("   Luego recarga la App.")

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
# MAGIC 3. El **umbral** del semáforo de riesgo (a partir de qué probabilidad se marca en rojo)
# MAGIC
# MAGIC Guarda y **vuelve a desplegar** (redeploy).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso C3 · Úsala ⭐
# MAGIC
# MAGIC Abre la App. Ingresa una transacción: **monto, categoría, canal, país y hora**. Presiona
# MAGIC calcular.
# MAGIC
# MAGIC La App te devuelve la **probabilidad de fraude** y un **semáforo** — calculados por el
# MAGIC modelo que **tú registraste hace 20 minutos** en el Módulo 6.
# MAGIC
# MAGIC 👀 Prueba dos casos:
# MAGIC - Monto normal, en Colombia, mediodía → probabilidad baja, verde
# MAGIC - Monto enorme, en el exterior, de madrugada → probabilidad alta, rojo
# MAGIC
# MAGIC **Ese es el ciclo cerrado:** del archivo crudo de la mañana a una decisión operativa,
# MAGIC calculada por tu propio modelo.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🏁 Cierre del capstone
# MAGIC
# MAGIC Ejecuta esta celda: verifica que los tres artefactos del día existen.

# COMMAND ----------

resultados = []

# el modelo del M6 con su alias champion (lo que sirve el endpoint).
# Verificamos que el alias EXISTE sin des-serializar el modelo (evita choques de versión).
try:
    from mlflow.tracking import MlflowClient
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    mv = MlflowClient().get_model_version_by_alias(
        f"{catalogo}.{schema}.modelo_fraude", "champion")
    resultados.append((True, f"Modelo modelo_fraude@champion — v{mv.version} (lo sirve el endpoint)"))
except Exception as e:
    resultados.append((False, f"No encuentro el alias @champion: {str(e)[:50]}"))

# gold, base del dashboard, Genie y los KPIs de la App
try:
    fg = spark.table("gold_riesgo_diario").count()
    resultados.append((fg > 0, f"gold_riesgo_diario: {fg:,} filas (dashboard, Genie y KPIs de la App)"))
except Exception:
    resultados.append((False, "No encuentro gold_riesgo_diario"))

print("=" * 68)
print("  MÓDULO 7 · CIERRE DEL CAPSTONE")
print("=" * 68)
for ok, msg in resultados:
    print(f"  {'✅' if ok else '❌'}  {msg}")
print("""
  Además, confirma tú en la UI:
    ☐ Dashboard publicado con 4 visualizaciones
    ☐ Genie responde bien la pregunta 3 DESPUÉS de la instruction
    ☐ Serving endpoint READY (Módulo 6, Paso 7)
    ☐ App desplegada que consulta el endpoint y devuelve el riesgo de fraude
""")
print("=" * 68)
if all(ok for ok, _ in resultados):
    print("""
  🎉🎉 ¡CAPSTONE COMPLETO! 🎉🎉

  En un día, de punta a punta:
    M1 · tu primera tabla Delta y time travel
    M2 · ingesta con Auto Loader → bronze
    M3 · pipeline declarativo → silver y gold
    M4 · gobierno: row filter y column masking
    M5 · orquestación: un job que corre solo
    M6 · un modelo de fraude, registrado con alias
    M7 · dashboard + Genie + App que usa tu modelo

  Empezaste con archivos crudos. Terminas con una App que califica
  el fraude en vivo. Todo esto lo construiste HOY.
""")
else:
    print("  ⚠️  Revisa los ❌. Para la App necesitas el modelo del M6.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Lo que te llevas
# MAGIC
# MAGIC | Concepto | Dónde lo viviste |
# MAGIC |---|---|
# MAGIC | **Dashboard** para métricas recurrentes | 4 visualizaciones sobre gold |
# MAGIC | **Genie se cultiva**, no es magia | Lo viste fallar y lo corregiste |
# MAGIC | **App** para una decisión concreta | Consulta tu **serving endpoint** por REST |
# MAGIC | **Cada herramienta para su audiencia** | Negocio · analista · operación |
# MAGIC | **El ciclo se cierra** | Del archivo crudo a la decisión, en un día |
# MAGIC
# MAGIC ## Y con esto terminamos
# MAGIC
# MAGIC Gracias por el día. Guarda los enlaces de tus tres artefactos en el formulario de
# MAGIC entrega — y lleva este notebook: es tuyo, con todo lo que construiste.
# MAGIC
# MAGIC **De Zero a Hero, de verdad.** 🚀
