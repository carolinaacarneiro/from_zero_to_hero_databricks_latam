# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # ⏰ Módulo 5 · Orquestación
# MAGIC
# MAGIC Vas a encadenar lo que construiste en un **job** que corre solo.
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - La mayor parte se hace **en la UI de Jobs**, no en este notebook. Acá está la guía.
# MAGIC - Donde dice **📝 TU TURNO**, configuras algo en la UI.
# MAGIC - 🙋 Trabado más de 3 minutos: levanta la mano.
# MAGIC
# MAGIC ## De dónde venimos
# MAGIC Tienes ingesta (M2), pipeline (M3) y gobierno (M4). Todo lo corriste **a mano**. Hoy lo
# MAGIC conviertes en un proceso programado, con reintentos y alertas.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** El método —orquestar tareas con dependencias,
# MAGIC > reintentos y observabilidad— es el mismo en cualquier industria.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 · Crea un job de 3 tareas
# MAGIC
# MAGIC En el menú de la izquierda: **Jobs & Pipelines → Create → Job**.
# MAGIC
# MAGIC Nómbralo `z2h_fraude_<tu_usuario>` y agrega **tres tareas**:
# MAGIC
# MAGIC | # | Task key | Tipo | Apunta a |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `ingesta_bronze` | Notebook | tu `02_ingesta` del Módulo 2 |
# MAGIC | 2 | `pipeline_silver_gold` | Pipeline | tu pipeline del Módulo 3 |
# MAGIC | 3 | `validacion` | Notebook | el `validacion` de esta carpeta |
# MAGIC
# MAGIC > 💡 Para la tarea 2, tipo **Pipeline**: eliges el pipeline que ya creaste, no un
# MAGIC > notebook. Un job puede orquestar pipelines, notebooks, consultas SQL y más.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · Declara las dependencias
# MAGIC
# MAGIC Cada tarea tiene un campo **"Depends on"**:
# MAGIC
# MAGIC ```
# MAGIC   ingesta_bronze  →  pipeline_silver_gold  →  validacion
# MAGIC ```
# MAGIC
# MAGIC - `pipeline_silver_gold` **depends on** `ingesta_bronze`
# MAGIC - `validacion` **depends on** `pipeline_silver_gold`
# MAGIC
# MAGIC Cuando las conectes, el job dibuja el **DAG** arriba. Igual que el pipeline del M3 —pero
# MAGIC un nivel más arriba: acá cada nodo es un proceso entero, no un dataset.
# MAGIC
# MAGIC 👀 La tarea 3 no arranca hasta que la 2 termine bien. Si la 2 falla, la 3 **no corre** —
# MAGIC no validas datos que no se generaron.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · 📝 TU TURNO — agrega parámetros
# MAGIC
# MAGIC Un job de producción casi siempre recibe **parámetros** — la fecha a procesar, el
# MAGIC ambiente, etc. Vamos a agregar tres.
# MAGIC
# MAGIC 1. En la configuración del job, sección **Parameters** (o *Job parameters*), agrega:
# MAGIC    - **Key**: `catalogo` | **Value**: `<tu_catalogo>` *(el que usaste en el Módulo 1)*
# MAGIC    - **Key**: `schema` | **Value**: `<tu_schema>` *(el que usaste en el Módulo 1)*
# MAGIC    - **Key**: `fecha_proceso` | **Value**: `2026-03-01`
# MAGIC
# MAGIC 2. Las tareas del job (particularmente `validacion`) los van a leer con `dbutils.widgets.get()`.
# MAGIC
# MAGIC > 💡 Así el mismo job sirve para reprocesar cualquier día y cualquier espacio (catálogo/schema):
# MAGIC > cambias los parámetros al lanzarlo, sin tocar el código. Los notebooks ya están preparados
# MAGIC > para leerlos.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · Fiabilidad — reintentos y alertas
# MAGIC
# MAGIC Un proceso de producción tiene que **sobrevivir a un mal día** y **avisar** cuando algo
# MAGIC sale mal.
# MAGIC
# MAGIC En la configuración del job:
# MAGIC
# MAGIC | Configura | Dónde | Valor |
# MAGIC |---|---|---|
# MAGIC | **Reintentos** | en la tarea o el job | 1 reintento |
# MAGIC | **Notificación** | sección Notifications | tu correo, "on failure" |
# MAGIC
# MAGIC > 💡 Un reintento resuelve la mayoría de los fallos **transitorios** (un pico de carga,
# MAGIC > un timeout de red). La notificación te avisa de los que no se resuelven solos, sin que
# MAGIC > tengas que estar mirando.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 5 · Programa y lanza
# MAGIC
# MAGIC 1. En **Schedules & Triggers**, presiona **Add trigger** y sigue estos pasos:
# MAGIC    1. **Trigger type** → elige **`Scheduled`** *(por defecto viene en `None (manual)`)*
# MAGIC    2. Aparece **Schedule type** → elige **`Schedule`** *(no `Interval`, que es «cada N
# MAGIC       minutos/horas» sin cron)*
# MAGIC    3. En **Schedule**, marca la casilla **`Show cron syntax`** ✅ — el campo se vuelve un
# MAGIC       cron editable. Ahí escribes: **`0 0 7 * * ?`** (todos los días a las 07:00)
# MAGIC    4. A la derecha, confirma la **zona horaria** (ej. `America/Bogota`)
# MAGIC    5. **Save**
# MAGIC 2. **No esperes al horario**: presiona **"Run now"** (arriba a la derecha) para lanzarlo ya.
# MAGIC
# MAGIC > 💡 **¿Dónde está el cron?** No aparece hasta que eliges `Scheduled` → `Schedule` → marcas
# MAGIC > **Show cron syntax**. Sin esa casilla, la UI te muestra selectores de día/hora en vez del
# MAGIC > texto cron. Las dos formas hacen lo mismo; el cron es solo la versión en texto.
# MAGIC >
# MAGIC > 🕐 El cron `0 0 7 * * ?` se lee: segundo 0, minuto 0, hora 7, cualquier día del mes,
# MAGIC > cualquier mes, cualquier día de la semana → **7:00 AM, todos los días**.
# MAGIC >
# MAGIC > 💡 El schedule deja el job corriendo solo cada día. El "Run now" es para probarlo sin
# MAGIC > esperar. Los dos conviven: puedes tener schedule y lanzar manualmente cuando quieras.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · Recorre la ejecución en vivo ⭐
# MAGIC
# MAGIC Mientras el job corre (1–4 minutos), esto es lo que vale la pena ver:
# MAGIC
# MAGIC | Dónde | Qué ves |
# MAGIC |---|---|
# MAGIC | **La matriz de ejecuciones** | Cada run como una fila; el de ahora, en curso |
# MAGIC | **El DAG del run** | Las 3 tareas, cuál corrió, cuál está en curso, cuál espera |
# MAGIC | **Clic en una tarea** | Sus **logs** en vivo y su **duración** |
# MAGIC | **Al terminar** | Todo en verde, con el tiempo total |
# MAGIC
# MAGIC 👀 Entra a la tarea `pipeline_silver_gold` y mira su duración. Entra a `validacion` y lee
# MAGIC su salida. **Esto es observabilidad de un run real**, no una captura de pantalla.
# MAGIC
# MAGIC ### 🏆 Reto opcional
# MAGIC Agrega una **tarea condicional** (tipo *If/else*) que solo corra si `validacion` detecta
# MAGIC descartes por encima de un umbral — por ejemplo, para mandar una alerta especial. Es
# MAGIC cómo se construye un job que reacciona a sus propios resultados.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · Verifica tu checkpoint
# MAGIC
# MAGIC Responde tú mismo, mirando la UI del job:

# COMMAND ----------

# MAGIC %md
# MAGIC | Checkpoint | ¿Listo? |
# MAGIC |---|---|
# MAGIC | El job tiene **3 tareas encadenadas** | ☐ |
# MAGIC | Tiene un **schedule** configurado | ☐ |
# MAGIC | Tiene **1 reintento** y **notificación** | ☐ |
# MAGIC | Completó **una ejecución en verde** | ☐ |
# MAGIC | Tiene el parámetro `fecha_proceso` | ☐ |
# MAGIC
# MAGIC Si las cinco están marcadas: **🎉 Módulo 5 completo.**

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Lo que te llevas
# MAGIC
# MAGIC | Concepto | Dónde lo viviste |
# MAGIC |---|---|
# MAGIC | **Un job encadena tareas con dependencias** | ingesta → pipeline → validación |
# MAGIC | **Triggers**: cron, archivo, continuo | Programaste 07:00 diario |
# MAGIC | **Fiabilidad**: reintentos y alertas | 1 reintento + email |
# MAGIC | **Observabilidad**: matriz, logs, duración | Recorriste un run en vivo |
# MAGIC | **Parámetros** | `fecha_proceso`, para reprocesar cualquier día |
# MAGIC | **CI/CD con DABs** | El job como YAML versionado *(lo viste en slides)* |
# MAGIC
# MAGIC ## ⚠️ Recordatorio
# MAGIC En producción, este job **no se arma clic por clic**: se declara en un Asset Bundle
# MAGIC (YAML), se versiona en Git y se promueve dev → staging → prod. Lo que hiciste hoy en la
# MAGIC UI es el mismo job, en su forma de aprendizaje.
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC Tienes un proceso que corre solo y produce datos confiables y gobernados. En el
# MAGIC **Módulo 6** entrenas un **modelo de ML** sobre esos datos, para predecir el fraude en
# MAGIC vez de solo contarlo.
