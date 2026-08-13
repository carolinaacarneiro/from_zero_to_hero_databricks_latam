# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🔒 Módulo 4 · Gobierno y seguridad
# MAGIC
# MAGIC Vas a documentar, clasificar, proteger y rastrear tus datos — y **ver el
# MAGIC efecto en tus propios datos**.
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC - Ejecuta celda por celda. Donde dice **📝 TU TURNO**, completas SQL.
# MAGIC - Si un filtro te deja sin ver nada, hay una **celda de rescate** marcada para quitarlo.
# MAGIC - 🙋 Trabado más de 3 minutos: levanta la mano.
# MAGIC
# MAGIC > 🖱️ **Dos caminos, tú eliges.** Casi todo lo de este módulo también se puede hacer con
# MAGIC > clics en **Catalog Explorer** (menú **Catalog** → tu tabla), sin escribir SQL. En cada
# MAGIC > paso te decimos dónde está la opción de UI. Puedes hacer todo por notebook, todo por la
# MAGIC > UI, o mezclar — el resultado es el mismo. Saber que existen las dos formas es parte del
# MAGIC > aprendizaje.
# MAGIC
# MAGIC ## Lo que vas a hacer
# MAGIC
# MAGIC | Paso | Tema |
# MAGIC |---|---|
# MAGIC | 1 | **Descripciones**: comentar tablas y columnas |
# MAGIC | 2 | **Tags e identificación de PII**: clasificar los datos sensibles |
# MAGIC | 3 | **Row filter**: quién ve qué fila ⭐ |
# MAGIC | 4 | **Column masking**: quién ve qué valor |
# MAGIC | 5 | **Permissions**: cómo se otorga el acceso *(informativo)* |
# MAGIC | 6 | **Lineage, insights y quality** en Catalog Explorer |
# MAGIC
# MAGIC ## El truco de este módulo
# MAGIC No puedes iniciar sesión como otra persona para probar que un control funciona. Así que
# MAGIC **simulas tu identidad** con dos campos (región y lector de PII): aplicas el control, y
# MAGIC luego **cambias los campos y vuelves a aplicar** para ver el efecto cambiar en vivo.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos. Requiere un workspace con Unity
# MAGIC > Catalog — no funciona en Free Edition.
# MAGIC >
# MAGIC > 💡 Gobernar datos es de cualquier industria: un hospital documenta y enmascara
# MAGIC > diagnósticos, un retailer clasifica y filtra ventas por región. Cambia el dato.
# MAGIC
# MAGIC ## 🎭 Cómo simulamos "quién eres" hoy
# MAGIC En una empresa real, quién ve qué se decide por el **grupo** al que perteneces
# MAGIC (`is_account_group_member(...)`). Pero hoy cada quien está en **su propio workspace**, sin
# MAGIC grupos configurados. Así que vamos a simular tu identidad con **dos campos** arriba del
# MAGIC notebook: tu **región** y si eres **lector de datos sensibles**.
# MAGIC
# MAGIC Lo potente: puedes **cambiar esos campos y volver a aplicar** — y ves el control cambiar
# MAGIC en vivo, tú solo. Es la misma mecánica que con grupos; solo cambia de dónde sale "quién
# MAGIC eres".

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
usuario = spark.sql("SELECT current_user()").collect()[0][0]

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
        f"   Corre los módulos anteriores (M1-M3) con estos mismos valores primero, o corrige el nombre "
        f"arriba.\n"
    )

spark.sql(f"USE `{catalogo}`.`{schema}`")

print(f"👤 Usuario       : {usuario}")
print(f"📁 Trabajarás en : {catalogo}.{schema}")
print("✅ Espacio declarado y verificado. Nadie más toca este schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 0 · Tu identidad simulada
# MAGIC Ejecuta esta celda. Arriba aparecen dos campos: elige tu **región** y si eres
# MAGIC **lector de PII**. Podrás cambiarlos más adelante para ver el efecto.

# COMMAND ----------

# dos campos que simulan "quién eres" — sin depender de grupos del workspace
dbutils.widgets.dropdown("mi_region", "centro", ["norte", "centro", "sur"], "1 · Tu región")
dbutils.widgets.dropdown("soy_lector_pii", "no", ["no", "sí"], "2 · ¿Lees datos sensibles (PII)?")

mi_region = dbutils.widgets.get("mi_region")
soy_lector_pii = dbutils.widgets.get("soy_lector_pii") == "sí"

print(f"🌎 Tu región      : {mi_region}")
print(f"🔓 Lector de PII  : {'sí' if soy_lector_pii else 'no'}")
print()
print("Estos dos campos simulan tu identidad. Cámbialos arriba y re-ejecuta las celdas")
print("de los pasos 3 y 4 para ver cómo cambia lo que ves.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sobre qué tabla trabajamos: `silver_transacciones`
# MAGIC
# MAGIC Gobernamos la capa **silver** — tus datos limpios y confiables del Módulo 3. Tiene las
# MAGIC dos columnas que este módulo necesita: **`region`** (para el row filter) y
# MAGIC **`numero_tarjeta`** (para la máscara).

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 · Descripciones: documenta tus datos
# MAGIC
# MAGIC ## Por qué importa
# MAGIC Un dato que nadie entiende, nadie lo usa — o peor, lo usa mal. **Documentar es gobierno:**
# MAGIC un comentario en una tabla o columna es la diferencia entre un dato confiable y uno que
# MAGIC genera dudas.
# MAGIC
# MAGIC Y hay un beneficio extra: **Genie y el Assistant leen estos comentarios** (lo verás en el
# MAGIC Módulo 7). Documentar mejora las respuestas de la IA sobre tus datos.
# MAGIC
# MAGIC > 🖱️ **También por la UI:** en **Catalog Explorer** → `silver_transacciones`, cada tabla y
# MAGIC > columna tiene un campo de comentario que editas con un clic (ícono de lápiz). Usa el SQL de
# MAGIC > abajo, o hazlo por la UI — lo que prefieras.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comentario a nivel de TABLA
# MAGIC COMMENT ON TABLE silver_transacciones IS
# MAGIC   'Transacciones limpias y enriquecidas con datos del cliente. Contiene PII (numero_tarjeta).';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 TU TURNO — documenta dos columnas
# MAGIC
# MAGIC Agrega un comentario a `monto` y a `es_fraude` de `silver_transacciones`.
# MAGIC
# MAGIC <details>
# MAGIC <summary>💡 Solución</summary>
# MAGIC
# MAGIC <pre>COMMENT ON COLUMN silver_transacciones.monto IS
# MAGIC   'Monto de la transacción, en pesos colombianos (COP)';
# MAGIC COMMENT ON COLUMN silver_transacciones.es_fraude IS
# MAGIC   'Etiqueta: si la transacción fue fraudulenta. La usa el modelo del Módulo 6';</pre>
# MAGIC </details>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: comenta silver_transacciones.monto y .es_fraude
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comprueba que quedaron

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE silver_transacciones

# COMMAND ----------

# MAGIC %md
# MAGIC 👀 En la columna `comment` del resultado ves tus descripciones. También aparecen en
# MAGIC **Catalog Explorer**, que exploras al final del módulo.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · Tags e identificación de PII
# MAGIC
# MAGIC ## Por qué importa
# MAGIC Con 4 tablas puedes proteger columna por columna. **Con 10.000, no.** Los **tags** te
# MAGIC dejan clasificar los datos —"esto es PII", "esto es de finanzas"— y luego gobernar
# MAGIC **por clasificación**, no tabla por tabla. Es la base de ABAC (control de acceso por
# MAGIC atributo).
# MAGIC
# MAGIC El primer paso de proteger datos sensibles es **saber dónde están**.
# MAGIC
# MAGIC > 🖱️ **También por la UI:** en **Catalog Explorer** → `silver_transacciones` → pestaña
# MAGIC > **Details** (o el ícono de tags), agregas tags con clics. Ejecuta el SQL de abajo o hazlo
# MAGIC > por la UI.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag a nivel de TABLA: clasifica silver como que contiene datos personales
# MAGIC ALTER TABLE silver_transacciones
# MAGIC SET TAGS ('dominio' = 'finanzas', 'contiene_pii' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 TU TURNO — etiqueta la columna sensible
# MAGIC
# MAGIC Marca `numero_tarjeta` como PII con un tag. Así, más adelante, puedes encontrar **todas**
# MAGIC las columnas PII del catálogo con una sola consulta — sin revisar tabla por tabla.
# MAGIC
# MAGIC <details>
# MAGIC <summary>💡 Solución</summary>
# MAGIC
# MAGIC <pre>ALTER TABLE silver_transacciones
# MAGIC ALTER COLUMN numero_tarjeta
# MAGIC SET TAGS ('clasificacion' = 'pii', 'tipo_pii' = 'tarjeta');</pre>
# MAGIC </details>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: etiqueta silver_transacciones.numero_tarjeta como PII
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encuentra el PII de todo tu schema con una consulta
# MAGIC
# MAGIC Esto es lo que los tags hacen posible: preguntar *«¿dónde están mis datos sensibles?»* y
# MAGIC obtener la respuesta al instante, aunque tuvieras miles de tablas.

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM {catalogo}.information_schema.column_tags
    WHERE schema_name = '{schema}'
      AND tag_value = 'pii'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC 🎉 Encontraste tu columna PII **sin abrir cada tabla**. Con este mismo patrón, un banco
# MAGIC localiza cada número de tarjeta, cada documento, cada dato personal de todo su catálogo.
# MAGIC
# MAGIC > 💡 Esto habilita **ABAC**: en vez de enmascarar `numero_tarjeta` tabla por tabla, defines
# MAGIC > una política que dice *«toda columna con tag `pii` se enmascara»*. Una regla, no mil.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 ¿Y si tuvieras miles de tablas? Data Classification lo hace solo
# MAGIC
# MAGIC Etiquetar el PII a mano funciona con una columna. Con un catálogo entero, no. Databricks
# MAGIC tiene **Data Classification**: un agente de IA que *«clasifica y etiqueta automáticamente
# MAGIC las tablas de tu Unity Catalog»*.
# MAGIC
# MAGIC | Lo que hiciste a mano (paso 2) | Lo que hace Data Classification |
# MAGIC |---|---|
# MAGIC | Etiquetaste **1 columna** con `clasificacion=pii` | **Escanea todo el catálogo** y etiqueta solo |
# MAGIC | Tú decides qué es PII | Un LLM **detecta** el tipo: nombre, email, tarjeta, CPF… |
# MAGIC | Un tag genérico | Tags precisos: `class.email_address`, `class.credit_card`, `class.us_ssn`… |
# MAGIC
# MAGIC **Qué detecta:** datos de PII, PCI DSS, GDPR, HIPAA y más — incluidos identificadores
# MAGIC locales (en Colombia y LATAM: documentos, tarjetas, cuentas). El resultado queda en la
# MAGIC tabla del sistema `system.data_classification.results`.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Es automático** | Escaneo incremental, sin configurar tabla por tabla |
# MAGIC | **Corre en serverless** | Se factura como DBUs de serverless |
# MAGIC | **Requiere** | Ser dueño del catálogo o tener el privilegio `MANAGE` |
# MAGIC
# MAGIC > 👀 **Esto es para conocerlo, no para ejecutarlo hoy:** activar la clasificación requiere
# MAGIC > permisos de administración del catálogo que probablemente no tengas en el taller, y el
# MAGIC > escaneo corre de forma programada. Lo que aprendiste a mano —tags para localizar PII— es
# MAGIC > exactamente lo que esto automatiza a escala. Se activa desde **Catalog Explorer → tu
# MAGIC > catálogo → pestaña de clasificación**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 RBAC vs. ABAC — dos formas de controlar el acceso
# MAGIC
# MAGIC Acabas de ver la idea detrás de **ABAC**. Vale entender los dos modelos, porque es una
# MAGIC pregunta que aparece siempre en gobierno de datos.
# MAGIC
# MAGIC | | **RBAC** (por rol) | **ABAC** (por atributo) |
# MAGIC |---|---|---|
# MAGIC | Cómo decide el acceso | Por el **rol/grupo** al que perteneces | Por **atributos** (tags) del dato y del usuario |
# MAGIC | La regla se ve como | *«el grupo `analistas_riesgo` puede leer la tabla X»* | *«toda columna con tag `pii` se enmascara»* |
# MAGIC | Dónde se define | Objeto por objeto (`GRANT` en tabla/schema) | Una **política** que aplica a todo lo que tenga el tag |
# MAGIC | Escala | Bien con pocas tablas | Bien con **miles** de tablas |
# MAGIC | Ejemplo | «los analistas ven `silver_transacciones`» | «nadie ve columnas `pii`, salvo investigadores autorizados» |
# MAGIC
# MAGIC ## Cuándo usar cada uno
# MAGIC - **RBAC** para el acceso base: *quién entra a qué tabla o schema* (lo del Paso 5). Es simple
# MAGIC   y suele ser suficiente para dar/quitar acceso a un conjunto de datos.
# MAGIC - **ABAC** para reglas **transversales** que deben valer en todo el catálogo sin repetirlas:
# MAGIC   *«todo lo que sea PII se protege igual, esté donde esté»*.
# MAGIC - **No compiten, se combinan:** RBAC define el acceso a las tablas; ABAC aplica, encima, las
# MAGIC   reglas por clasificación. Los tags del Paso 2 son lo que hace posible el ABAC.
# MAGIC
# MAGIC ## 🔷 Por qué es un diferencial en Databricks
# MAGIC En muchas plataformas el control es **solo RBAC**, y proteger un dato sensible que aparece en
# MAGIC 500 tablas significa **500 reglas** que alguien tiene que recordar mantener. En **Unity
# MAGIC Catalog**, como los tags, el linaje y los permisos viven en **un solo lugar** para todo
# MAGIC —tablas, columnas, archivos, modelos, dashboards—, puedes gobernar **por atributo**: una
# MAGIC política, aplicada a todo lo etiquetado, que se mantiene sola aunque mañana aparezcan tablas
# MAGIC nuevas con ese tag. **Una regla en vez de mil**, y sin herramientas aparte.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 · Row filter ⭐ — quién ve qué fila
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Hacer que `silver_transacciones` te muestre **solo las filas de tu región** (la que elegiste
# MAGIC arriba), sin duplicar la tabla ni crear una vista.
# MAGIC
# MAGIC > 🖱️ **También por la UI:** una vez creada la función de filtro (celda de abajo), puedes
# MAGIC > aplicarla desde **Catalog Explorer** → `silver_transacciones` → **Permissions / Row
# MAGIC > filter**. Acá lo hacemos por SQL para verlo completo, pero la opción de UI existe.
# MAGIC
# MAGIC ### Primero, cuenta cuántas filas ves — y anótalo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS filas_totales FROM silver_transacciones

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crea la función de filtro
# MAGIC
# MAGIC Una función que decide, fila por fila, si la ves: **¿la región de la fila es la tuya?**
# MAGIC
# MAGIC En una empresa real, en vez de comparar con una región fija, se usa
# MAGIC `is_account_group_member('z2h_region_' || region_fila)` — así la respuesta depende del
# MAGIC grupo de quien consulta. Hoy, como no hay grupos, comparamos con **tu región del widget**.
# MAGIC La mecánica del row filter es idéntica.
# MAGIC
# MAGIC La celda de abajo crea la función usando el valor que elegiste arriba (`mi_region`).

# COMMAND ----------

# la función compara cada fila con TU región (la del widget)
spark.sql(f"""
    CREATE OR REPLACE FUNCTION filtro_region(region_fila STRING)
    RETURN region_fila = '{mi_region}'
""")
print(f"✅ Función filtro_region creada. Tu región es: {mi_region}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplica el filtro a la tabla
# MAGIC ALTER TABLE silver_transacciones SET ROW FILTER filtro_region ON (region)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cuenta otra vez: ahora ves menos
# MAGIC SELECT COUNT(*) AS filas_que_veo_ahora FROM silver_transacciones

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿de qué región son las filas que ves? solo la tuya
# MAGIC SELECT region, COUNT(*) AS filas FROM silver_transacciones GROUP BY region

# COMMAND ----------

# MAGIC %md
# MAGIC 🎉 **El número bajó, y solo ves tu región** — sobre la **misma tabla**, sin copia.
# MAGIC
# MAGIC ### 🔁 Compruébalo tú mismo: cambia de identidad
# MAGIC 1. Arriba, cambia el campo **Tu región** (por ejemplo, de `centro` a `sur`)
# MAGIC 2. Re-ejecuta la celda que crea `filtro_region` (dos celdas más arriba)
# MAGIC 3. Vuelve a contar → **ves otras filas, las de la región nueva**
# MAGIC
# MAGIC El mismo `SELECT COUNT(*)`, distinto resultado — porque cambió **quién pregunta**. Eso es
# MAGIC exactamente lo que pasaría con dos personas de regiones distintas mirando la misma tabla.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🆘 Celda de rescate — QUITAR EL ROW FILTER
# MAGIC ¿Te quedaste sin ver ninguna fila, o quieres volver al estado inicial? Pon `QUITAR = True`
# MAGIC en la celda de abajo y ejecútala.

# COMMAND ----------

QUITAR = False   # ← cámbialo a True si quieres quitar el row filter

if QUITAR:
    spark.sql("ALTER TABLE silver_transacciones DROP ROW FILTER")
    print("✅ Row filter quitado. Vuelves a ver todas las filas.")
else:
    print("Row filter intacto. (Pon QUITAR = True arriba si quieres quitarlo.)")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · Column masking — quién ve qué valor
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Enmascarar el `numero_tarjeta` (la columna que etiquetaste como PII en el paso 2) para que
# MAGIC solo un **lector de PII autorizado** vea el número completo.
# MAGIC
# MAGIC > 🖱️ **También por la UI:** creada la función de máscara (celda de abajo), se aplica desde
# MAGIC > **Catalog Explorer** → `silver_transacciones` → la columna `numero_tarjeta` → **Mask**. Acá
# MAGIC > lo hacemos por SQL; la opción de UI existe.
# MAGIC
# MAGIC ### Míralo sin máscara — se ve completo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT numero_tarjeta, monto, categoria_comercio FROM silver_transacciones LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crea la máscara
# MAGIC
# MAGIC Una función que devuelve el número completo **solo si eres lector de PII**; si no, los
# MAGIC últimos 4 dígitos.
# MAGIC
# MAGIC En una empresa real, "eres lector de PII" se comprueba con
# MAGIC `is_account_group_member('z2h_pii_readers')`. Hoy usamos tu campo **¿Lees datos
# MAGIC sensibles?** del widget. La mecánica del column mask es idéntica.
# MAGIC
# MAGIC La celda de abajo crea la máscara usando ese campo (`soy_lector_pii`).

# COMMAND ----------

# la máscara revela el número solo si marcaste que eres lector de PII (widget)
_puede_ver = "true" if soy_lector_pii else "false"
spark.sql(f"""
    CREATE OR REPLACE FUNCTION enmascarar_tarjeta(tarjeta STRING)
    RETURN CASE
        WHEN {_puede_ver} THEN tarjeta
        ELSE CONCAT('****-****-****-', SUBSTRING(tarjeta, -4))
    END
""")
print(f"✅ Máscara creada. ¿Eres lector de PII? {'sí' if soy_lector_pii else 'no'}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplica la máscara a la columna
# MAGIC ALTER TABLE silver_transacciones
# MAGIC ALTER COLUMN numero_tarjeta SET MASK enmascarar_tarjeta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta otra vez
# MAGIC SELECT numero_tarjeta, monto, categoria_comercio FROM silver_transacciones LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC Si dejaste **¿Lees datos sensibles? = no** (lo normal), ves `****-****-****-1234`. La
# MAGIC columna no se movió ni se copió: devuelve un valor distinto según quién lee.
# MAGIC
# MAGIC ### 🔁 Compruébalo: cambia de identidad
# MAGIC 1. Arriba, cambia **¿Lees datos sensibles?** a `sí`
# MAGIC 2. Re-ejecuta la celda que crea `enmascarar_tarjeta` (dos celdas más arriba) y el `ALTER`
# MAGIC 3. Consulta de nuevo → **ahora ves el número completo**
# MAGIC
# MAGIC Misma columna, misma consulta, distinto resultado — según quién pregunta. Así, en un
# MAGIC banco, un analista ve `****` y un investigador de fraude autorizado ve el número.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 5 · Permissions — cómo se otorga el acceso *(solo lectura)*
# MAGIC
# MAGIC Este paso es **para conocerlo, no para ejecutarlo** — no hay nada que completar.
# MAGIC
# MAGIC ## La idea
# MAGIC Con Unity Catalog gestionas quién accede a cada objeto, **a grupos o a usuarios**, con
# MAGIC `GRANT` y `REVOKE`. La regla de oro: **otorga a grupos, no a personas** — el acceso
# MAGIC describe un rol, no un individuo, y así sobrevive a una rotación.
# MAGIC
# MAGIC ```sql
# MAGIC -- Dar lectura a un grupo (lo recomendado)
# MAGIC GRANT SELECT ON TABLE silver_transacciones TO `analistas_riesgo`;
# MAGIC
# MAGIC -- Quitar el acceso
# MAGIC REVOKE SELECT ON TABLE silver_transacciones FROM `analistas_riesgo`;
# MAGIC
# MAGIC -- Ver quién tiene acceso
# MAGIC SHOW GRANTS ON TABLE silver_transacciones;
# MAGIC ```
# MAGIC
# MAGIC | Concepto | |
# MAGIC |---|---|
# MAGIC | **A grupos, no a personas** | El acceso describe un rol; sobrevive a cambios de equipo |
# MAGIC | **Herencia** | Para leer una tabla: `USE CATALOG` + `USE SCHEMA` + `SELECT`. Otorga en el nivel correcto y no repites tabla por tabla |
# MAGIC | **También desde la UI** | En Catalog Explorer → pestaña **Permissions** se otorga con clics |
# MAGIC
# MAGIC > 💡 No lo ejecutamos hoy porque en tu propio workspace ya eres dueño de todo (verías poco
# MAGIC > efecto), y gestionar grupos requiere permisos de administración. Pero **así es como
# MAGIC > controlarías el acceso** en tu organización: con estas mismas sentencias o desde la UI.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · Catalog Explorer: lineage, insights y quality
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Ver, sin escribir código, todo lo que Unity Catalog sabe de tus tablas. Abre **Catalog**
# MAGIC (menú izquierdo) → tu catálogo → `{schema}` → `silver_transacciones`.
# MAGIC
# MAGIC ### Recorre estas pestañas
# MAGIC
# MAGIC | Pestaña | Qué vas a ver | Lo que ya hiciste hoy |
# MAGIC |---|---|---|
# MAGIC | **Overview** | Columnas, tipos y **tus comentarios** (paso 1) | Descripciones |
# MAGIC | **Details / Tags** | Los **tags** que pusiste (paso 2) | Clasificación PII |
# MAGIC | **Permissions** | Quién tiene acceso | Donde se gestiona con clics |
# MAGIC | **Lineage** | De qué tablas viene y quién la consume | ⬇️ míralo con cuidado |
# MAGIC | **Insights** | Consultas frecuentes, usuarios, popularidad | Uso real de la tabla |
# MAGIC | **Quality** | Métricas de calidad de los datos | Salud de la tabla |
# MAGIC | **History** | Versiones Delta (como en el M1) | Auditoría de cambios |
# MAGIC
# MAGIC ### 🔎 Detente en Lineage
# MAGIC
# MAGIC Vas a ver el grafo:
# MAGIC ```
# MAGIC   archivos raw → bronze → silver → gold_riesgo_diario → (pronto: dashboard, Genie, app)
# MAGIC ```
# MAGIC **Nadie lo dibujó.** Unity Catalog lo registró solo, a medida que corrían los pipelines.
# MAGIC
# MAGIC | Para qué sirve | Ejemplo |
# MAGIC |---|---|
# MAGIC | **Análisis de impacto** | *«Si cambio silver, ¿qué se rompe?»* → lo ves antes de tocar |
# MAGIC | **Auditoría** | *«¿De qué archivo salió este número?»* → hasta el origen |
# MAGIC | **Confianza** | Un dato con linaje visible es un dato en el que puedes confiar |
# MAGIC
# MAGIC > 💡 **Insights** y **Quality** dependen de que la tabla haya tenido uso y de la edición
# MAGIC > del workspace: si están vacías es normal en un entorno recién creado. Lo importante es
# MAGIC > saber que existen y qué responden.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🩺 Data Quality Monitoring: la calidad que se vigila sola
# MAGIC
# MAGIC En el Módulo 3 pusiste **expectativas** en el pipeline: reglas que tú escribes. Unity
# MAGIC Catalog tiene además un monitoreo que **funciona sin que escribas reglas**: analiza el
# MAGIC historial de la tabla y aprende su comportamiento normal.
# MAGIC
# MAGIC ### Anomaly Detection — frescura y completitud, automáticas
# MAGIC
# MAGIC | Qué vigila | Cómo |
# MAGIC |---|---|
# MAGIC | **Frescura** | *«¿hace cuánto no se actualiza?»* — aprende cada cuánto llegan datos y avisa si un commit tarda de más |
# MAGIC | **Completitud** | *«¿llegaron las filas esperadas en las últimas 24h?»* — predice el rango normal y marca si falta |
# MAGIC | **Percent null** *(beta)* | El % de nulos por columna, contra su histórico |
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Es automático** | Escanea cada tabla a la frecuencia con que se actualiza, sin configurar regla por tabla |
# MAGIC | **No toca tus datos** | *«no modifica las tablas que monitorea ni agrega carga a los jobs»* |
# MAGIC | **Corre en serverless** | Se factura como DBUs de `DATA_QUALITY_MONITORING` |
# MAGIC | **Requiere** | Privilegio `MANAGE` sobre el schema o el catálogo para activarlo |
# MAGIC
# MAGIC ### Expectativas (M3) vs Anomaly Detection (M4) — se complementan
# MAGIC
# MAGIC | | Expectativas del pipeline | Anomaly Detection |
# MAGIC |---|---|---|
# MAGIC | Quién define la regla | **Tú** (`monto > 0`) | La plataforma, del histórico |
# MAGIC | Cuándo actúa | En cada fila, al procesar | Vigila la tabla en el tiempo |
# MAGIC | Atrapa | Filas que violan **tu** regla | Que la tabla dejó de llegar, o llegó a medias |
# MAGIC
# MAGIC > 👀 **Para conocerlo, no para activarlo hoy:** requiere el privilegio `MANAGE` sobre el
# MAGIC > schema, que probablemente no tengas en el taller. Se activa desde **Catalog Explorer →
# MAGIC > tu schema → Quality**. Antes se llamaba *Lakehouse Monitoring*.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · Verifica tu checkpoint

# COMMAND ----------

resultados = []

# 1 · comentarios
try:
    d = spark.sql("DESCRIBE TABLE silver_transacciones").collect()
    con_comentario = any(r["col_name"] == "monto" and r["comment"] for r in d
                         if r["col_name"] and r["comment"])
    resultados.append((con_comentario, "Descripciones: monto tiene comentario"))
except Exception as e:
    resultados.append((False, f"No pude leer comentarios: {str(e)[:50]}"))

# 2 · tag PII
try:
    n = spark.sql(f"""SELECT COUNT(*) c FROM {catalogo}.information_schema.column_tags
                      WHERE schema_name = '{schema}' AND tag_value = 'pii'""").collect()[0][0]
    resultados.append((n > 0, f"Tags: {n} columna(s) clasificada(s) como PII"))
except Exception as e:
    resultados.append((False, f"No pude leer tags: {str(e)[:50]}"))

# 3 · row filter
try:
    regs = [r[0] for r in spark.sql("SELECT DISTINCT region FROM silver_transacciones").collect()]
    resultados.append((len(regs) <= 1, f"Row filter: ves {len(regs)} región(es) {regs}"))
except Exception as e:
    resultados.append((False, f"No pude leer silver: {str(e)[:50]}"))

# 4 · máscara: el valor está enmascarado, o lo ves completo por ser lector de PII.
#     Ambos son estados correctos — la máscara está aplicada en los dos casos.
try:
    t = spark.sql("SELECT numero_tarjeta FROM silver_transacciones LIMIT 1").collect()[0][0]
    enmascarado = t and t.startswith("****")
    ok_mask = enmascarado or soy_lector_pii
    detalle = "enmascarado" if enmascarado else ("visible por ser lector de PII" if soy_lector_pii
                                                 else "SIN máscara — ¿aplicaste el SET MASK?")
    resultados.append((ok_mask, f"Column masking: ves '{t}' ({detalle})"))
except Exception as e:
    resultados.append((False, f"No pude leer silver: {str(e)[:50]}"))

print("=" * 68)
print("  MÓDULO 4 · CHECKPOINT")
print("=" * 68)
for ok, msg in resultados:
    print(f"  {'✅' if ok else '❌'}  {msg}")
print("=" * 68)
if all(ok for ok, _ in resultados):
    print("""
  🎉 ¡Módulo 4 completo!
    · Documentaste tablas y columnas (descripciones)
    · Clasificaste el PII con tags y lo encontraste con una consulta
    · Aplicaste row filter y viste solo tu región
    · Enmascaraste el número de tarjeta
    · Otorgaste acceso a un grupo
    · Exploraste lineage, insights y quality

  Gobierno que se ve, no solo que se configura.

  ⚠️  ANTES de seguir: corre el Paso 8 (limpieza) para quitar el row filter.
  👉 Después, en el Módulo 5 orquestas todo esto como un job programado.
""")
else:
    print("  ⚠️  Revisa los ❌. Si el row filter te dejó sin datos, usa la celda de rescate del paso 3.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧹 Paso 8 · Limpieza OBLIGATORIA — quita el row filter y la máscara ⚠️
# MAGIC
# MAGIC **No te saltes esta celda.** El row filter que aplicaste deja `silver_transacciones`
# MAGIC mostrando **solo tu región**. Si sigues al Módulo 5 (o re-ejecutas el pipeline del M3) con
# MAGIC el filtro puesto, el pipeline recalcula `gold_riesgo_diario` **viendo solo tu región** — y
# MAGIC el dashboard y el modelo del Módulo 6/7 saldrían sesgados a una sola región.
# MAGIC
# MAGIC Esta celda **quita el row filter y la máscara**, y **verifica** que vuelves a ver todas las
# MAGIC regiones. Déjala en verde antes de pasar al Módulo 5.

# COMMAND ----------

# quita el row filter y la máscara (idempotente: no falla si ya no están)
for ddl in [
    "ALTER TABLE silver_transacciones DROP ROW FILTER",
    "ALTER TABLE silver_transacciones ALTER COLUMN numero_tarjeta DROP MASK",
]:
    try:
        spark.sql(ddl)
        print(f"✅ {ddl}")
    except Exception as e:
        print(f"ℹ️  {ddl.split('silver_transacciones')[1].strip()} — ya estaba quitado ({str(e)[:50]})")

# verificación: ¿vuelvo a ver TODAS las regiones?
regiones = [r[0] for r in spark.sql("SELECT DISTINCT region FROM silver_transacciones").collect()]
print()
print("=" * 68)
if len(regiones) >= 3:
    print(f"  ✅ LIMPIO · silver_transacciones vuelve a mostrar {len(regiones)} regiones: {sorted(regiones)}")
    print("     Ya puedes pasar al Módulo 5 sin riesgo de congelar el gold en una sola región.")
else:
    print(f"  ❌ ATENCIÓN · silver_transacciones todavía muestra solo {len(regiones)}: {sorted(regiones)}")
    print("     El row filter NO se quitó del todo. NO sigas al Módulo 5 así.")
    print("     Vuelve a ejecutar esta celda; si persiste, avísale a un TA.")
print("=" * 68)

# COMMAND ----------

# MAGIC %md
# MAGIC # 📌 Lo que te llevas
# MAGIC
# MAGIC | Concepto | Dónde lo viviste |
# MAGIC |---|---|
# MAGIC | **Descripciones** documentan el dato | `COMMENT ON TABLE/COLUMN` |
# MAGIC | **Tags** clasifican (PII, dominio) | `SET TAGS` + `information_schema` |
# MAGIC | **Row filter**: quién ve qué fila | Viste solo tu región |
# MAGIC | **Column masking**: quién ve qué valor | `****-****-****-1234` |
# MAGIC | **Permissions** a grupos | `GRANT ... TO grupo` *(informativo)* |
# MAGIC | **Lineage / Insights / Quality** | Catalog Explorer |
# MAGIC
# MAGIC ## ⚠️ Recordatorio
# MAGIC A escala real, los tags habilitan **ABAC** (gobernar por atributo: «toda columna PII se
# MAGIC enmascara») en vez de tabla por tabla, y los grupos se sincronizan desde el proveedor de
# MAGIC identidad. Los conceptos son los mismos.
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC Tienes datos confiables (M3), documentados, clasificados y gobernados (M4). En el
# MAGIC **Módulo 5** conviertes todo el flujo en un **job programado** que corre solo.
