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
# MAGIC | 2 | **Tags y datos confidenciales**: clasificar |
# MAGIC | 3 | **Row filter**: quién ve qué país ⭐ |
# MAGIC | 4 | **Column masking**: quién ve el costo (el margen) |
# MAGIC | 5 | **Permissions**: cómo se otorga el acceso *(informativo)* |
# MAGIC | 6 | **Lineage, insights y quality** en Catalog Explorer |
# MAGIC
# MAGIC ## El truco de este módulo
# MAGIC No puedes iniciar sesión como otra persona para probar que un control funciona. Así que
# MAGIC **simulas tu identidad** con dos campos (país y acceso a datos de costo): aplicas el
# MAGIC control, y luego **cambias los campos y vuelves a aplicar** para ver el efecto en vivo.
# MAGIC
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Datos 100% sintéticos. Requiere Unity Catalog — no
# MAGIC > funciona en Free Edition.
# MAGIC >
# MAGIC > 💡 Gobernar datos es de cualquier industria: un banco enmascara tarjetas, un hospital
# MAGIC > diagnósticos, un retailer el costo y el margen. Cambia el dato.
# MAGIC
# MAGIC ## 🎭 Cómo simulamos "quién eres" hoy
# MAGIC En una empresa real, quién ve qué se decide por el **grupo** al que perteneces
# MAGIC (`is_account_group_member(...)`). Pero hoy cada quien está en **su propio workspace**, sin
# MAGIC grupos. Así que simulamos tu identidad con **dos campos** arriba del notebook: tu **país**
# MAGIC y si tienes **acceso a datos de costo**.

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
# MAGIC Ejecuta esta celda. Arriba aparecen dos campos: elige tu **país** y si tienes **acceso a
# MAGIC datos de costo**. Podrás cambiarlos más adelante para ver el efecto.

# COMMAND ----------

# dos campos que simulan "quién eres" — sin depender de grupos del workspace
dbutils.widgets.dropdown("mi_pais", "CO", ["CO", "MX", "BR", "CL", "PE", "AR"], "1 · Tu país")
dbutils.widgets.dropdown("veo_costo", "no", ["no", "sí"], "2 · ¿Ves datos de costo/margen?")

mi_pais = dbutils.widgets.get("mi_pais")
veo_costo = dbutils.widgets.get("veo_costo") == "sí"

print(f"🌎 Tu país         : {mi_pais}")
print(f"💰 Acceso a costo  : {'sí' if veo_costo else 'no'}")
print()
print("Estos dos campos simulan tu identidad. Cámbialos arriba y re-ejecuta las celdas")
print("de los pasos 3 y 4 para ver cómo cambia lo que ves.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sobre qué tabla trabajamos: `silver_ventas`
# MAGIC
# MAGIC Gobernamos la capa **silver** — tus datos limpios del Módulo 3. Tiene las dos columnas que
# MAGIC este módulo necesita: **`pais`** (para el row filter) y **`costo_unitario`** (para la
# MAGIC máscara — es el dato confidencial del negocio, revela el margen).

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 · Descripciones: documenta tus datos
# MAGIC
# MAGIC Un dato que nadie entiende, nadie lo usa — o peor, lo usa mal. **Documentar es gobierno.**
# MAGIC Y hay un beneficio extra: **Genie y el Assistant leen estos comentarios** (Módulo 7).
# MAGIC Documentar mejora las respuestas de la IA sobre tus datos.
# MAGIC
# MAGIC > 🖱️ **También por la UI:** en **Catalog Explorer** → `silver_ventas`, cada tabla y columna
# MAGIC > tiene un campo de comentario que editas con un clic (ícono de lápiz). Usa el SQL de abajo,
# MAGIC > o hazlo por la UI — lo que prefieras.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE silver_ventas IS
# MAGIC   'Ventas limpias y enriquecidas con datos del producto. Contiene costo_unitario (confidencial).';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 TU TURNO — documenta dos columnas
# MAGIC
# MAGIC Agrega un comentario a `monto` y a `utilidad` de `silver_ventas`.
# MAGIC
# MAGIC <details>
# MAGIC <summary>💡 Solución</summary>
# MAGIC
# MAGIC <pre>COMMENT ON COLUMN silver_ventas.monto IS
# MAGIC   'Valor de la línea de venta, en dólares (USD)';
# MAGIC COMMENT ON COLUMN silver_ventas.utilidad IS
# MAGIC   'Utilidad de la línea: monto menos costo. Dato sensible de margen';</pre>
# MAGIC </details>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: comenta silver_ventas.monto y .utilidad
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE silver_ventas

# COMMAND ----------

# MAGIC %md
# MAGIC 👀 En la columna `comment` del resultado ves tus descripciones. También aparecen en
# MAGIC **Catalog Explorer**, que exploras al final del módulo.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 · Tags y datos confidenciales
# MAGIC
# MAGIC Con 4 tablas puedes proteger columna por columna. **Con 10.000, no.** Los **tags** te
# MAGIC dejan clasificar los datos —"esto es confidencial", "esto es de finanzas"— y luego
# MAGIC gobernar **por clasificación**, no tabla por tabla. Es la base de ABAC (control de acceso
# MAGIC por atributo). El primer paso de proteger datos sensibles es **saber dónde están**.
# MAGIC
# MAGIC Esta celda ya está resuelta: clasifica la tabla y marca `costo_unitario` como confidencial.
# MAGIC
# MAGIC > 🖱️ **También por la UI:** en **Catalog Explorer** → `silver_ventas` → pestaña **Details**
# MAGIC > (o el ícono de tags), agregas tags con clics. Ejecuta el SQL de abajo o hazlo por la UI.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver_ventas
# MAGIC SET TAGS ('dominio' = 'retail', 'contiene_confidencial' = 'true');
# MAGIC
# MAGIC ALTER TABLE silver_ventas
# MAGIC ALTER COLUMN costo_unitario
# MAGIC SET TAGS ('clasificacion' = 'confidencial', 'tipo' = 'costo');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encuentra los datos confidenciales de todo tu schema con una consulta
# MAGIC
# MAGIC Esto es lo que los tags hacen posible: preguntar *«¿dónde están mis datos sensibles?»* y
# MAGIC obtener la respuesta al instante, aunque tuvieras miles de tablas.

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM {catalogo}.information_schema.column_tags
    WHERE schema_name = '{schema}'
      AND tag_value = 'confidencial'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC 🎉 Encontraste tu columna confidencial **sin abrir cada tabla**. Con este mismo patrón,
# MAGIC una empresa localiza cada dato sensible de todo su catálogo.
# MAGIC
# MAGIC > 💡 Esto habilita **ABAC**: en vez de enmascarar `costo_unitario` tabla por tabla, defines
# MAGIC > una política *«toda columna con tag `confidencial` se enmascara»*. Una regla, no mil.
# MAGIC >
# MAGIC > 🤖 Y a escala, **Data Classification** (un agente de IA de Databricks) escanea todo el
# MAGIC > catálogo y etiqueta solo el PII/PCI/datos sensibles, con tags precisos (`class.credit_card`,
# MAGIC > `class.email_address`…). Se activa desde **Catalog Explorer → tu catálogo → clasificación**.
# MAGIC > Requiere permisos de administración, por eso hoy solo lo conoces.

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
# MAGIC | La regla se ve como | *«el grupo `finanzas` puede leer la tabla X»* | *«toda columna con tag `confidencial` se enmascara»* |
# MAGIC | Dónde se define | Objeto por objeto (`GRANT` en tabla/schema) | Una **política** que aplica a todo lo que tenga el tag |
# MAGIC | Escala | Bien con pocas tablas | Bien con **miles** de tablas |
# MAGIC | Ejemplo | «los analistas ven `silver_ventas`» | «nadie ve columnas `confidencial`, salvo `finanzas`» |
# MAGIC
# MAGIC ## Cuándo usar cada uno
# MAGIC - **RBAC** para el acceso base: *quién entra a qué tabla o schema* (lo del Paso 5). Es simple
# MAGIC   y suele ser suficiente para dar/quitar acceso a un conjunto de datos.
# MAGIC - **ABAC** para reglas **transversales** que deben valer en todo el catálogo sin repetirlas:
# MAGIC   *«todo lo que sea PII / confidencial se protege igual, esté donde esté»*.
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
# MAGIC # Paso 3 · Row filter ⭐ — quién ve qué país
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Hacer que `silver_ventas` te muestre **solo las ventas de tu país** (el que elegiste
# MAGIC arriba), sin duplicar la tabla ni crear una vista.
# MAGIC
# MAGIC > 🖱️ **También por la UI:** una vez creada la función de filtro (celda de abajo), puedes
# MAGIC > aplicarla desde **Catalog Explorer** → `silver_ventas` → **Permissions / Row filter**. Acá
# MAGIC > lo hacemos por SQL para verlo completo, pero la opción de UI existe.
# MAGIC
# MAGIC ### Primero, cuenta cuántas filas ves — y anótalo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS filas_totales FROM silver_ventas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crea la función de filtro
# MAGIC
# MAGIC Una función que decide, fila por fila, si la ves: **¿el país de la fila es el tuyo?**
# MAGIC
# MAGIC En una empresa real, en vez de comparar con un país fijo, se usa
# MAGIC `is_account_group_member('z2h_pais_' || pais_fila)` — así la respuesta depende del grupo
# MAGIC de quien consulta. Hoy comparamos con **tu país del widget**. La mecánica es idéntica.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE FUNCTION filtro_pais(pais_fila STRING)
    RETURN pais_fila = '{mi_pais}'
""")
print(f"✅ Función filtro_pais creada. Tu país es: {mi_pais}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplica el filtro a la tabla
# MAGIC ALTER TABLE silver_ventas SET ROW FILTER filtro_pais ON (pais)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿de qué país son las filas que ves? solo el tuyo
# MAGIC SELECT pais, COUNT(*) AS filas FROM silver_ventas GROUP BY pais

# COMMAND ----------

# MAGIC %md
# MAGIC 🎉 **Ahora solo ves tu país** — sobre la **misma tabla**, sin copia.
# MAGIC
# MAGIC ### 🔁 Compruébalo tú mismo: cambia de identidad
# MAGIC 1. Arriba, cambia el campo **Tu país** (por ejemplo, de `CO` a `BR`)
# MAGIC 2. Re-ejecuta la celda que crea `filtro_pais` (dos celdas más arriba)
# MAGIC 3. Vuelve a contar → **ves otras filas, las del país nuevo**
# MAGIC
# MAGIC El mismo `SELECT`, distinto resultado — porque cambió **quién pregunta**. Eso es
# MAGIC exactamente lo que pasaría con dos analistas de países distintos mirando la misma tabla.
# MAGIC
# MAGIC > 💡 Cada país tiene un volumen distinto de ventas (Brasil y México mueven más), así que
# MAGIC > el conteo cambiará según el país que elijas. Es lo esperado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🆘 Celda de rescate — QUITAR EL ROW FILTER
# MAGIC ¿Te quedaste sin ver ninguna fila, o quieres volver al estado inicial? Pon `QUITAR = True`
# MAGIC en la celda de abajo y ejecútala.

# COMMAND ----------

QUITAR = False   # ← cámbialo a True si quieres quitar el row filter

if QUITAR:
    spark.sql("ALTER TABLE silver_ventas DROP ROW FILTER")
    print("✅ Row filter quitado. Vuelves a ver todas las filas.")
else:
    print("Row filter intacto. (Pon QUITAR = True arriba si quieres quitarlo.)")

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 · Column masking — quién ve el costo (el margen)
# MAGIC
# MAGIC ## Qué vas a hacer
# MAGIC Enmascarar `costo_unitario` (la columna que clasificaste como confidencial) para que solo
# MAGIC quien tiene **acceso a datos de costo** vea el valor real. El costo revela el **margen** —
# MAGIC un analista de ventas no debería verlo, finanzas sí.
# MAGIC
# MAGIC > 🖱️ **También por la UI:** creada la función de máscara (celda de abajo), se aplica desde
# MAGIC > **Catalog Explorer** → `silver_ventas` → la columna `costo_unitario` → **Mask**. Acá lo
# MAGIC > hacemos por SQL; la opción de UI existe.
# MAGIC
# MAGIC ### Míralo sin máscara — se ve completo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nombre_producto, monto, costo_unitario, utilidad FROM silver_ventas LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crea la máscara
# MAGIC
# MAGIC Una función que devuelve el costo real **solo si tienes acceso**; si no, `NULL`.
# MAGIC
# MAGIC En una empresa real, "tienes acceso a costo" se comprueba con
# MAGIC `is_account_group_member('z2h_finanzas')`. Hoy usamos tu campo **¿Ves datos de costo?** del
# MAGIC widget. La mecánica del column mask es idéntica.

# COMMAND ----------

_puede_ver = "true" if veo_costo else "false"
spark.sql(f"""
    CREATE OR REPLACE FUNCTION enmascarar_costo(costo DOUBLE)
    RETURN CASE WHEN {_puede_ver} THEN costo ELSE NULL END
""")
print(f"✅ Máscara creada. ¿Ves datos de costo? {'sí' if veo_costo else 'no'}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplica la máscara a la columna
# MAGIC ALTER TABLE silver_ventas
# MAGIC ALTER COLUMN costo_unitario SET MASK enmascarar_costo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta otra vez
# MAGIC SELECT nombre_producto, monto, costo_unitario, utilidad FROM silver_ventas LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC Si dejaste **¿Ves datos de costo? = no** (lo normal), `costo_unitario` sale en `NULL`. La
# MAGIC columna no se movió ni se copió: devuelve un valor distinto según quién lee.
# MAGIC
# MAGIC ### 🔁 Compruébalo: cambia de identidad
# MAGIC 1. Arriba, cambia **¿Ves datos de costo?** a `sí`
# MAGIC 2. Re-ejecuta la celda que crea `enmascarar_costo` y el `ALTER`
# MAGIC 3. Consulta de nuevo → **ahora ves el costo real**
# MAGIC
# MAGIC Misma columna, misma consulta, distinto resultado — según quién pregunta. Así, un analista
# MAGIC de ventas ve `NULL` en el costo y alguien de finanzas ve el valor.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 5 · Permissions — cómo se otorga el acceso *(solo lectura)*
# MAGIC
# MAGIC Este paso es **para conocerlo, no para ejecutarlo** — no hay nada que completar.
# MAGIC
# MAGIC Con Unity Catalog gestionas quién accede a cada objeto con `GRANT` y `REVOKE`. La regla de
# MAGIC oro: **otorga a grupos, no a personas** — el acceso describe un rol, no un individuo.
# MAGIC
# MAGIC ```sql
# MAGIC GRANT SELECT ON TABLE silver_ventas TO `analistas_comercial`;
# MAGIC REVOKE SELECT ON TABLE silver_ventas FROM `analistas_comercial`;
# MAGIC SHOW GRANTS ON TABLE silver_ventas;
# MAGIC ```
# MAGIC
# MAGIC | Concepto | |
# MAGIC |---|---|
# MAGIC | **A grupos, no a personas** | El acceso describe un rol; sobrevive a cambios de equipo |
# MAGIC | **Herencia** | Para leer una tabla: `USE CATALOG` + `USE SCHEMA` + `SELECT` |
# MAGIC | **También desde la UI** | Catalog Explorer → pestaña **Permissions**, con clics |
# MAGIC
# MAGIC > 💡 No lo ejecutamos hoy porque en tu propio workspace ya eres dueño de todo. Pero así es
# MAGIC > como controlarías el acceso en tu organización.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 6 · Catalog Explorer: lineage, insights y quality
# MAGIC
# MAGIC Abre **Catalog** (menú izquierdo) → tu catálogo → `{schema}` → `silver_ventas`.
# MAGIC
# MAGIC | Pestaña | Qué vas a ver | Lo que ya hiciste hoy |
# MAGIC |---|---|---|
# MAGIC | **Overview** | Columnas, tipos y **tus comentarios** (paso 1) | Descripciones |
# MAGIC | **Details / Tags** | Los **tags** que pusiste (paso 2) | Clasificación confidencial |
# MAGIC | **Permissions** | Quién tiene acceso | Donde se gestiona con clics |
# MAGIC | **Lineage** | De qué tablas viene y quién la consume | ⬇️ míralo con cuidado |
# MAGIC | **Insights / Quality** | Uso y salud de la tabla | Uso real |
# MAGIC | **History** | Versiones Delta (como en el M1) | Auditoría |
# MAGIC
# MAGIC ### 🔎 Detente en Lineage
# MAGIC ```
# MAGIC   archivos raw → bronze → silver → gold_* → (pronto: dashboard, Genie, app)
# MAGIC ```
# MAGIC **Nadie lo dibujó.** Unity Catalog lo registró solo. Sirve para *análisis de impacto*
# MAGIC («si cambio silver, ¿qué se rompe?»), *auditoría* («¿de qué archivo salió este número?») y
# MAGIC *confianza* (un dato con linaje visible es un dato en el que puedes confiar).
# MAGIC
# MAGIC > 💡 **Data Quality Monitoring / Anomaly Detection**: Unity Catalog puede vigilar la
# MAGIC > frescura y completitud de una tabla **sin que escribas reglas** — aprende su
# MAGIC > comportamiento normal y avisa si un día no llegan datos, o llegan a medias. Complementa
# MAGIC > las expectativas del M3 (que tú defines). Se activa desde **Catalog Explorer → tu schema
# MAGIC > → Quality**; requiere el privilegio `MANAGE`, por eso hoy solo lo conoces.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 7 · Verifica tu checkpoint

# COMMAND ----------

resultados = []

try:
    d = spark.sql("DESCRIBE TABLE silver_ventas").collect()
    con_comentario = any(r["col_name"] == "monto" and r["comment"] for r in d
                         if r["col_name"] and r["comment"])
    resultados.append((con_comentario, "Descripciones: monto tiene comentario"))
except Exception as e:
    resultados.append((False, f"No pude leer comentarios: {str(e)[:50]}"))

try:
    n = spark.sql(f"""SELECT COUNT(*) c FROM {catalogo}.information_schema.column_tags
                      WHERE schema_name = '{schema}' AND tag_value = 'confidencial'""").collect()[0][0]
    resultados.append((n > 0, f"Tags: {n} columna(s) clasificada(s) como confidencial"))
except Exception as e:
    resultados.append((False, f"No pude leer tags: {str(e)[:50]}"))

try:
    paises = [r[0] for r in spark.sql("SELECT DISTINCT pais FROM silver_ventas").collect()]
    resultados.append((len(paises) <= 1, f"Row filter: ves {len(paises)} país(es) {paises}"))
except Exception as e:
    resultados.append((False, f"No pude leer silver: {str(e)[:50]}"))

try:
    c = spark.sql("SELECT costo_unitario FROM silver_ventas LIMIT 1").collect()[0][0]
    enmascarado = c is None
    ok_mask = enmascarado or veo_costo
    detalle = "enmascarado (NULL)" if enmascarado else ("visible por tener acceso" if veo_costo
                                                        else "SIN máscara — ¿aplicaste el SET MASK?")
    resultados.append((ok_mask, f"Column masking: costo = {c} ({detalle})"))
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
    · Clasificaste el dato confidencial con tags y lo encontraste con una consulta
    · Aplicaste row filter y viste solo tu país
    · Enmascaraste el costo (el margen)
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
# MAGIC **No te saltes esta celda.** El row filter que aplicaste deja `silver_ventas` mostrando
# MAGIC **solo tu país**. Si sigues al Módulo 5 (o re-ejecutas el pipeline del M3) con el filtro
# MAGIC puesto, el pipeline recalcula `gold_ventas_diarias` **viendo solo tu país** — y el
# MAGIC pronóstico y el dashboard del Módulo 7 saldrían con **un solo país**.
# MAGIC
# MAGIC Esta celda **quita el row filter y la máscara**, y **verifica** que vuelves a ver los 6
# MAGIC países. Déjala en verde antes de pasar al Módulo 5.

# COMMAND ----------

# quita el row filter y la máscara (idempotente: no falla si ya no están)
for ddl in [
    "ALTER TABLE silver_ventas DROP ROW FILTER",
    "ALTER TABLE silver_ventas ALTER COLUMN costo_unitario DROP MASK",
]:
    try:
        spark.sql(ddl)
        print(f"✅ {ddl}")
    except Exception as e:
        # si no había filtro/máscara, Databricks avisa — no es un error para nosotros
        print(f"ℹ️  {ddl.split('silver_ventas')[1].strip()} — ya estaba quitado ({str(e)[:50]})")

# verificación: ¿vuelvo a ver TODOS los países?
paises = [r[0] for r in spark.sql("SELECT DISTINCT pais FROM silver_ventas").collect()]
print()
print("=" * 68)
if len(paises) >= 6:
    print(f"  ✅ LIMPIO · silver_ventas vuelve a mostrar {len(paises)} países: {sorted(paises)}")
    print("     Ya puedes pasar al Módulo 5 sin riesgo de congelar el gold en un solo país.")
else:
    print(f"  ❌ ATENCIÓN · silver_ventas todavía muestra solo {len(paises)}: {sorted(paises)}")
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
# MAGIC | **Tags** clasifican (confidencial, dominio) | `SET TAGS` + `information_schema` |
# MAGIC | **Row filter**: quién ve qué fila | Viste solo tu país |
# MAGIC | **Column masking**: quién ve qué valor | El costo en `NULL` |
# MAGIC | **Permissions** a grupos | `GRANT ... TO grupo` *(informativo)* |
# MAGIC | **Lineage / Insights / Quality** | Catalog Explorer |
# MAGIC
# MAGIC ## Lo que sigue
# MAGIC Tienes datos confiables (M3), documentados, clasificados y gobernados (M4). En el
# MAGIC **Módulo 5** conviertes todo el flujo en un **job programado** que corre solo.
