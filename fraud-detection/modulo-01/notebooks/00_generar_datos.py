# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🏦 Generador de datos — Detección de fraude
# MAGIC
# MAGIC Este notebook **crea los datos sintéticos** que vas a usar durante todo el taller.
# MAGIC Córrelo una sola vez, al inicio.
# MAGIC
# MAGIC ## El caso de negocio
# MAGIC
# MAGIC Trabajas en el área de datos de un banco. El equipo de riesgo necesita **detectar
# MAGIC transacciones fraudulentas** antes de que el dinero se pierda, y hoy se enteran
# MAGIC demasiado tarde.
# MAGIC
# MAGIC Durante el día vas a construir el camino completo: desde los archivos crudos que
# MAGIC llegan del sistema de pagos, hasta un modelo que califica cada transacción y una
# MAGIC aplicación que el equipo de operaciones puede usar.
# MAGIC
# MAGIC ## Qué crea este notebook
# MAGIC
# MAGIC | Archivo | Qué es | Formato |
# MAGIC |---|---|---|
# MAGIC | `raw/transacciones/` | 12 archivos · ~377.000 transacciones | JSON |
# MAGIC | `raw/clientes/` | ~18.500 clientes | CSV |
# MAGIC | `raw/transacciones_nuevas/` | 1 archivo · ~20.000 transacciones que "llegan después" | JSON |
# MAGIC
# MAGIC > 🔒 **Todos los datos son 100% sintéticos.** No hay información real de ninguna
# MAGIC > persona ni de ninguna institución financiera. Los nombres de comercios son
# MAGIC > inventados.
# MAGIC >
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Este generador y los notebooks
# MAGIC > del taller enseñan conceptos; no están pensados para copiarse a producción tal cual.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 · Elige dónde trabajar
# MAGIC
# MAGIC Son **dos celdas**: la **A** crea los campos, la **B** los lee. Van separadas porque
# MAGIC los campos no existen hasta que corres la celda A — antes de eso no habría nada
# MAGIC dónde escribir.
# MAGIC
# MAGIC Los dos campos son:
# MAGIC
# MAGIC | Campo | Qué poner |
# MAGIC |---|---|
# MAGIC | **catalogo** | El catálogo donde vas a trabajar. **Debe existir de antemano.** En Free Edition suele ser `workspace` |
# MAGIC | **schema** | El nombre de tu espacio de trabajo. Lo eliges tú (p. ej. `z2h_ana`). **Se crea si no existe** |
# MAGIC
# MAGIC > ⭐ **Este es el momento más importante del día para no tener problemas después.**
# MAGIC > El catálogo y el schema que declares aquí son los que vas a usar en **todos** los
# MAGIC > módulos (M1 a M7). **Anótalos** y escríbelos **idénticos** en cada notebook. La causa
# MAGIC > #1 de errores en el taller es usar un nombre distinto en cada módulo.
# MAGIC >
# MAGIC > ⚠️ **No adivinamos nada por ti.** Los campos empiezan **vacíos**: tú declaras dónde
# MAGIC > trabajas. Si los dejas vacíos, el notebook se detiene y te lo pide.
# MAGIC >
# MAGIC > 💡 **Sobre el catálogo:** casi ningún workspace deja crear catálogos. Usa uno que
# MAGIC > **ya exista** (en Free Edition, `workspace`). Si no sabes cuál, esta celda te lista
# MAGIC > los que tienes disponibles.

# COMMAND ----------

# ═══ CELDA A · crea los campos y mira qué catálogos existen ═══════════════
# Ejecuta esta celda. Después escribe tu catálogo y tu schema en los campos de ARRIBA,
# y sigue con la celda B.

import re

# Campos VACÍOS: tú declaras explícitamente dónde trabajas. No se infiere ni se propone nada.
dbutils.widgets.text("catalogo", "", "1 · Catálogo")
dbutils.widgets.text("schema",   "", "2 · Schema (tu espacio de trabajo)")

# solo para AYUDARTE a elegir: se listan los catálogos que ya existen (no se elige por ti)
_ocultos = ("system", "samples", "__databricks_internal", "hive_metastore")
_disponibles = [r[0] for r in spark.sql("SHOW CATALOGS").collect()
                if r[0].lower() not in _ocultos]

print("👆 Aparecieron dos campos ARRIBA. Escribe en ellos tu CATÁLOGO y tu SCHEMA.\n")
print("📦 Catálogos que ya existen en este workspace (elige uno de estos para el campo 1):")
for c in _disponibles:
    print(f"     · {c}")
print()
print("💡 En Free Edition usa 'workspace'. El notebook NO crea catálogos: usa uno que ya exista.")
print("   El SCHEMA sí lo creamos: escribe el nombre que quieras (p. ej. z2h_tu_nombre).")
print("⭐ ANOTA los dos nombres: los usarás IGUALES en todos los módulos. Sigue con la celda B.")

# COMMAND ----------

# ═══ CELDA B · leer los campos ════════════════════════════════════════════
# Si cambias algo en los campos de arriba, vuelve a ejecutar desde esta celda.

catalogo = dbutils.widgets.get("catalogo").strip()
schema = dbutils.widgets.get("schema").strip()

if not catalogo or not schema:
    raise Exception(
        "\n❌ Falta declarar tu espacio de trabajo.\n"
        "   Escribe el CATÁLOGO y el SCHEMA en los campos de arriba y vuelve a correr.\n"
        "   ⭐ Anótalos: son los que usarás en TODOS los módulos del taller.\n"
    )

print(f"📦 Catálogo : {catalogo}")
print(f"📁 Schema   : {schema}")
print("⭐ Anota estos dos nombres — los repetirás IGUALES en cada módulo (M1 a M7).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crear el catálogo y el schema si no existen

# COMMAND ----------

# ── validación de los nombres ──
for nombre, valor in (("catalogo", catalogo), ("schema", schema)):
    if not valor:
        raise ValueError(f"El campo '{nombre}' está vacío. Complétalo arriba.")
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", valor):
        raise ValueError(
            f"'{valor}' no es un nombre válido para {nombre}. Usa solo letras, números y "
            f"guión bajo, empezando por una letra."
        )

catalogos = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]


def catalogos_escribibles():
    """Devuelve los catálogos donde este usuario sí puede crear schemas.

    Se comprueba intentando crear y borrar un schema: un catálogo puede estar visible y
    no ser escribible, y eso solo se descubre intentándolo.
    """
    ok = []
    for c in catalogos:
        if c.lower() in ("system", "samples", "__databricks_internal", "hive_metastore"):
            continue
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{c}`._z2h_probe")
            spark.sql(f"DROP SCHEMA IF EXISTS `{c}`._z2h_probe")
            ok.append(c)
        except Exception:
            pass
    return ok


# ── 1 · el catálogo ──
if catalogo in catalogos:
    print(f"✅ El catálogo '{catalogo}' ya existe")
else:
    print(f"ℹ️  El catálogo '{catalogo}' no existe — intentando crearlo…")
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalogo}`")
        print(f"✅ Catálogo '{catalogo}' creado")
    except Exception as e:
        posibles = catalogos_escribibles()
        mensaje = (
            f"\n❌ No se pudo crear el catálogo '{catalogo}'.\n"
            f"   Motivo: {str(e).split(chr(10))[0][:160]}\n\n"
            f"   Esto es normal: la mayoría de los workspaces corporativos no permiten\n"
            f"   crear catálogos. No hace falta uno nuevo para el taller.\n\n"
        )
        if posibles:
            mensaje += (
                f"   👉 Catálogos donde SÍ puedes trabajar:\n"
                + "".join(f"        · {c}\n" for c in posibles[:10])
                + f"\n   Escribe uno de esos en el campo 'catalogo' de arriba y vuelve a\n"
                f"   ejecutar esta celda.\n"
            )
        else:
            mensaje += (
                "   ⚠️  Tampoco encontré ningún catálogo donde puedas crear schemas.\n"
                "   Pídele a tu administrador acceso a uno, o avísale a un TA.\n"
            )
        raise Exception(mensaje)

# ── 2 · el schema ──
schemas = [r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{catalogo}`").collect()]
if schema in schemas:
    print(f"✅ El schema '{catalogo}.{schema}' ya existe")
else:
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalogo}`.`{schema}`")
        print(f"✅ Schema '{catalogo}.{schema}' creado")
    except Exception as e:
        raise Exception(
            f"\n❌ No se pudo crear el schema en '{catalogo}'.\n"
            f"   Motivo: {str(e).split(chr(10))[0][:160]}\n\n"
            f"   Necesitas permiso CREATE SCHEMA en ese catálogo. Prueba con otro\n"
            f"   catálogo en el campo de arriba, o avísale a un TA.\n"
        )

spark.sql(f"USE `{catalogo}`.`{schema}`")

# ── 3 · el volumen para los archivos crudos ──
# En el taller los datos "llegan" como archivos: es lo que hace realista la ingesta del M2.
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalogo}`.`{schema}`.raw")
    print(f"✅ Volumen '{catalogo}.{schema}.raw' listo")
except Exception as e:
    raise Exception(
        f"\n❌ No se pudo crear el volumen.\n"
        f"   Motivo: {str(e).split(chr(10))[0][:160]}\n\n"
        f"   Necesitas permiso CREATE VOLUME en el schema. Avísale a un TA.\n"
    )

RAW = f"/Volumes/{catalogo}/{schema}/raw"
usuario = spark.sql("SELECT current_user()").collect()[0][0]

print()
print("─" * 60)
print(f"  Trabajarás en : {catalogo}.{schema}")
print(f"  Archivos en   : {RAW}")
print("─" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 · Generar los clientes
# MAGIC
# MAGIC ~18.500 clientes con su segmento, antigüedad, región y score crediticio.
# MAGIC
# MAGIC Fíjate en los detalles: las cantidades **no son redondas**, los ids **tienen huecos**,
# MAGIC y las distribuciones **no reparten parejo**. Un dataset sintético mal hecho se nota
# MAGIC justamente por lo contrario.
# MAGIC
# MAGIC La columna **`region`** (`norte` / `centro` / `sur`) es importante: en el **Módulo 4**
# MAGIC vas a aplicar una regla de seguridad que filtra por región y vas a ver el efecto en
# MAGIC vivo.

# COMMAND ----------

from pyspark.sql import functions as F

# Cantidades deliberadamente NO redondas. Un dataset con exactamente 20.000 clientes y
# 400.000 transacciones se ve fabricado a simple vista; los datos reales nunca son así.
N_CLIENTES = 18_473
SEMILLA = 42  # fija, para que todos generen exactamente los mismos datos

clientes = (
    spark.range(1, N_CLIENTES + 1)
    .withColumnRenamed("id", "n")
    # IDs con hueco: los datos reales tienen bajas, cuentas canceladas, saltos de secuencia.
    #
    # La fórmula es ARITMÉTICA PURA (n*7+3), sin hash y sin rand. Dos razones:
    #  · las transacciones tienen que reconstruir el mismo id para que el join case
    #  · HASH() devuelve valores DISTINTOS para BIGINT y para INT — y range() produce
    #    BIGINT mientras que el lado de transacciones produce INT. Con hash, el join
    #    perdía dos tercios de las filas.
    .withColumn("_cn", F.col("n") * 7 + F.lit(3))
    .withColumn("cliente_id",
                F.concat(F.lit("CLI"), F.lpad(F.col("_cn").cast("string"), 7, "0")))
    # segmento: proporciones no redondas
    .withColumn("_r", F.rand(SEMILLA))
    .withColumn(
        "segmento",
        F.when(F.col("_r") < 0.6382, "retail")
        .when(F.col("_r") < 0.9127, "premium")
        .otherwise("empresarial"),
    )
    # antigüedad: sesgada a clientes nuevos (una cartera real capta más de lo que retiene),
    # con un tope distinto por segmento
    .withColumn(
        "antiguedad_meses",
        F.when(F.col("segmento") == "empresarial",
               F.pow(F.rand(SEMILLA + 1), F.lit(0.75)) * 214 + 4)
        .otherwise(F.pow(F.rand(SEMILLA + 1), F.lit(1.6)) * 178 + 1).cast("int"),
    )
    .withColumn("_rr", F.rand(SEMILLA + 2))
    .withColumn(
        "region",
        F.when(F.col("_rr") < 0.2841, "norte")
        .when(F.col("_rr") < 0.7683, "centro")   # centro concentra ~48%
        .otherwise("sur"),
    )
    # edad: distribución con forma, no plana. Concentrada en 28–45, con cola hacia arriba.
    .withColumn(
        "edad",
        F.least(
            F.lit(88),
            F.greatest(F.lit(18), (F.randn(SEMILLA + 3) * 13.4 + 39.6).cast("int")),
        ),
    )
    # score crediticio: correlacionado con segmento Y con antigüedad, con dispersión real
    .withColumn(
        "_score_base",
        F.when(F.col("segmento") == "empresarial", 712)
        .when(F.col("segmento") == "premium", 648)
        .otherwise(521),
    )
    .withColumn(
        "score_crediticio",
        F.least(
            F.lit(850),
            F.greatest(
                F.lit(300),
                (F.col("_score_base")
                 + F.col("antiguedad_meses") * 0.42     # más antigüedad, mejor score
                 + F.randn(SEMILLA + 4) * 74).cast("int"),
            ),
        ),
    )
    .select("cliente_id", "segmento", "antiguedad_meses", "region", "edad",
            "score_crediticio")
)

# ── Materializar en una tabla Delta ──
#
# No se usa .cache(): no está soportado en compute serverless. Y hay una razón más
# importante para escribir a disco: las funciones rand() y randn() se REEVALÚAN en cada
# acción. Sin materializar, cada count(), cada display() y cada escritura produciría
# valores distintos — la tabla de clientes no coincidiría con las transacciones que la
# referencian. Escribir a Delta congela los valores una sola vez.
(clientes.write.mode("overwrite").saveAsTable("_gen_clientes"))
clientes = spark.table("_gen_clientes")

n_clientes_real = clientes.count()
print(f"✅ {n_clientes_real:,} clientes generados y materializados")
display(clientes.limit(8))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Se ven realistas?
# MAGIC
# MAGIC Un dataset sintético mal hecho se delata por tener todo repartido en partes iguales.
# MAGIC Estas distribuciones deben verse **desparejas**.

# COMMAND ----------

print("Clientes por segmento y región (no deberían ser proporciones redondas):")
display(
    clientes.groupBy("segmento", "region")
    .agg(F.count("*").alias("clientes"),
         F.round(F.avg("score_crediticio"), 1).alias("score_promedio"),
         F.round(F.avg("antiguedad_meses"), 1).alias("antiguedad_promedio"))
    .orderBy("segmento", "region")
)

print("Distribución de edad (debe tener forma de campana, no plana):")
display(
    clientes.withColumn("rango_edad",
        F.when(F.col("edad") < 25, "18-24")
        .when(F.col("edad") < 35, "25-34")
        .when(F.col("edad") < 45, "35-44")
        .when(F.col("edad") < 55, "45-54")
        .when(F.col("edad") < 65, "55-64")
        .otherwise("65+"))
    .groupBy("rango_edad").count().orderBy("rango_edad")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 · Generar las transacciones
# MAGIC
# MAGIC ~397.000 transacciones a lo largo de 18 meses.
# MAGIC
# MAGIC ### El fraude no es aleatorio
# MAGIC
# MAGIC Si el fraude fuera al azar, ningún modelo podría aprenderlo y el **Módulo 6** no
# MAGIC tendría sentido. Estas son las **señales plantadas** a propósito:
# MAGIC
# MAGIC | Señal | Cómo se ve en los datos |
# MAGIC |---|---|
# MAGIC | Monto muy alto | Transacciones muy por encima del promedio del segmento |
# MAGIC | Transacción en el exterior | `pais` distinto de `CO`, sin historial de viaje |
# MAGIC | Horario atípico | Entre las 02:00 y las 05:00 |
# MAGIC | Canal de riesgo | `online` y `atm` concentran más fraude |
# MAGIC | Cliente nuevo | Poca antigüedad + score bajo |
# MAGIC
# MAGIC El objetivo es un **AUC alcanzable de ~0,85–0,92**: suficientemente bueno para ser
# MAGIC satisfactorio, no tan perfecto que parezca falso.

# COMMAND ----------

N_TX = 397_412   # otra cantidad no redonda

# Comercios inventados — no corresponden a empresas reales.
# El tercer valor es un PESO: los comercios NO son equiprobables. Un supermercado tiene
# muchas más transacciones que una agencia de viajes, y eso tiene que verse en los datos.
COMERCIOS = [
    ("SuperMercado Andino",       "supermercado",     140),
    ("Tienda La Esquina",         "supermercado",      95),
    ("MiniMarket 24h",            "supermercado",      61),
    ("Farmacia Vida",             "salud",             48),
    ("Droguería Central",         "salud",             33),
    ("Restaurante El Fogón",      "restaurante",       42),
    ("Café Montaña",              "restaurante",       58),
    ("Comidas Rápidas Sabor",     "restaurante",       71),
    ("Almacén Textil",            "ropa",              27),
    ("Moda Urbana",               "ropa",              19),
    ("TecnoMundo",                "electronica",       14),
    ("ElectroCasa",               "electronica",        9),
    ("Estación Norte",            "combustible",       83),
    ("Servicentro Sur",           "combustible",       52),
    ("Aerolínea Cielo Azul",      "viajes",             6),
    ("Hotel Vista Verde",         "viajes",             4),
    ("Agencia Viajes Horizonte",  "viajes",             3),
    ("Cine Estelar",              "entretenimiento",   22),
    ("Librería Páginas",          "entretenimiento",   11),
    ("Streaming PlayMax",         "entretenimiento",   38),
    ("Gimnasio Fuerza",           "servicios",         17),
    ("Peluquería Estilo",         "servicios",         13),
    ("Telecom Conecta",           "servicios",         45),
    ("Ferretería Martillo",       "hogar",             16),
    ("Muebles Confort",           "hogar",              7),
]

# se construye una lista donde cada comercio aparece tantas veces como su peso:
# así el muestreo uniforme produce una distribución desigual y realista
_urna = [n for n, _, w in COMERCIOS for _ in range(w)]
urna_comercios = F.array(*[F.lit(n) for n in _urna])
mapa_comercio = F.create_map(
    *[x for nombre, cat, _ in COMERCIOS for x in (F.lit(nombre), F.lit(cat))]
)

# prefijos de tarjeta por marca — no todas empiezan con 4539
PREFIJOS = ["4539", "4024", "4485", "5412", "5535", "5164", "3782", "6011"]
urna_prefijos = F.array(*[F.lit(p) for p in PREFIJOS])

tx = (
    spark.range(1, N_TX + 1)
    .withColumnRenamed("id", "n")
    .withColumn("transaccion_id",
                F.concat(F.lit("TX"), F.lpad(F.col("n").cast("string"), 10, "0")))

    # ── cliente: NO uniforme. Unos clientes transan mucho más que otros (ley de Pareto).
    # Elevar rand() a una potencia >1 concentra las transacciones en los primeros ids.
    .withColumn("_ci",
        (F.pow(F.rand(SEMILLA + 10), F.lit(1.45)) * N_CLIENTES).cast("int") + 1)
    # se reconstruye el id con la MISMA fórmula aritmética que usó la tabla de clientes
    .withColumn("_cn", F.col("_ci") * 7 + F.lit(3))
    .withColumn("cliente_id",
                F.concat(F.lit("CLI"), F.lpad(F.col("_cn").cast("string"), 7, "0")))

    # ── fecha: 18 meses, pero con MÁS volumen en los meses recientes (el banco crece)
    .withColumn("_dias", (F.pow(F.rand(SEMILLA + 11), F.lit(1.35)) * 548).cast("int"))
    .withColumn("_f", F.date_sub(F.current_date(), F.col("_dias")))
    .withColumn("_dow", F.dayofweek(F.col("_f")))   # 1=domingo … 7=sábado

    # ── hora: distribución realista con dos picos (almuerzo y tarde/noche),
    # no 24 horas equiprobables. La madrugada existe pero es marginal.
    .withColumn("_rh", F.rand(SEMILLA + 12))
    .withColumn(
        "_hora",
        F.when(F.col("_rh") < 0.021, (F.rand(SEMILLA + 41) * 4 + 1).cast("int"))    # 01-04
        .when(F.col("_rh") < 0.055, F.lit(5) + (F.rand(SEMILLA + 42) * 2).cast("int"))
        .when(F.col("_rh") < 0.185, F.lit(7) + (F.rand(SEMILLA + 43) * 4).cast("int"))
        .when(F.col("_rh") < 0.435, F.lit(11) + (F.rand(SEMILLA + 44) * 3).cast("int"))
        .when(F.col("_rh") < 0.660, F.lit(14) + (F.rand(SEMILLA + 45) * 4).cast("int"))
        .when(F.col("_rh") < 0.925, F.lit(18) + (F.rand(SEMILLA + 46) * 4).cast("int"))
        .otherwise(F.lit(22) + (F.rand(SEMILLA + 47) * 2).cast("int")),
    )
    .withColumn(
        "fecha",
        F.to_timestamp(
            F.concat(
                F.col("_f").cast("string"), F.lit(" "),
                F.lpad(F.col("_hora").cast("string"), 2, "0"), F.lit(":"),
                F.lpad((F.rand(SEMILLA + 13) * 60).cast("int").cast("string"), 2, "0"),
                F.lit(":"),
                F.lpad((F.rand(SEMILLA + 14) * 60).cast("int").cast("string"), 2, "0"),
            )
        ),
    )

    # ── comercio: muestreo sobre la urna ponderada
    .withColumn("_idx", (F.rand(SEMILLA + 15) * len(_urna)).cast("int"))
    .withColumn("comercio", F.element_at(urna_comercios, F.col("_idx") + 1))
    .withColumn("categoria_comercio", mapa_comercio[F.col("comercio")])

    # ── canal: depende de la CATEGORÍA. Nadie compra combustible por internet,
    # y el streaming no se paga en un cajero.
    .withColumn("_rc", F.rand(SEMILLA + 16))
    .withColumn(
        "canal",
        F.when(F.col("categoria_comercio").isin("combustible", "supermercado"),
               F.when(F.col("_rc") < 0.83, "presencial")
                .when(F.col("_rc") < 0.94, "movil").otherwise("atm"))
        .when(F.col("categoria_comercio").isin("entretenimiento", "viajes"),
              F.when(F.col("_rc") < 0.71, "online")
               .when(F.col("_rc") < 0.93, "movil").otherwise("presencial"))
        .when(F.col("categoria_comercio") == "servicios",
              F.when(F.col("_rc") < 0.58, "online")
               .when(F.col("_rc") < 0.88, "movil").otherwise("presencial"))
        .otherwise(
              F.when(F.col("_rc") < 0.52, "presencial")
               .when(F.col("_rc") < 0.77, "online")
               .when(F.col("_rc") < 0.95, "movil").otherwise("atm")),
    )

    # ── país: el exterior se concentra en viajes, no reparte parejo
    .withColumn("_rp", F.rand(SEMILLA + 17))
    .withColumn(
        "pais",
        F.when(F.col("categoria_comercio") == "viajes",
               F.when(F.col("_rp") < 0.62, "CO")
                .when(F.col("_rp") < 0.79, "US")
                .when(F.col("_rp") < 0.89, "MX")
                .when(F.col("_rp") < 0.96, "PA").otherwise("ES"))
        .otherwise(
               F.when(F.col("_rp") < 0.9814, "CO")
                .when(F.col("_rp") < 0.9906, "US")
                .when(F.col("_rp") < 0.9963, "MX")
                .otherwise("PA")),
    )
    .withColumn("moneda", F.when(F.col("pais") == "CO", "COP").otherwise("USD"))

    # ── dispositivo: los canales digitales casi siempre lo tienen; el presencial no
    .withColumn("_rd", F.rand(SEMILLA + 18))
    .withColumn(
        "dispositivo_id",
        F.when(
            ((F.col("canal").isin("online", "movil")) & (F.col("_rd") < 0.962))
            | ((F.col("canal") == "presencial") & (F.col("_rd") < 0.184))
            | ((F.col("canal") == "atm") & (F.col("_rd") < 0.071)),
            F.concat(F.lit("DEV-"),
                     F.lpad((F.rand(SEMILLA + 19) * 47000).cast("int").cast("string"), 6, "0")),
        ).otherwise(F.lit(None).cast("string")),
    )
)

# unir con clientes para poder usar segmento, antigüedad y score en las señales de fraude
_antes = tx.count()
tx = tx.join(F.broadcast(clientes), on="cliente_id", how="inner")
_despues = tx.count()

# Control: el join NO debe perder filas. Si las pierde, los cliente_id de las transacciones
# no coinciden con los de la tabla de clientes — y eso pasa si alguno de los dos lados usa
# rand() para construir el id en vez de una función determinística.
if _despues < _antes * 0.99:
    raise Exception(
        f"\n❌ El join con clientes perdió filas: {_antes:,} → {_despues:,} "
        f"({(1 - _despues/_antes):.1%} perdido)\n\n"
        f"   Los cliente_id de las transacciones no coinciden con los de la tabla de\n"
        f"   clientes. Revisa que ambos lados usen hash() y no rand() para construir el id.\n"
    )

print(f"✅ estructura base lista · {_despues:,} transacciones "
      f"(join sin pérdida de filas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plantar las señales de fraude
# MAGIC
# MAGIC Se calcula un **score de riesgo** a partir de las señales, y se marca como fraude a
# MAGIC las transacciones de mayor score. Así el fraude queda **correlacionado con las
# MAGIC features** — que es lo que permite entrenar un modelo útil en el Módulo 6.

# COMMAND ----------

# ── monto: depende del SEGMENTO **y** de la CATEGORÍA del comercio.
# Un café no cuesta lo mismo que un vuelo, y un cliente empresarial gasta más que uno
# retail en el mismo comercio. Sin las dos dimensiones, los montos se ven fabricados.
tx = (
    tx.withColumn("_base_seg",
        F.when(F.col("segmento") == "empresarial", 1.0)
        .when(F.col("segmento") == "premium", 0.47)
        .otherwise(0.19))
    .withColumn("_base_cat",
        F.when(F.col("categoria_comercio") == "viajes", 1_840_000)
        .when(F.col("categoria_comercio") == "electronica", 920_000)
        .when(F.col("categoria_comercio") == "hogar", 610_000)
        .when(F.col("categoria_comercio") == "ropa", 285_000)
        .when(F.col("categoria_comercio") == "supermercado", 148_000)
        .when(F.col("categoria_comercio") == "combustible", 132_000)
        .when(F.col("categoria_comercio") == "salud", 97_000)
        .when(F.col("categoria_comercio") == "restaurante", 54_000)
        .when(F.col("categoria_comercio") == "servicios", 88_000)
        .otherwise(41_000))   # entretenimiento
    # log-normal: la forma real del gasto — muchos montos bajos, cola larga a la derecha
    .withColumn("_mult", F.exp(F.randn(SEMILLA + 20) * 0.62))
    .withColumn("monto",
        F.round(F.col("_base_cat") * F.col("_base_seg") * F.col("_mult") * 2.9, 0))
    # se guarda la desviación relativa del cliente: es la señal de "monto inusual" del M6
    .withColumn("_desv", F.col("_mult"))
)

# score de riesgo: suma de señales. Cada término es una regla de negocio reconocible.
tx = tx.withColumn(
    "_riesgo",
    # monto desproporcionado respecto al segmento
    F.when(F.col("_desv") > 3.4, 3.0).when(F.col("_desv") > 2.1, 1.5).otherwise(0.0)
    # transacción en el exterior
    + F.when(F.col("pais") != "CO", 2.5).otherwise(0.0)
    # horario atípico (madrugada)
    + F.when(F.col("_hora").between(2, 5), 2.0).otherwise(0.0)
    # canal de riesgo
    + F.when(F.col("canal") == "online", 1.2)
    .when(F.col("canal") == "atm", 0.8).otherwise(0.0)
    # cliente nuevo con score bajo
    + F.when((F.col("antiguedad_meses") < 12) & (F.col("score_crediticio") < 500), 1.5)
    .otherwise(0.0)
    # sin dispositivo identificado
    + F.when(F.col("dispositivo_id").isNull(), 0.6).otherwise(0.0)
    # ruido: evita que el modelo llegue a AUC 1.0, que se vería falso
    + F.randn(SEMILLA + 21) * 1.4,
)

# ── número de tarjeta ──
# La tarjeta pertenece al CLIENTE, no a la transacción: se deriva de su id (con hash, no
# con rand) para que el mismo cliente use siempre la misma tarjeta. El prefijo varía por
# marca. Es la columna PII que se enmascara en el Módulo 4.
tx = (
    tx.withColumn("_pi", F.abs(F.hash(F.col("cliente_id"))) % len(PREFIJOS))
    .withColumn("_h", F.abs(F.hash(F.concat(F.col("cliente_id"), F.lit("t")))))
    .withColumn(
        "numero_tarjeta",
        F.concat(
            F.element_at(urna_prefijos, F.col("_pi") + 1), F.lit("-"),
            F.lpad((F.col("_h") % 10000).cast("string"), 4, "0"), F.lit("-"),
            F.lpad(((F.col("_h") / 7).cast("long") % 10000).cast("string"), 4, "0"),
            F.lit("-"),
            F.lpad(((F.col("_h") / 13).cast("long") % 10000).cast("string"), 4, "0"),
        ),
    )
)

# ── Materializar ANTES de calcular el umbral ──
#
# Esto es imprescindible, no una optimización. El umbral de fraude se calcula con un
# percentil sobre _riesgo, que depende de randn(). Si no se materializa primero, Spark
# recalcula los valores aleatorios al aplicar el filtro y el umbral queda calculado sobre
# datos distintos de los que se etiquetan: la tasa de fraude saldría cualquier cosa.
columnas = [
    "transaccion_id", "cliente_id", "numero_tarjeta", "monto", "moneda",
    "comercio", "categoria_comercio", "canal", "pais", "region", "fecha",
    "dispositivo_id",
]
# se conservan _riesgo y _dias como auxiliares: el umbral y el corte del lote los necesitan.
# No se escriben a los archivos finales.
(tx.select(*columnas,
           F.col("_riesgo").alias("_riesgo"),
           F.col("_dias").alias("_dias_orden"))
   .write.mode("overwrite").saveAsTable("_gen_tx"))

tx_mat = spark.table("_gen_tx")

# ahora sí: el umbral se calcula sobre valores fijos, calibrado para ~1,5% de fraude
UMBRAL = tx_mat.approxQuantile("_riesgo", [0.985], 0.001)[0]
transacciones = tx_mat.withColumn("es_fraude", F.col("_riesgo") > F.lit(UMBRAL)) \
                      .drop("_riesgo")

total = transacciones.count()
fraudes = transacciones.filter("es_fraude").count()
print(f"✅ {total:,} transacciones · {fraudes:,} fraudes ({fraudes / total:.2%})")
print(f"   (umbral de riesgo: {UMBRAL:.3f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificar que las señales quedaron bien plantadas
# MAGIC
# MAGIC Si estos números no muestran diferencias claras, el modelo del Módulo 6 no va a
# MAGIC aprender nada. Esta celda es el control de calidad del generador.

# COMMAND ----------

print("Tasa de fraude por canal:")
display(
    transacciones.groupBy("canal")
    .agg(F.count("*").alias("transacciones"),
         F.round(F.avg(F.col("es_fraude").cast("int")) * 100, 2).alias("tasa_fraude_pct"))
    .orderBy(F.desc("tasa_fraude_pct"))
)

print("Tasa de fraude: Colombia vs exterior")
display(
    transacciones.withColumn("es_exterior", F.col("pais") != "CO")
    .groupBy("es_exterior")
    .agg(F.count("*").alias("transacciones"),
         F.round(F.avg(F.col("es_fraude").cast("int")) * 100, 2).alias("tasa_fraude_pct"))
)

print("Monto promedio: normal vs fraude")
display(
    transacciones.groupBy("es_fraude")
    .agg(F.round(F.avg("monto"), 0).alias("monto_promedio"),
         F.count("*").alias("transacciones"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 · Agregar filas inválidas a propósito
# MAGIC
# MAGIC **~2% de las filas tendrán `monto <= 0`.**
# MAGIC
# MAGIC No es un descuido: en el **Módulo 3** vas a escribir una regla de calidad que
# MAGIC descarta estas filas, y vas a ver el conteo de descartes en el panel del pipeline.
# MAGIC Si los datos fueran perfectos, la regla no descartaría nada y el ejercicio no
# MAGIC demostraría nada.

# COMMAND ----------

# 2% de las filas quedan con monto inválido (0 o negativo), como pasa en la vida real
# cuando un sistema origen manda reversos mal formateados.
#
# Se usa hash(transaccion_id) en vez de rand(): así la decisión es DETERMINÍSTICA y no
# cambia entre acciones. Con rand() el conteo que se imprime abajo no coincidiría con las
# filas que finalmente se escriben a los archivos.
transacciones = (
    transacciones
    .withColumn("_hm", F.abs(F.hash(F.concat(F.col("transaccion_id"), F.lit("m")))) % 1000)
    .withColumn(
        "monto",
        F.when(F.col("_hm") < 13, F.lit(0.0))                      # ~1,3% en cero
        .when(F.col("_hm") < 20, F.round(-F.col("monto"), 0))       # ~0,7% negativos
        .otherwise(F.col("monto")),
    )
    .drop("_hm")
)

invalidas = transacciones.filter("monto <= 0").count()
print(f"✅ {invalidas:,} filas con monto inválido ({invalidas / total:.2%}) — "
      f"para la regla de calidad del Módulo 3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 · Escribir los archivos crudos
# MAGIC
# MAGIC Los datos se escriben como **archivos en un volumen**, no como tablas. Es
# MAGIC deliberado: así el **Módulo 2** empieza como empieza en la vida real — con archivos
# MAGIC que llegan de un sistema origen.
# MAGIC
# MAGIC Se reservan **las últimas transacciones** para un archivo aparte, que simula datos
# MAGIC que llegan después. Lo usarás para comprobar que Auto Loader **no reprocesa** lo que
# MAGIC ya leyó.

# COMMAND ----------

# Separar el lote "que llega después": las transacciones más recientes.
# Se corta por FECHA, no por id — así el lote nuevo es realmente "lo último que pasó",
# que es como llega un archivo incremental en la vida real.
corte = transacciones.approxQuantile("_dias_orden", [0.052], 0.002)[0]
transacciones = transacciones.withColumn(
    "_lote",
    F.when(F.col("_dias_orden") <= F.lit(corte), "nuevo").otherwise("inicial"),
)

lote_inicial = transacciones.filter("_lote = 'inicial'").drop("_lote", "_dias_orden")
lote_nuevo = transacciones.filter("_lote = 'nuevo'").drop("_lote", "_dias_orden")

# --- transacciones iniciales: 12 archivos JSON ---
(lote_inicial.repartition(12)
    .write.mode("overwrite").format("json")
    .save(f"{RAW}/transacciones"))

# --- el archivo que "llega después": 1 archivo JSON ---
(lote_nuevo.coalesce(1)
    .write.mode("overwrite").format("json")
    .save(f"{RAW}/transacciones_nuevas"))

# --- clientes: CSV con encabezado ---
(clientes.coalesce(1)
    .write.mode("overwrite").option("header", "true").format("csv")
    .save(f"{RAW}/clientes"))

# Los conteos se calculan ANTES de borrar las tablas auxiliares: lote_inicial y lote_nuevo
# son DataFrames perezosos que leen de _gen_tx. Si se borra primero, el count() falla.
n_inicial = lote_inicial.count()
n_nuevo = lote_nuevo.count()

# ahora sí: las tablas auxiliares ya cumplieron su función
for aux in ("_gen_tx", "_gen_clientes"):
    spark.sql(f"DROP TABLE IF EXISTS {aux}")

print(f"✅ archivos escritos en {RAW}")
print(f"   transacciones        : {n_inicial:,} filas")
print(f"   transacciones_nuevas : {n_nuevo:,} filas")
print(f"   clientes             : {n_clientes_real:,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 · Verificar lo que quedó

# COMMAND ----------

print("📁 Archivos de transacciones:")
archivos = [f for f in dbutils.fs.ls(f"{RAW}/transacciones") if f.name.endswith(".json")]
for f in archivos[:4]:
    print(f"   {f.name}  ({f.size / 1024 / 1024:.1f} MB)")
print(f"   … {len(archivos)} archivos en total")

print()
print("📄 Una muestra del contenido crudo:")
display(spark.read.json(f"{RAW}/transacciones").limit(5))

print("📄 Los clientes:")
display(spark.read.option("header", "true").csv(f"{RAW}/clientes").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Control final: ¿los archivos quedaron bien?
# MAGIC
# MAGIC Esta celda lee **los archivos ya escritos** — no los DataFrames en memoria — y
# MAGIC comprueba que el resultado sea el esperado. Si algo salió mal, lo dice acá y no tres
# MAGIC módulos más adelante.

# COMMAND ----------

_tx = spark.read.json(f"{RAW}/transacciones")
_cl = spark.read.option("header", "true").csv(f"{RAW}/clientes")

_r = _tx.agg(
    F.count("*").alias("filas"),
    F.countDistinct("cliente_id").alias("clientes_ref"),
    F.sum(F.when(F.col("es_fraude"), 1).otherwise(0)).alias("fraudes"),
    F.sum(F.when(F.col("monto") <= 0, 1).otherwise(0)).alias("invalidas"),
    F.countDistinct("comercio").alias("comercios"),
    F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("sin_cliente"),
).collect()[0]

n_cl = _cl.count()
pct_fraude = _r["fraudes"] / _r["filas"] * 100
pct_inval = _r["invalidas"] / _r["filas"] * 100

checks = [
    (_r["filas"] > 300_000,
     f"Transacciones: {_r['filas']:,} (se esperan ~377.000)"),
    (_r["sin_cliente"] == 0,
     f"Sin cliente_id nulo: {_r['sin_cliente']} nulos"),
    (1.0 <= pct_fraude <= 2.5,
     f"Tasa de fraude: {pct_fraude:.2f}% (se espera ~1,5%)"),
    (1.0 <= pct_inval <= 3.0,
     f"Filas inválidas: {pct_inval:.2f}% (se espera ~2%) — para el M3"),
    (_r["comercios"] >= 20,
     f"Comercios distintos: {_r['comercios']}"),
    (n_cl > 15_000,
     f"Clientes: {n_cl:,} (se esperan ~18.473)"),
]

print("=" * 66)
print("  CONTROL FINAL DE LOS ARCHIVOS GENERADOS")
print("=" * 66)
for ok, msg in checks:
    print(f"  {'✅' if ok else '❌'}  {msg}")
print("=" * 66)

if all(ok for ok, _ in checks):
    print("\n  🎉 Los datos están listos. Pasa al notebook 01_lakehouse_101.\n")
else:
    print("""
  ⚠️  Algo no cuadra. Lo más probable:

  · Pocas transacciones → el join con clientes perdió filas. Revisa que
    ambos lados construyan cliente_id con la misma fórmula aritmética.
  · Tasa de fraude fuera de rango → el umbral se calculó antes de
    materializar _gen_tx.

  Vuelve a ejecutar el notebook completo desde el principio.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Listo
# MAGIC
# MAGIC ```
# MAGIC Tu catálogo  : se muestra arriba
# MAGIC Tu schema    : fin_<tu_usuario>
# MAGIC Tus archivos : /Volumes/<catálogo>/<schema>/raw/
# MAGIC ```
# MAGIC
# MAGIC ### Qué sigue
# MAGIC
# MAGIC | Módulo | Qué harás con estos datos |
# MAGIC |---|---|
# MAGIC | **1** | Tu primera tabla Delta e historial de versiones ← *empieza ahora* |
# MAGIC | 2 | Ingesta con Auto Loader → capa **bronze** |
# MAGIC | 3 | Pipeline con calidad de datos → **silver** y **gold** |
# MAGIC | 4 | Enmascarar `numero_tarjeta` y filtrar por `region` |
# MAGIC | 5 | Orquestar todo como un job programado |
# MAGIC | 6 | Entrenar el modelo de detección de fraude |
# MAGIC | 7 | Dashboard, Genie y una app de scoring |
# MAGIC
# MAGIC **👉 Abre el notebook `01_lakehouse_101` y empieza.**
# MAGIC
# MAGIC > Si algo falló acá, avísale a un TA antes de seguir: todos los módulos usan estos
# MAGIC > datos.
