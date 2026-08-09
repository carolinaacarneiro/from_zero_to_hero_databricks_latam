# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks: From Zero to Hero
# MAGIC # 🛒 Generador de datos — Pronóstico de ventas (Retail)
# MAGIC
# MAGIC Este notebook **crea los datos sintéticos** que vas a usar durante todo el taller de
# MAGIC retail. Córrelo una sola vez, al inicio.
# MAGIC
# MAGIC ## El caso de negocio
# MAGIC
# MAGIC Trabajas en el área de datos de una **cadena de retail** con presencia en varios países
# MAGIC de América Latina. El equipo comercial necesita **pronosticar las ventas** — cuánto se
# MAGIC venderá por producto, por país, en dólares y en unidades — para planear el inventario y
# MAGIC no quedarse sin stock ni comprar de más.
# MAGIC
# MAGIC Durante el día vas a construir el camino completo: desde los archivos crudos que llegan
# MAGIC del punto de venta, hasta un **pronóstico con AI Functions**, un **dashboard**, un
# MAGIC **Genie** que responde preguntas y una **app** que el equipo de compras puede usar.
# MAGIC
# MAGIC ## Qué crea este notebook
# MAGIC
# MAGIC | Archivo | Qué es | Formato |
# MAGIC |---|---|---|
# MAGIC | `raw/ventas/` | 12 archivos · ~600.000 líneas de venta de 24 meses | JSON |
# MAGIC | `raw/productos/` | ~300 productos con categoría, marca, precio y costo | CSV |
# MAGIC | `raw/inventario/` | stock actual por producto y país | CSV |
# MAGIC | `raw/ventas_nuevas/` | 1 archivo · ventas que "llegan después" | JSON |
# MAGIC
# MAGIC > 🔒 **Todos los datos son 100% sintéticos.** No hay información real de ninguna empresa,
# MAGIC > marca ni persona. Los nombres de productos y marcas son inventados y no corresponden a
# MAGIC > compañías reales.
# MAGIC >
# MAGIC > ⚠️ **Material de aprendizaje — no es production-ready.** Este generador y los notebooks
# MAGIC > del taller enseñan conceptos; no están pensados para copiarse a producción tal cual.
# MAGIC
# MAGIC > 💡 **El caso es retail, pero el método es de cualquier industria.** Pronosticar una
# MAGIC > serie de tiempo —ventas, demanda de energía, tickets de soporte, tráfico— es el mismo
# MAGIC > patrón. Cambia el dato, no la técnica.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 · Elige dónde trabajar
# MAGIC
# MAGIC Son **dos celdas**: la **A** crea los campos, la **B** los lee. Van separadas porque los
# MAGIC campos no existen hasta que corres la celda A — antes de eso no habría nada dónde
# MAGIC escribir.
# MAGIC
# MAGIC | Campo | Qué poner |
# MAGIC |---|---|
# MAGIC | **catalogo** | El catálogo donde quieres trabajar. Si tu organización te asignó uno, escríbelo |
# MAGIC | **schema** | Tu schema personal. Se propone uno derivado de tu usuario |
# MAGIC
# MAGIC **Si el catálogo o el schema no existen, el notebook los crea.**
# MAGIC
# MAGIC > 💡 En muchos workspaces corporativos **no se puede crear catálogos**. Si eso pasa, el
# MAGIC > notebook te lo dice con claridad y te muestra los catálogos donde **sí** puedes
# MAGIC > trabajar, para que elijas uno y vuelvas a ejecutar.

# COMMAND ----------

# ═══ CELDA A · crear los campos ═══════════════════════════════════════════
# Ejecuta esta celda. Después mira los dos campos que aparecen ARRIBA del notebook,
# ajústalos si hace falta, y sigue con la celda B.

import re

# valor propuesto para el schema, derivado del usuario. Prefijo 'retail_' para no chocar con
# el caso de fraude (que usa 'fin_') si corres los dos en el mismo workspace.
_usuario = spark.sql("SELECT current_user()").collect()[0][0]
_schema_sugerido = "retail_" + re.sub(r"[^a-z0-9_]", "_", _usuario.split("@")[0].lower())

# se descartan los catálogos del sistema: no son para trabajar en ellos
_ocultos = ("system", "samples", "__databricks_internal", "hive_metastore")
_disponibles = [r[0] for r in spark.sql("SHOW CATALOGS").collect()
                if r[0].lower() not in _ocultos]

dbutils.widgets.text("catalogo", "z2h", "1 · Catálogo")
dbutils.widgets.text("schema", _schema_sugerido, "2 · Schema (tu espacio personal)")

print("👆 Ya aparecieron los dos campos ARRIBA de este notebook.\n")
print(f"   Usuario          : {_usuario}")
print(f"   Schema propuesto : {_schema_sugerido}")
print()
print("📦 Catálogos que ya existen en este workspace:")
for c in _disponibles:
    print(f"     · {c}")
print()
print("Puedes usar uno de esos, o dejar 'z2h' para que el notebook intente crearlo.")
print("Cuando estés listo, sigue con la celda B.")

# COMMAND ----------

# ═══ CELDA B · leer los campos ════════════════════════════════════════════
# Si cambias algo en los campos de arriba, vuelve a ejecutar desde esta celda.

catalogo = dbutils.widgets.get("catalogo").strip()
schema = dbutils.widgets.get("schema").strip()

print(f"📦 Catálogo : {catalogo}")
print(f"📁 Schema   : {schema}")

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
# MAGIC ## Paso 2 · Generar el catálogo de productos
# MAGIC
# MAGIC ~300 productos con su categoría, subcategoría, marca, precio de lista y costo.
# MAGIC
# MAGIC Fíjate en los detalles: las cantidades **no son redondas**, los ids **tienen huecos**, y
# MAGIC los precios **no reparten parejo** entre categorías. Un producto de electrónica no
# MAGIC cuesta lo mismo que uno de alimentos, y eso tiene que verse en los datos.
# MAGIC
# MAGIC > 💡 La columna **`costo_unitario`** es dato **confidencial** (el margen del negocio). En
# MAGIC > el **Módulo 4** vas a enmascararla para que solo finanzas la vea.

# COMMAND ----------

from pyspark.sql import functions as F

SEMILLA = 42  # fija, para que todos generen exactamente los mismos datos

# Categorías con su rango de precio típico (USD) y peso de volumen.
# El peso NO es uniforme: alimentos se vende muchísimo más que electrónica.
#   (categoria, subcategorias, precio_base_usd, peso_volumen)
CATEGORIAS = [
    ("alimentos",   ["abarrotes", "bebidas", "snacks", "lacteos"],      8,   150),
    ("hogar",       ["cocina", "limpieza", "decoracion", "muebles"],    34,   70),
    ("belleza",     ["cuidado_piel", "maquillaje", "fragancias"],       22,   58),
    ("moda",        ["ropa", "calzado", "accesorios"],                  41,   64),
    ("electronica", ["celulares", "computo", "audio", "tv"],           320,   26),
    ("deportes",    ["fitness", "outdoor", "ciclismo"],                 57,   31),
    ("juguetes",    ["educativos", "aire_libre", "electronicos"],       26,   24),
    ("oficina",     ["papeleria", "tecnologia", "mobiliario"],          19,   19),
]

# marcas inventadas — no corresponden a empresas reales
MARCAS = ["Andes", "Marea", "Volcán", "Selva", "Cumbre", "Delta", "Norte", "Coral",
          "Vértice", "Origen", "Pampa", "Sol"]

_filas_prod = []
_pid = 0
for cat, subs, precio_base, peso in CATEGORIAS:
    # nº de productos por categoría, proporcional (con algo de ruido) al peso — no redondo
    n_en_cat = int(peso * 0.62) + (7 if cat == "alimentos" else 3)
    for _ in range(n_en_cat):
        _pid += 1
        _filas_prod.append((_pid, cat, subs, precio_base))

N_PRODUCTOS = len(_filas_prod)

# se arma un DataFrame base con producto_id ARITMÉTICO (n*7+3): mismos motivos que en el caso
# de fraude — el join tiene que reconstruir el id sin hash (HASH difiere BIGINT vs INT) y sin
# rand (se reevalúa por acción).
_urna_sub = {cat: F.array(*[F.lit(s) for s in subs]) for cat, subs, _, _ in CATEGORIAS}
_marcas_arr = F.array(*[F.lit(m) for m in MARCAS])

filas_prod = spark.createDataFrame(
    [(pid, cat, precio_base) for pid, cat, _subs, precio_base in _filas_prod],
    "n INT, categoria STRING, _precio_base DOUBLE",
)

productos = (
    filas_prod
    .withColumn("_pn", F.col("n") * 7 + F.lit(3))
    .withColumn("producto_id",
                F.concat(F.lit("PRD"), F.lpad(F.col("_pn").cast("string"), 5, "0")))
    # subcategoría: se elige dentro de las de su categoría
    .withColumn("_si", (F.rand(SEMILLA + 1) * 4).cast("int"))
    .withColumn("subcategoria",
        F.when(F.col("categoria") == "alimentos",
               F.element_at(_urna_sub["alimentos"], (F.col("_si") % 4) + 1))
        .when(F.col("categoria") == "hogar",
              F.element_at(_urna_sub["hogar"], (F.col("_si") % 4) + 1))
        .when(F.col("categoria") == "belleza",
              F.element_at(_urna_sub["belleza"], (F.col("_si") % 3) + 1))
        .when(F.col("categoria") == "moda",
              F.element_at(_urna_sub["moda"], (F.col("_si") % 3) + 1))
        .when(F.col("categoria") == "electronica",
              F.element_at(_urna_sub["electronica"], (F.col("_si") % 4) + 1))
        .when(F.col("categoria") == "deportes",
              F.element_at(_urna_sub["deportes"], (F.col("_si") % 3) + 1))
        .when(F.col("categoria") == "juguetes",
              F.element_at(_urna_sub["juguetes"], (F.col("_si") % 3) + 1))
        .otherwise(F.element_at(_urna_sub["oficina"], (F.col("_si") % 3) + 1)))
    # marca
    .withColumn("_mi", (F.rand(SEMILLA + 2) * len(MARCAS)).cast("int"))
    .withColumn("marca", F.element_at(_marcas_arr, F.col("_mi") + 1))
    # nombre del producto: marca + subcategoría + un número de modelo
    .withColumn("_modelo", (F.rand(SEMILLA + 3) * 900 + 100).cast("int"))
    .withColumn("nombre_producto",
                F.concat_ws(" ", F.col("marca"),
                            F.initcap(F.regexp_replace(F.col("subcategoria"), "_", " ")),
                            F.col("_modelo").cast("string")))
    # precio de lista: base de la categoría, con dispersión log-normal (cola a la derecha)
    .withColumn("precio_lista",
        F.round(F.col("_precio_base") * F.exp(F.randn(SEMILLA + 4) * 0.45) * 1.0, 2))
    # costo: entre 45% y 72% del precio de lista (margen variable por producto)
    .withColumn("_margen", F.rand(SEMILLA + 5) * 0.27 + 0.45)
    .withColumn("costo_unitario", F.round(F.col("precio_lista") * F.col("_margen"), 2))
    .select("producto_id", "nombre_producto", "categoria", "subcategoria", "marca",
            "precio_lista", "costo_unitario")
)

# ── Materializar en Delta ──
# No se usa .cache() (no soportado en serverless). Y hay una razón más fuerte: rand()/randn()
# se REEVALÚAN en cada acción; sin materializar, la tabla de productos no coincidiría con las
# ventas que la referencian. Escribir a Delta congela los valores una sola vez.
(productos.write.mode("overwrite").saveAsTable("_gen_productos"))
productos = spark.table("_gen_productos")

print(f"✅ {productos.count():,} productos generados y materializados")
display(productos.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Se ven realistas?
# MAGIC
# MAGIC Los precios promedio deben ser **muy distintos** entre categorías (electrónica cara,
# MAGIC alimentos barato) y el número de productos por categoría **desparejo**.

# COMMAND ----------

display(
    productos.groupBy("categoria")
    .agg(F.count("*").alias("productos"),
         F.round(F.avg("precio_lista"), 2).alias("precio_promedio"),
         F.round(F.avg("costo_unitario") / F.avg("precio_lista"), 3).alias("costo_sobre_precio"))
    .orderBy(F.desc("precio_promedio"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 · Generar las ventas (24 meses)
# MAGIC
# MAGIC ~600.000 líneas de venta a lo largo de **los últimos 24 meses hasta hoy**.
# MAGIC
# MAGIC ### Las ventas no son planas — tienen forma
# MAGIC
# MAGIC Si las ventas fueran uniformes, ningún pronóstico tendría sentido y el **Módulo 6** no
# MAGIC serviría. Estos son los **patrones plantados** a propósito, los mismos que tiene una
# MAGIC serie real de retail:
# MAGIC
# MAGIC | Patrón | Cómo se ve en los datos |
# MAGIC |---|---|
# MAGIC | **Tendencia** | El negocio crece: hay más ventas en los meses recientes |
# MAGIC | **Estacionalidad anual** | Pico fuerte en noviembre–diciembre, valle en enero |
# MAGIC | **Estacionalidad semanal** | Fines de semana venden más que días de semana |
# MAGIC | **Diferencias por país** | Brasil y México mueven más volumen que Chile o Perú |
# MAGIC | **Diferencias por categoría** | Alimentos: mucho volumen, ticket bajo. Electrónica: al revés |
# MAGIC
# MAGIC El objetivo es que `ai_forecast` (Módulo 6) tenga **una serie con tendencia y
# MAGIC estacionalidad claras** que pueda aprender y proyectar.

# COMMAND ----------

N_VENTAS = 613_847   # cantidad deliberadamente no redonda

# países LATAM con su peso de volumen — NO uniforme
#   (pais, peso)
PAISES = [("BR", 145), ("MX", 118), ("CO", 84), ("CL", 47), ("PE", 39), ("AR", 33)]
_urna_pais = [p for p, w in PAISES for _ in range(w)]
urna_pais = F.array(*[F.lit(p) for p in _urna_pais])

ventas = (
    spark.range(1, N_VENTAS + 1)
    .withColumnRenamed("id", "n")
    .withColumn("venta_id",
                F.concat(F.lit("VTA"), F.lpad(F.col("n").cast("string"), 10, "0")))

    # ── producto: NO uniforme. Unos productos se venden mucho más que otros (Pareto).
    # Elevar rand() a una potencia >1 concentra las ventas en los primeros ids.
    .withColumn("_pi",
        (F.pow(F.rand(SEMILLA + 10), F.lit(1.5)) * N_PRODUCTOS).cast("int") + 1)
    .withColumn("_pn", F.col("_pi") * 7 + F.lit(3))   # misma fórmula aritmética que productos
    .withColumn("producto_id",
                F.concat(F.lit("PRD"), F.lpad(F.col("_pn").cast("string"), 5, "0")))

    # ── fecha: 24 meses (730 días), con MÁS volumen en los meses recientes (tendencia)
    .withColumn("_dias", (F.pow(F.rand(SEMILLA + 11), F.lit(1.28)) * 730).cast("int"))
    .withColumn("_f", F.date_sub(F.current_date(), F.col("_dias")))
    .withColumn("_mes", F.month("_f"))
    .withColumn("_dow", F.dayofweek("_f"))   # 1=domingo … 7=sábado

    # ── hora de la venta: retail concentra en horario comercial, con pico al mediodía y tarde
    .withColumn("_rh", F.rand(SEMILLA + 12))
    .withColumn(
        "_hora",
        F.when(F.col("_rh") < 0.06, F.lit(8) + (F.rand(SEMILLA + 41) * 2).cast("int"))
        .when(F.col("_rh") < 0.34, F.lit(10) + (F.rand(SEMILLA + 42) * 3).cast("int"))
        .when(F.col("_rh") < 0.62, F.lit(13) + (F.rand(SEMILLA + 43) * 3).cast("int"))
        .when(F.col("_rh") < 0.90, F.lit(16) + (F.rand(SEMILLA + 44) * 4).cast("int"))
        .otherwise(F.lit(20) + (F.rand(SEMILLA + 45) * 2).cast("int")),
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

    # ── país: muestreo sobre la urna ponderada (Brasil y México pesan más)
    .withColumn("_idx", (F.rand(SEMILLA + 15) * len(_urna_pais)).cast("int"))
    .withColumn("pais", F.element_at(urna_pais, F.col("_idx") + 1))

    # ── canal: tienda física domina, online creciendo, marketplace menor
    .withColumn("_rc", F.rand(SEMILLA + 16))
    .withColumn(
        "canal",
        F.when(F.col("_rc") < 0.548, "tienda")
        .when(F.col("_rc") < 0.842, "online")
        .otherwise("marketplace"),
    )
)

# unir con productos para tener precio_lista, costo y categoría — base de monto y estacionalidad
_antes = ventas.count()
ventas = ventas.join(F.broadcast(productos), on="producto_id", how="inner")
_despues = ventas.count()

# Control: el join NO debe perder filas. Si las pierde, los producto_id de las ventas no
# coinciden con los de la tabla de productos — pasa si algún lado usa rand()/hash() para el id.
if _despues < _antes * 0.99:
    raise Exception(
        f"\n❌ El join con productos perdió filas: {_antes:,} → {_despues:,} "
        f"({(1 - _despues/_antes):.1%} perdido)\n\n"
        f"   Los producto_id de las ventas no coinciden con los de la tabla de productos.\n"
        f"   Revisa que ambos lados construyan el id con la misma fórmula aritmética (n*7+3).\n"
    )

print(f"✅ estructura base lista · {_despues:,} líneas de venta (join sin pérdida)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plantar la estacionalidad y calcular el monto
# MAGIC
# MAGIC La **cantidad** y el **monto** de cada línea se modulan por temporada: fines de semana y
# MAGIC noviembre–diciembre suben, enero baja. Así la suma diaria muestra los patrones que el
# MAGIC pronóstico va a aprender.

# COMMAND ----------

ventas = (
    ventas
    # factor estacional por MES: pico en nov–dic, valle en enero, repunte a mitad de año
    .withColumn(
        "_f_mes",
        F.when(F.col("_mes") == 12, 1.95)
        .when(F.col("_mes") == 11, 1.48)   # temporada de ofertas
        .when(F.col("_mes") == 1, 0.72)    # resaca post-fiestas
        .when(F.col("_mes") == 7, 1.18)    # mitad de año
        .when(F.col("_mes").isin(5, 6), 1.08)
        .otherwise(0.96),
    )
    # factor por DÍA DE LA SEMANA: sábado y domingo más altos
    .withColumn(
        "_f_dow",
        F.when(F.col("_dow") == 7, 1.42)   # sábado
        .when(F.col("_dow") == 1, 1.31)    # domingo
        .when(F.col("_dow") == 6, 1.16)    # viernes
        .otherwise(0.93),
    )
    # inflación suave a lo largo de los 24 meses: los precios de hace 2 años eran ~9% menores
    .withColumn("_f_inflacion", F.lit(1.0) + (F.lit(730) - F.col("_dias")) / F.lit(730) * 0.09)

    # ── cantidad de unidades: base por categoría (alimentos se compra de a muchas,
    # electrónica de a una), modulada por estacionalidad. Log-normal, mínimo 1.
    .withColumn(
        "_cant_base",
        F.when(F.col("categoria") == "alimentos", 4.2)
        .when(F.col("categoria") == "belleza", 2.1)
        .when(F.col("categoria") == "hogar", 1.6)
        .when(F.col("categoria") == "oficina", 2.4)
        .when(F.col("categoria") == "juguetes", 1.7)
        .when(F.col("categoria") == "electronica", 1.08)
        .otherwise(1.4),
    )
    .withColumn(
        "cantidad",
        F.greatest(
            F.lit(1),
            F.round(F.col("_cant_base") * F.col("_f_dow") * F.col("_f_mes")
                    * F.exp(F.randn(SEMILLA + 20) * 0.5)).cast("int"),
        ),
    )

    # ── descuento: la mayoría sin descuento; algunos con 10–40% (más frecuente en temporada)
    .withColumn("_rd", F.rand(SEMILLA + 21))
    .withColumn(
        "descuento_pct",
        F.when((F.col("_f_mes") > 1.4) & (F.col("_rd") < 0.42),
               F.round(F.rand(SEMILLA + 22) * 0.30 + 0.10, 2))   # temporada: más ofertas
        .when(F.col("_rd") < 0.18, F.round(F.rand(SEMILLA + 23) * 0.25 + 0.05, 2))
        .otherwise(F.lit(0.0)),
    )

    # ── precio_unitario: el de lista, con inflación, menos el descuento, con ruido pequeño
    .withColumn(
        "precio_unitario",
        F.round(F.col("precio_lista") * F.col("_f_inflacion")
                * (F.lit(1.0) - F.col("descuento_pct"))
                * F.exp(F.randn(SEMILLA + 24) * 0.06), 2),
    )
    # ── monto de la línea: unidades × precio unitario
    .withColumn("monto", F.round(F.col("cantidad") * F.col("precio_unitario"), 2))
)

# ── Materializar ANTES de forzar filas inválidas y de cortar el lote nuevo ──
# Imprescindible, no una optimización: cantidad/monto dependen de randn(). Sin materializar,
# Spark recalcula los valores aleatorios en cada acción y los conteos de control no
# coincidirían con lo que se escribe a los archivos.
columnas = [
    "venta_id", "fecha", "producto_id", "pais", "canal",
    "cantidad", "precio_unitario", "descuento_pct", "monto",
]
(ventas.select(*columnas, F.col("_dias").alias("_dias_orden"))
   .write.mode("overwrite").saveAsTable("_gen_ventas"))

ventas_mat = spark.table("_gen_ventas")
total = ventas_mat.count()
monto_total = ventas_mat.agg(F.round(F.sum("monto"), 0)).collect()[0][0]
print(f"✅ {total:,} líneas de venta · monto total ${monto_total:,.0f} USD")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificar que los patrones quedaron bien plantados
# MAGIC
# MAGIC Si estas curvas no muestran tendencia y estacionalidad, el pronóstico del Módulo 6 no
# MAGIC tendría nada que aprender. Esta es la prueba de calidad del generador.

# COMMAND ----------

print("Ventas por mes calendario (debe verse pico en nov–dic, valle en enero):")
display(
    ventas_mat.withColumn("mes", F.date_format("fecha", "yyyy-MM"))
    .groupBy("mes")
    .agg(F.round(F.sum("monto"), 0).alias("monto"),
         F.sum("cantidad").alias("unidades"),
         F.count("*").alias("lineas"))
    .orderBy("mes")
)

print("Ventas por país (Brasil y México deben liderar):")
display(
    ventas_mat.groupBy("pais")
    .agg(F.round(F.sum("monto"), 0).alias("monto"),
         F.count("*").alias("lineas"))
    .orderBy(F.desc("monto"))
)

print("Ventas por día de la semana (fin de semana más alto):")
display(
    ventas_mat.withColumn("dow", F.date_format("fecha", "E"))
    .groupBy("dow")
    .agg(F.round(F.avg("monto"), 2).alias("ticket_promedio"),
         F.count("*").alias("lineas"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 · Agregar líneas inválidas a propósito
# MAGIC
# MAGIC **~2% de las líneas tendrán `cantidad <= 0`** (devoluciones mal registradas, errores del
# MAGIC punto de venta).
# MAGIC
# MAGIC No es un descuido: en el **Módulo 3** vas a escribir una regla de calidad que descarta
# MAGIC estas filas, y vas a ver el conteo de descartes en el panel del pipeline. Si los datos
# MAGIC fueran perfectos, la regla no descartaría nada y el ejercicio no demostraría nada.

# COMMAND ----------

# 2% de las filas quedan con cantidad inválida. Se usa hash(venta_id) en vez de rand(): así la
# decisión es DETERMINÍSTICA y el conteo impreso coincide con lo que se escribe a los archivos.
ventas_final = (
    ventas_mat
    .withColumn("_hm", F.abs(F.hash(F.concat(F.col("venta_id"), F.lit("q")))) % 1000)
    .withColumn(
        "cantidad",
        F.when(F.col("_hm") < 12, F.lit(0))                 # ~1,2% en cero
        .when(F.col("_hm") < 20, -F.col("cantidad"))         # ~0,8% negativas (devoluciones)
        .otherwise(F.col("cantidad")),
    )
    # el monto sigue a la cantidad: si es inválida, el monto también
    .withColumn("monto",
                F.when(F.col("_hm") < 20, F.round(F.col("cantidad") * F.col("precio_unitario"), 2))
                .otherwise(F.col("monto")))
    .drop("_hm")
)

invalidas = ventas_final.filter("cantidad <= 0").count()
print(f"✅ {invalidas:,} líneas con cantidad inválida ({invalidas / total:.2%}) — "
      f"para la regla de calidad del Módulo 3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 · Generar el inventario
# MAGIC
# MAGIC Stock actual por producto y país. No es una serie de tiempo: es una **foto** del
# MAGIC inventario hoy. En el **dashboard** y la **app** del Módulo 7 lo vas a cruzar con la
# MAGIC demanda para saber **qué productos hay que reabastecer**.
# MAGIC
# MAGIC No todos los productos se venden en todos los países — el inventario refleja eso.

# COMMAND ----------

_urna_pais_inv = F.array(*[F.lit(p) for p, _ in PAISES])

# cruce producto × país, pero solo ~55% de las combinaciones (no todo se vende en todos lados)
inventario = (
    productos.select("producto_id", "categoria", "precio_lista")
    .crossJoin(spark.createDataFrame([(p,) for p, _ in PAISES], "pais STRING"))
    .withColumn("_hc", F.abs(F.hash(F.concat("producto_id", "pais"))) % 100)
    .filter(F.col("_hc") < 55)
    # stock mínimo: depende de la categoría (alimentos rota rápido → mínimos altos)
    .withColumn(
        "stock_minimo",
        F.when(F.col("categoria") == "alimentos", (F.rand(SEMILLA + 30) * 120 + 40).cast("int"))
        .when(F.col("categoria") == "electronica", (F.rand(SEMILLA + 30) * 15 + 5).cast("int"))
        .otherwise((F.rand(SEMILLA + 30) * 50 + 15).cast("int")),
    )
    # stock actual: alrededor del mínimo, pero algunos por debajo (para que haya que reabastecer)
    .withColumn(
        "stock_actual",
        F.greatest(F.lit(0),
                   F.round(F.col("stock_minimo") * (F.rand(SEMILLA + 31) * 2.4 + 0.3)).cast("int")),
    )
    # lead time de reposición: días que tarda en llegar el pedido al proveedor
    .withColumn("lead_time_dias", (F.rand(SEMILLA + 32) * 25 + 3).cast("int"))
    .select("producto_id", "pais", "stock_actual", "stock_minimo", "lead_time_dias")
)

(inventario.write.mode("overwrite").saveAsTable("_gen_inventario"))
inventario = spark.table("_gen_inventario")

n_inv = inventario.count()
bajo_minimo = inventario.filter("stock_actual < stock_minimo").count()
print(f"✅ {n_inv:,} registros de inventario · {bajo_minimo:,} por debajo del mínimo "
      f"({bajo_minimo / n_inv:.1%}) — hay productos que reabastecer")
display(inventario.limit(8))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 · Escribir los archivos crudos
# MAGIC
# MAGIC Los datos se escriben como **archivos en un volumen**, no como tablas. Es deliberado:
# MAGIC así el **Módulo 2** empieza como empieza en la vida real — con archivos que llegan de un
# MAGIC sistema origen (el punto de venta).
# MAGIC
# MAGIC Se reservan las **ventas más recientes** para un archivo aparte, que simula datos que
# MAGIC llegan después. Lo usarás para comprobar que Auto Loader **no reprocesa** lo ya leído.

# COMMAND ----------

# Separar el lote "que llega después": las ventas más recientes (por fecha, no por id).
corte = ventas_final.approxQuantile("_dias_orden", [0.045], 0.002)[0]
ventas_final = ventas_final.withColumn(
    "_lote",
    F.when(F.col("_dias_orden") <= F.lit(corte), "nuevo").otherwise("inicial"),
)

lote_inicial = ventas_final.filter("_lote = 'inicial'").drop("_lote", "_dias_orden")
lote_nuevo = ventas_final.filter("_lote = 'nuevo'").drop("_lote", "_dias_orden")

# --- ventas iniciales: 12 archivos JSON ---
(lote_inicial.repartition(12)
    .write.mode("overwrite").format("json")
    .save(f"{RAW}/ventas"))

# --- el archivo que "llega después": 1 archivo JSON ---
(lote_nuevo.coalesce(1)
    .write.mode("overwrite").format("json")
    .save(f"{RAW}/ventas_nuevas"))

# --- productos e inventario: CSV con encabezado ---
(productos.coalesce(1)
    .write.mode("overwrite").option("header", "true").format("csv")
    .save(f"{RAW}/productos"))
(inventario.coalesce(1)
    .write.mode("overwrite").option("header", "true").format("csv")
    .save(f"{RAW}/inventario"))

# Los conteos se calculan ANTES de borrar las tablas auxiliares (los DataFrames son perezosos).
n_inicial = lote_inicial.count()
n_nuevo = lote_nuevo.count()

for aux in ("_gen_ventas", "_gen_productos", "_gen_inventario"):
    spark.sql(f"DROP TABLE IF EXISTS {aux}")

print(f"✅ archivos escritos en {RAW}")
print(f"   ventas        : {n_inicial:,} líneas (12 archivos JSON)")
print(f"   ventas_nuevas : {n_nuevo:,} líneas (1 archivo JSON)")
print(f"   productos     : {N_PRODUCTOS:,} filas (CSV)")
print(f"   inventario    : {n_inv:,} filas (CSV)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7 · Verificar lo que quedó

# COMMAND ----------

print("📁 Archivos de ventas:")
archivos = [f for f in dbutils.fs.ls(f"{RAW}/ventas") if f.name.endswith(".json")]
for f in archivos[:4]:
    print(f"   {f.name}  ({f.size / 1024 / 1024:.1f} MB)")
print(f"   … {len(archivos)} archivos en total")

print()
print("📄 Una muestra del contenido crudo de ventas:")
display(spark.read.json(f"{RAW}/ventas").limit(5))

print("📄 Los productos:")
display(spark.read.option("header", "true").csv(f"{RAW}/productos").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Control final: ¿los archivos quedaron bien?
# MAGIC
# MAGIC Esta celda lee **los archivos ya escritos** —no los DataFrames en memoria— y comprueba
# MAGIC que el resultado sea el esperado. Si algo salió mal, lo dice acá y no seis módulos más
# MAGIC adelante.

# COMMAND ----------

_v = spark.read.json(f"{RAW}/ventas")
_p = spark.read.option("header", "true").csv(f"{RAW}/productos")

_r = _v.agg(
    F.count("*").alias("filas"),
    F.countDistinct("producto_id").alias("productos_ref"),
    F.sum(F.when(F.col("cantidad") <= 0, 1).otherwise(0)).alias("invalidas"),
    F.countDistinct("pais").alias("paises"),
    F.sum(F.when(F.col("producto_id").isNull(), 1).otherwise(0)).alias("sin_producto"),
    F.min(F.to_date("fecha")).alias("fecha_min"),
    F.max(F.to_date("fecha")).alias("fecha_max"),
).collect()[0]

n_p = _p.count()
pct_inval = _r["invalidas"] / _r["filas"] * 100
meses = (_r["fecha_max"].year - _r["fecha_min"].year) * 12 + (_r["fecha_max"].month - _r["fecha_min"].month)

checks = [
    (_r["filas"] > 500_000,
     f"Ventas: {_r['filas']:,} (se esperan ~585.000 tras separar el lote nuevo)"),
    (_r["sin_producto"] == 0,
     f"Sin producto_id nulo: {_r['sin_producto']} nulos"),
    (1.0 <= pct_inval <= 3.0,
     f"Filas inválidas: {pct_inval:.2f}% (se espera ~2%) — para el M3"),
    (_r["paises"] == 6,
     f"Países distintos: {_r['paises']} (se esperan 6 de LATAM)"),
    (meses >= 22,
     f"Rango de fechas: {_r['fecha_min']} → {_r['fecha_max']} (~{meses} meses)"),
    (n_p > 250,
     f"Productos: {n_p:,} (se esperan ~300)"),
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

  · Pocas ventas → el join con productos perdió filas. Revisa que ambos
    lados construyan producto_id con la misma fórmula aritmética (n*7+3).
  · Rango de fechas corto → revisa el cálculo de _dias sobre 730.

  Vuelve a ejecutar el notebook completo desde el principio.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Listo
# MAGIC
# MAGIC ```
# MAGIC Tu catálogo  : se muestra arriba
# MAGIC Tu schema    : retail_<tu_usuario>
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
# MAGIC | 4 | Gobernar: enmascarar `costo_unitario` y filtrar por `pais` |
# MAGIC | 5 | Orquestar todo como un job programado |
# MAGIC | 6 | **Pronóstico de ventas con AI Functions** (`ai_forecast`) |
# MAGIC | 7 | Dashboard, Genie y una app de reabastecimiento |
# MAGIC
# MAGIC **👉 Abre el notebook `01_lakehouse_101` y empieza.**
# MAGIC
# MAGIC > Si algo falló acá, avísale a un TA antes de seguir: todos los módulos usan estos datos.
