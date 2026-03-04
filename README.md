# Sistema de Gestión de Inventario (ETL Pipeline Automatizado en la Nube)

Este repositorio contiene la arquitectura completa para la extracción, transformación y carga (ETL) de un sistema de inventarios, optimizado para procesar transacciones masivas directamente desde Kaggle hacia Google BigQuery.

---

## 1. Arquitectura y Flujo de Trabajo (Pipeline)

El proyecto utiliza un orquestador maestro en Python (`main_etl.py`) que gobierna el proceso completo en 3 etapas sin intervención humana:

1. **Extracción Automática (Kaggle API):** 
   Se conecta al dataset en la nube y descarga los más de 3 millones de registros crudos en formato CSV directamente en la carpeta `/data/`.
2. **Transformación (Jupyter Notebooks Limpios):**
   Utilizando subprocesos en Background (`nbconvert`), el Orquestador ejecuta secuencialmente 6 Notebooks especializados que viven en la carpeta `/scripts/`.
   - Limpian caracteres nulos e inyectan valores "Sin Especificar".
   - Convierten medidas de empaques (Packs x Cantidad) en unidades reales.
   - Corrigen incongruencias generadas por la fuente original.
   - Pumblican los resultados "perfectos" como un Modelo Estrella en `/data/DatosIngesta/`.
3. **Carga en la Nube (Google BigQuery):**
   El Orquestador lee el bloque Maestro, mapea todas las llaves (Foreign Keys) para asegurar que no se suban filas huérfanas o truncadas, *Auto-Genera las Fechas que pudiesen faltar en la dimensión calendario* (salvando el historial de inventarios) y sube 9 tablas mediante `pandas-gbq` directo a tu Instancia de BigQuery lista para **Power BI**.

---

## 2. Requisitos Previos

Antes de comenzar, asegúrate de tener instalado:
* **Python 3.x**.
* Una cuenta de Kaggle configurada (`kaggle.json` en `~/.kaggle/`).
* Tu archivo de Google Cloud Credentials llamado **`google_key.json`** alojado en la raíz de este proyecto (no subas este archivo a GitHub por seguridad; ya está ignorado en `.gitignore`).

---

## 3. ¿Cómo Ejecutarlo?

Solamente necesitas correr un único comando en tu terminal para desatar la automatización:

```bash
python main_etl.py
```

### ¿Qué hará por ti?
1. Detectará si te falta alguna librería como Pandas, Google Cloud, PyArrow o Jupyter, y la **auto-instalará silenciósamente** de acuerdo al `requirements.txt`.
2. Descargará los datos y ejecutará los notebooks.
3. Subirá casi **4 millones de registros limpios** agrupados en Hechos (Fact) y Dimensiones (Dim) a la Base de Datos.

---

## 4. Estructura del Data Warehouse (BigQuery/SQL Server)

Los datos cargan respondiendo a un modelo estrella tradicional configurado en los Scripts de creación de Bases de Datos (`sql/creacion_tablas.sql`):

**Catálogo (Dimensiones):**
- `Catalogo.Dim_Calendario`: 387 filas.
- `Catalogo.Dim_Proveedor`: 126 filas.
- `Catalogo.Dim_Tienda`: 80 filas.
- `Catalogo.Dim_Producto`: 12,260 filas.

**Operaciones (Hechos):**
- `Operaciones.Fact_Compras`: 5,543 filas.
- `Operaciones.Fact_Inventario_Inicial`: 206,529 filas.
- `Operaciones.Fact_Inventario_Final`: 224,489 filas.
- `Operaciones.Fact_Ventas`: ~1,048,575 filas.
- `Operaciones.Fact_Detalle_Compras`: ~2,372,471 filas.

> [!NOTE] 
> Todas las carpetas de datos masivos `.csv` intermedios, entornos virtuales e historiales pesados se encuentran protegidos fuera de Control de Versiones. El repositorio solo almacena la ingeniería y el cerebro (`*.py` e `*.ipynb`).
