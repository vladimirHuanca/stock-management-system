import os
import sys
import subprocess
import glob
import json
import datetime

# --- 0. AUTO-INSTALADOR DE DEPENDENCIAS ---
def instalar_dependencias():
    print("--- 0. COMPROBANDO DEPENDENCIAS (requirements.txt) ---")
    if os.path.exists("requirements.txt"):
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "--quiet"], check=True)
            print("Dependencias verificadas y listas.")
        except subprocess.CalledProcessError:
            print("Aviso: Hubo un problema al instalar las dependencias silenciosamente.")
            
instalar_dependencias()

# --- IMPORTACIONES DE TERCEROS ---
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from google.cloud import bigquery
from kaggle.api.kaggle_api_extended import KaggleApi

# CONFIGURACIÓN
PROJECT_ID = "henry-inventory-analytics"
DATASET_ID = "Inventario_DWH"
DATASET_SLUG = "bhanupratapbiswas/inventory-analysis-case-study"
RUTA_INGESTA = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "DatosIngesta"))

def obtener_cliente_bq():
    if os.path.exists("google_key.json"):
        return bigquery.Client.from_service_account_json("google_key.json")
    else:
        info = json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=PROJECT_ID)

def descargar_datos():
    print("--- 1. DESCARGANDO DATOS DE KAGGLE ---")
    api = KaggleApi()
    api.authenticate()
    if not os.path.exists('data'): os.makedirs('data')
    api.dataset_download_files(DATASET_SLUG, path='data/', unzip=True)
    print("Descarga completada.")

def limpiar_datos():
    print("--- 2. EJECUTANDO PIPELINE DE LIMPIEZA (NOTEBOOKS) ---")
    notebooks = [
        'scripts/limpieza_inventario_inicial.ipynb',
        'scripts/limpieza_inventario_final.ipynb',
        'scripts/limpieza_compras.ipynb',
        'scripts/limpieza_detalle_compras.ipynb',
        'scripts/limpieza_ventas.ipynb',
        'scripts/limpieza_productos.ipynb'
    ]
    for nb in notebooks:
        print(f"Ejecutando {nb}...")
        try:
            subprocess.run(['python', '-m', 'jupyter', 'nbconvert', '--to', 'notebook', '--execute', nb, '--inplace'], check=True)
            print(f"[{nb}] OK.")
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Fallo al ejecutar {nb}: {e}")
            raise e

def cargar_bigquery():
    print(f"--- 3. INICIANDO INGESTA AUTOMATIZADA A BIGQUERY ({DATASET_ID}) ---")
    client = obtener_cliente_bq()
    credentials = client._credentials
    
    pipeline_carga = [
        ('Dim_Calendario.csv', 'Catalogo', 'Dim_Calendario'),
        ('Dim_Proveedor.csv', 'Catalogo', 'Dim_Proveedor'),
        ('Dim_Tienda.csv', 'Catalogo', 'Dim_Tienda'),
        ('Dim_Producto.csv', 'Catalogo', 'Dim_Producto'),
        ('Fact_Ventas.csv', 'Operaciones', 'Fact_Ventas'),
        ('Fact_Compras.csv', 'Operaciones', 'Fact_Compras'),
        ('Fact_Detalle_Compras.csv', 'Operaciones', 'Fact_Detalle_Compras'),
        ('Fact_Inventario_Inicial.csv', 'Operaciones', 'Fact_Inventario_Inicial'),
        ('Fact_Inventario.csv', 'Operaciones', 'Fact_Inventario_Final')
    ]

    maestros = {
        'Marca_ID': [],
        'Tienda_ID': [],
        'Proveedor_ID': [],
        'Fecha_ID': [],
        'Compra_ID': []
    }

    for archivo, esquema, tabla in pipeline_carga:
        ruta_archivo = os.path.join(RUTA_INGESTA, archivo)
        if os.path.exists(ruta_archivo):
            print(f"Procesando: {archivo} >> {esquema}_{tabla}")
            df = pd.read_csv(ruta_archivo, low_memory=False)

            # --- CORRECCIONES Y LLAVES ---
            if tabla == 'Dim_Producto':
                df['Volumen'] = pd.to_numeric(df['Volumen'], errors='coerce').fillna(0)
                # Clasificacion ahora viene como texto ("Vinos"/"Licores") desde el DataFrame de limpieza
                if 'Tamaño' in df.columns:
                    df.rename(columns={'Tamaño': 'Tamano'}, inplace=True)
            elif tabla == 'Dim_Calendario':
                if 'Año' in df.columns:
                    df.rename(columns={'Año': 'Anio'}, inplace=True)

            if esquema == 'Catalogo':
                col_pk = df.columns[0]
                df = df.drop_duplicates(subset=[col_pk])

            if tabla == 'Dim_Producto': maestros['Marca_ID'] = df['Marca_ID'].unique().tolist()
            elif tabla == 'Dim_Tienda': maestros['Tienda_ID'] = df['Tienda_ID'].unique().tolist()
            elif tabla == 'Dim_Proveedor': maestros['Proveedor_ID'] = df['Proveedor_ID'].unique().tolist()
            elif tabla == 'Dim_Calendario': maestros['Fecha_ID'] = df['Fecha_ID'].unique().tolist()
            elif tabla == 'Fact_Compras': maestros['Compra_ID'] = df['Compra_ID'].unique().tolist()

            # --- INTEGRIDAD REFERENCIAL ---
            if esquema == 'Operaciones':
                total_antes = len(df)
                if 'Marca_ID' in df.columns: df = df[df['Marca_ID'].isin(maestros['Marca_ID'])]
                if 'Tienda_ID' in df.columns: df = df[df['Tienda_ID'].isin(maestros['Tienda_ID'])]
                if 'Proveedor_ID' in df.columns: df = df[df['Proveedor_ID'].isin(maestros['Proveedor_ID'])]
                
                if 'Fecha_ID' in df.columns:
                    fechas_nuevas = df[~df['Fecha_ID'].isin(maestros['Fecha_ID'])]['Fecha_ID'].unique()
                    if len(fechas_nuevas) > 0:
                        print(f"     => Auto-generando {len(fechas_nuevas)} fechas faltantes en BigQuery...")
                        nuevas_filas_cal = []
                        for fid in fechas_nuevas:
                            try:
                                dt = datetime.datetime.strptime(str(fid), '%Y%m%d')
                                nuevas_filas_cal.append({'Fecha_ID': fid, 'Fecha': dt.strftime('%Y-%m-%d'), 'Anio': dt.year, 'Mes': dt.month, 'Trimestre': (dt.month-1)//3 + 1, 'Semana': dt.isocalendar()[1]})
                                maestros['Fecha_ID'].append(fid)
                            except: pass
                        if nuevas_filas_cal:
                            df_cal = pd.DataFrame(nuevas_filas_cal)
                            df_cal['Fecha'] = pd.to_datetime(df_cal['Fecha'])
                            df_cal.to_gbq(f"{DATASET_ID}.Catalogo_Dim_Calendario", project_id=PROJECT_ID, if_exists='append', credentials=credentials)
                    
                    df = df[df['Fecha_ID'].isin(maestros['Fecha_ID'])]
                
                if 'Compra_ID' in df.columns and tabla == 'Fact_Detalle_Compras':
                    df = df[df['Compra_ID'].isin(maestros['Compra_ID'])]

                filas_huerfanas = total_antes - len(df)
                if filas_huerfanas > 0:
                    print(f"     => Limpieza referencial: {filas_huerfanas} filas ignoradas.")

            # --- CARGA A BIGQUERY ---
            # El esquema en formato BigQuery es DATASET.ESQUEMA_TABLA
            table_id = f"{esquema}_{tabla}"
            
            # Casteos especificos de tipos en DataFrames antes de ir a BQ
            if 'Fecha' in df.columns:
                df['Fecha'] = pd.to_datetime(df['Fecha'])

            # Optimizamos a chunksize moderado dada la red HTTP
            df.to_gbq(f"{DATASET_ID}.{table_id}", project_id=PROJECT_ID, if_exists='replace', credentials=credentials, chunksize=50000)
            print(f"   Exito: Tabla {table_id} subida ({len(df)} filas).")
            
        else:
            print(f"   Aviso: {archivo} no existe en la ruta.")

def procesar_etl():
    descargar_datos()
    limpiar_datos()
    cargar_bigquery()
    print("--- ETL BIGQUERY FINALIZADO ---")

if __name__ == "__main__":
    procesar_etl()