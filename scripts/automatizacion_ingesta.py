import pandas as pd
from sqlalchemy import create_engine
import os
import sys

# =================================================================
# 1. CONFIGURACION DEL ENTORNO
# =================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_INGESTA = os.path.abspath(os.path.join(BASE_DIR, "..", "data", "DatosIngesta"))

CONFIG_SQL = {
    'server': 'localhost\\SQLEXPRESS', 
    'database': 'Inventario_DWH',
    'driver': 'ODBC Driver 17 for SQL Server'
}

# =================================================================
# 2. MOTOR DE CONEXION
# =================================================================
def obtener_motor():
    try:
        conn_str = (
            f"mssql+pyodbc://@{CONFIG_SQL['server']}/{CONFIG_SQL['database']}"
            f"?driver={CONFIG_SQL['driver']}&trusted_connection=yes"
        )
        # fast_executemany acelera masivamente los inserts y evita timeouts de buffer
        return create_engine(conn_str, fast_executemany=True, connect_args={'connect_timeout': 100})
    except Exception as e:
        print(f"Error de conexion: {e}")
        sys.exit()

# =================================================================
# 3. LOGICA DE INGESTA Y LIMPIEZA
# =================================================================
def ejecutar_carga():
    engine = obtener_motor()
    
    # El orden es fundamental para respetar las llaves foraneas
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

    print(f"--- INICIANDO INGESTA AUTOMATIZADA EN {CONFIG_SQL['database']} ---")
    
    # Diccionarios para validar integridad referencial en memoria
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
            try:
                print(f"Procesando: {archivo} >> {esquema}.{tabla}")
                df = pd.read_csv(ruta_archivo, low_memory=False)

                # Eliminacion de errores especificos 
                if tabla == 'Dim_Producto':
                    df['Volumen'] = pd.to_numeric(df['Volumen'], errors='coerce').fillna(0)
                    df['Clasificacion'] = pd.to_numeric(df['Clasificacion'], errors='coerce').fillna(0)
                    df.rename(columns={'Tamaño': 'Tamano'}, inplace=True)
                elif tabla == 'Dim_Calendario':
                    df.rename(columns={'Año': 'Anio'}, inplace=True)
                
                # Evitar duplicados en llaves primarias de dimensiones
                if esquema == 'Catalogo':
                    col_pk = df.columns[0]
                    df = df.drop_duplicates(subset=[col_pk])

                # Recoleccion de llaves para validar hechos y FK
                if tabla == 'Dim_Producto': maestros['Marca_ID'] = df['Marca_ID'].unique().tolist()
                elif tabla == 'Dim_Tienda': maestros['Tienda_ID'] = df['Tienda_ID'].unique().tolist()
                elif tabla == 'Dim_Proveedor': maestros['Proveedor_ID'] = df['Proveedor_ID'].unique().tolist()
                elif tabla == 'Dim_Calendario': maestros['Fecha_ID'] = df['Fecha_ID'].unique().tolist()
                elif tabla == 'Fact_Compras': maestros['Compra_ID'] = df['Compra_ID'].unique().tolist()

                # Validacion de Integridad Referencial (evita errores de FK en SQL)
                if esquema == 'Operaciones':
                    total_antes = len(df)
                    if 'Marca_ID' in df.columns:
                        df = df[df['Marca_ID'].isin(maestros['Marca_ID'])]
                    if 'Tienda_ID' in df.columns:
                        df = df[df['Tienda_ID'].isin(maestros['Tienda_ID'])]
                    if 'Proveedor_ID' in df.columns:
                        df = df[df['Proveedor_ID'].isin(maestros['Proveedor_ID'])]
                    
                    if 'Fecha_ID' in df.columns:
                        fechas_nuevas = df[~df['Fecha_ID'].isin(maestros['Fecha_ID'])]['Fecha_ID'].unique()
                        if len(fechas_nuevas) > 0:
                            print(f"     => Auto-generando {len(fechas_nuevas)} fechas faltantes en Dim_Calendario para mantener integridad FK...")
                            # Insertamos a la dimension calendario on-the-fly para no perder los hechos
                            import datetime
                            nuevas_filas_cal = []
                            for fid in fechas_nuevas:
                                try:
                                    f_str = str(fid)
                                    dt = datetime.datetime.strptime(f_str, '%Y%m%d')
                                    nuevas_filas_cal.append({'Fecha_ID': fid, 'Fecha': dt.strftime('%Y-%m-%d'), 'Anio': dt.year, 'Mes': dt.month, 'Trimestre': (dt.month-1)//3 + 1, 'Semana': dt.isocalendar()[1]})
                                    maestros['Fecha_ID'].append(fid)
                                except: pass
                            if nuevas_filas_cal:
                                pd.DataFrame(nuevas_filas_cal).to_sql(name='Dim_Calendario', con=engine, schema='Catalogo', if_exists='append', index=False)
                        
                        df = df[df['Fecha_ID'].isin(maestros['Fecha_ID'])]
                        
                    if 'Compra_ID' in df.columns and tabla == 'Fact_Detalle_Compras':
                        df = df[df['Compra_ID'].isin(maestros['Compra_ID'])]
                        
                    filas_huerfanas = total_antes - len(df)
                    if filas_huerfanas > 0:
                        print(f"     => Limpieza referencial: {filas_huerfanas} filas ignoradas por no existir en las dimensiones maestras.")

                # Insercion en la base de datos protegiendo la transaccion
                # Agregamos timeout y gestion de engine connection con chunksize seguro
                with engine.begin() as connection:
                    df.to_sql(name=tabla, con=connection, schema=esquema, if_exists='append', index=False, chunksize=5000)
                
                print(f"   Exito: {len(df)} filas insertadas.")

            except Exception as e:
                print(f"   Error en tabla {tabla}: {e}")
        else:
            print(f"   Aviso: El archivo {archivo} no se encuentra en la ruta.")

    print("--- PROCESO FINALIZADO ---")

if __name__ == "__main__":
    ejecutar_carga()