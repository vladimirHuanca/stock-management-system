USE master;
GO

-- 1. ELIMINACIÓN Y CREACIÓN DE LA BASE DE DATOS
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'Inventario_DWH')
BEGIN
    ALTER DATABASE Inventario_DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Inventario_DWH;
END
GO

CREATE DATABASE Inventario_DWH;
GO
USE Inventario_DWH;
GO

-- 2. CREACIÓN DE ESQUEMAS
CREATE SCHEMA Catalogo;   -- Dimensiones
GO
CREATE SCHEMA Operaciones; -- Tablas de Hechos
GO

-- 3. TABLAS DE DIMENSIONES (DIM TABLES)

CREATE TABLE Catalogo.Dim_Calendario (
    Fecha_ID INT PRIMARY KEY,
    Fecha DATE,
    Anio INT,
    Mes INT,
    Trimestre INT,
    Semana INT
);

CREATE TABLE Catalogo.Dim_Proveedor (
    Proveedor_ID INT PRIMARY KEY,
    Nombre_Proveedor NVARCHAR(255)
);

CREATE TABLE Catalogo.Dim_Tienda (
    Tienda_ID INT PRIMARY KEY,
    Ciudad NVARCHAR(100)
);

CREATE TABLE Catalogo.Dim_Producto (
    Marca_ID INT PRIMARY KEY,
    Descripcion NVARCHAR(500),
    Tamano NVARCHAR(50),
    Volumen DECIMAL(18, 2),        -- Cambiado a DECIMAL para aceptar '162.5'
    Clasificacion DECIMAL(18, 2),  -- Cambiado a DECIMAL por seguridad
    Pack NVARCHAR(100)
);

-- 4. TABLAS DE HECHOS (FACT TABLES)

CREATE TABLE Operaciones.Fact_Ventas (
    Venta_ID INT PRIMARY KEY,
    Marca_ID INT NOT NULL,
    Tienda_ID INT NOT NULL,
    Fecha_ID INT NOT NULL,
    Cantidad INT,
    Venta_Total DECIMAL(18, 2),
    Precio_Unitario DECIMAL(18, 2),
    Impuesto DECIMAL(18, 2),
    CONSTRAINT FK_Ventas_Producto FOREIGN KEY (Marca_ID) REFERENCES Catalogo.Dim_Producto(Marca_ID),
    CONSTRAINT FK_Ventas_Tienda FOREIGN KEY (Tienda_ID) REFERENCES Catalogo.Dim_Tienda(Tienda_ID),
    CONSTRAINT FK_Ventas_Calendario FOREIGN KEY (Fecha_ID) REFERENCES Catalogo.Dim_Calendario(Fecha_ID)
);

CREATE TABLE Operaciones.Fact_Compras (
    Compra_ID INT PRIMARY KEY,
    Fecha_ID INT NOT NULL,
    Proveedor_ID INT NOT NULL,
    Cantidad INT,
    Importe DECIMAL(18, 2),
    Costo_Envio DECIMAL(18, 2),
    CONSTRAINT FK_Compras_Proveedor FOREIGN KEY (Proveedor_ID) REFERENCES Catalogo.Dim_Proveedor(Proveedor_ID),
    CONSTRAINT FK_Compras_Calendario FOREIGN KEY (Fecha_ID) REFERENCES Catalogo.Dim_Calendario(Fecha_ID)
);

CREATE TABLE Operaciones.Fact_Detalle_Compras (
    Detalle_Compra_ID INT PRIMARY KEY,
    Compra_ID INT,
    Marca_ID INT NOT NULL,
    Proveedor_ID INT NOT NULL,
    Fecha_ID INT NOT NULL,
    Cantidad INT,
    Precio_Compra DECIMAL(18, 2),
    Importe DECIMAL(18, 2),
    CONSTRAINT FK_Detalle_Producto FOREIGN KEY (Marca_ID) REFERENCES Catalogo.Dim_Producto(Marca_ID),
    CONSTRAINT FK_Detalle_Proveedor FOREIGN KEY (Proveedor_ID) REFERENCES Catalogo.Dim_Proveedor(Proveedor_ID),
    CONSTRAINT FK_Detalle_Calendario FOREIGN KEY (Fecha_ID) REFERENCES Catalogo.Dim_Calendario(Fecha_ID),
    CONSTRAINT FK_Detalle_Compra FOREIGN KEY (Compra_ID) REFERENCES Operaciones.Fact_Compras(Compra_ID)
);

CREATE TABLE Operaciones.Fact_Inventario_Inicial (
    Inventario_ID VARCHAR(100) PRIMARY KEY, -- Soporta IDs alfanuméricos
    Marca_ID INT NOT NULL,
    Tienda_ID INT NOT NULL,
    Fecha_ID INT NOT NULL,
    Unidades_Disponibles INT,
    CONSTRAINT FK_InvIni_Producto FOREIGN KEY (Marca_ID) REFERENCES Catalogo.Dim_Producto(Marca_ID),
    CONSTRAINT FK_InvIni_Tienda FOREIGN KEY (Tienda_ID) REFERENCES Catalogo.Dim_Tienda(Tienda_ID),
    CONSTRAINT FK_InvIni_Calendario FOREIGN KEY (Fecha_ID) REFERENCES Catalogo.Dim_Calendario(Fecha_ID)
);

CREATE TABLE Operaciones.Fact_Inventario_Final (
    Inventario_ID VARCHAR(100) PRIMARY KEY, -- Soporta IDs alfanuméricos
    Marca_ID INT NOT NULL,
    Tienda_ID INT NOT NULL,
    Fecha_ID INT NOT NULL,
    Unidades_Disponibles INT,
    CONSTRAINT FK_InvFin_Producto FOREIGN KEY (Marca_ID) REFERENCES Catalogo.Dim_Producto(Marca_ID),
    CONSTRAINT FK_InvFin_Tienda FOREIGN KEY (Tienda_ID) REFERENCES Catalogo.Dim_Tienda(Tienda_ID),
    CONSTRAINT FK_InvFin_Calendario FOREIGN KEY (Fecha_ID) REFERENCES Catalogo.Dim_Calendario(Fecha_ID)
);
GO
