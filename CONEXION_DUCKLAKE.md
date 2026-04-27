# Cómo conectarse a DuckLake (PostgreSQL + MinIO en cbhcloud)

## Arquitectura

Este setup usa tres componentes:

| Componente | Rol | Visibilidad |
|---|---|---|
| **PostgreSQL** (`ducklake-postgres2`) | Catálogo de metadatos (nombres de tablas, esquemas, historial) | Privado |
| **MinIO** (`ducklake-minio`) | Almacenamiento de archivos parquet (los datos reales) | Público |
| **DuckDB** (Python local) | Motor de consultas que conecta ambos | — |

PostgreSQL guarda el "mapa" del lago. MinIO guarda los datos. DuckDB los une.

---

## Paso 1 — Abrir el SSH tunnel hacia PostgreSQL

PostgreSQL está en modo privado, así que no es accesible directamente desde internet.
Hay que redirigir su puerto a tu máquina local antes de correr el script.

Abre una terminal y deja este comando corriendo (no lo cierres):

```bash
ssh -L 5432:localhost:5432 ducklake-postgres2@deploy.cloud.cbh.kth.se -N
```

Mientras ese comando esté activo, tu máquina escucha en `localhost:5432`
y reenvía todo al PostgreSQL en cbhcloud.

---

## Paso 2 — Crear el entorno Python

```bash
python -m venv venv
source venv/bin/activate        # en Windows: venv\Scripts\activate
pip install duckdb minio numpy pandas
```

---

## Paso 3 — Crear el archivo `main.py`

```python
import duckdb
from minio import Minio

# ── Credenciales MinIO (público en cbhcloud) ────────────────────────────────
MINIO_ENDPOINT = "ducklake-minio.app.cloud.cbh.kth.se"
MINIO_USER     = "minioadmin"
MINIO_PASSWORD = "87654321"
BUCKET_NAME    = "ducklake"

# ── Credenciales PostgreSQL (privado, accesible via SSH tunnel) ─────────────
PG_HOST     = "localhost"   # el tunnel redirige localhost:5432 → cbhcloud
PG_DB       = "ducklake"
PG_USER     = "duck"
PG_PASSWORD = "123456"
PG_PORT     = 5432


def ensure_bucket():
    """Crea el bucket en MinIO si no existe."""
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_USER, secret_key=MINIO_PASSWORD, secure=True)
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' creado.")


def connect():
    """Devuelve una conexión DuckDB con el catálogo DuckLake adjunto."""
    con = duckdb.connect()

    # Instalar extensiones necesarias (solo la primera vez, luego quedan cacheadas)
    con.execute("INSTALL ducklake;")
    con.execute("INSTALL postgres;")
    con.execute("LOAD ducklake;")
    con.execute("LOAD postgres;")

    # Secret de MinIO: le dice a DuckDB cómo hablar con el almacenamiento S3/MinIO
    con.execute(f"""
    CREATE OR REPLACE SECRET minio_secret (
        TYPE s3,
        KEY_ID '{MINIO_USER}',
        SECRET '{MINIO_PASSWORD}',
        ENDPOINT '{MINIO_ENDPOINT}',
        URL_STYLE 'path',   -- MinIO requiere path-style, no virtual-hosted
        USE_SSL true
    );
    """)

    # ATTACH adjunta el catálogo DuckLake:
    #   - La cadena "ducklake:postgres:..." le dice que el catálogo está en PostgreSQL
    #   - DATA_PATH es donde están los archivos parquet en MinIO
    #   - El valor s3://ducklake/ debe coincidir con lo que el catálogo tiene registrado
    con.execute(f"""
    ATTACH 'ducklake:postgres:host={PG_HOST} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} port={PG_PORT}'
    AS my_lake (DATA_PATH 's3://{BUCKET_NAME}/');
    """)

    return con


def main():
    ensure_bucket()
    con = connect()
    print("Conectado a DuckLake.\n")

    # Listar solo las tablas de usuario (filtrar tablas internas de DuckLake)
    tablas = con.execute("""
        SELECT database, schema, name
        FROM (SHOW ALL TABLES)
        WHERE database = 'my_lake'
    """).fetchall()

    print("Tablas disponibles:")
    for db, schema, nombre in tablas:
        print(f"  {db}.{schema}.{nombre}")

    # Ejemplo de consulta
    print("\n--- kunder ---")
    print(con.execute("SELECT * FROM my_lake.main.kunder LIMIT 5").fetchdf())

    print("\n--- produkter ---")
    print(con.execute("SELECT * FROM my_lake.main.produkter LIMIT 5").fetchdf())

    print("\n--- ordrar ---")
    print(con.execute("SELECT * FROM my_lake.main.ordrar LIMIT 5").fetchdf())


if __name__ == "__main__":
    main()
```

---

## Paso 4 — Correr el script

Con el SSH tunnel activo (Paso 1), en otra terminal:

```bash
python main.py
```

---

## Notas importantes

**¿Por qué `localhost` como host de PostgreSQL?**
El SSH tunnel redirige `localhost:5432` al servidor real en cbhcloud.
Si cierras la terminal con el tunnel, el script no puede conectarse a PostgreSQL.

**¿Por qué `URL_STYLE 'path'`?**
MinIO no soporta el estilo de URL virtual-hosted (`bucket.endpoint.com`).
Necesita path-style (`endpoint.com/bucket`).

**¿Por qué el DATA_PATH es `s3://ducklake/` y no otra ruta?**
DuckLake guarda la ruta de datos dentro del catálogo de PostgreSQL al inicializarse.
Si intentas usar una ruta diferente, DuckDB lanza un error de incompatibilidad.
El valor correcto es el que ya está registrado en el catálogo.

**¿Qué son las tablas `__ducklake_metadata_*`?**
Son tablas internas que DuckLake usa para gestionar el catálogo (snapshots, columnas, archivos).
No las modifiques. Tus datos reales están en las tablas sin ese prefijo.
