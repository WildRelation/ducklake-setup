import duckdb
from minio import Minio

MINIO_ENDPOINT = "ducklake-minio.app.cloud.cbh.kth.se"
MINIO_USER     = "minioadmin"
MINIO_PASSWORD = "87654321"
BUCKET_NAME    = "ducklake"

PG_HOST     = "localhost"  # via SSH tunnel: ssh -L 5432:localhost:5432 ducklake-postgres2@deploy.cloud.cbh.kth.se -N
PG_DB       = "ducklake"
PG_USER     = "duck"
PG_PASSWORD = "123456"
PG_PORT     = 5432


def ensure_bucket():
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_USER, secret_key=MINIO_PASSWORD, secure=True)
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' creado.")


def connect():
    con = duckdb.connect()
    con.execute("INSTALL ducklake;")
    con.execute("INSTALL postgres;")
    con.execute("LOAD ducklake;")
    con.execute("LOAD postgres;")

    con.execute(f"""
    CREATE OR REPLACE SECRET minio_secret (
        TYPE s3,
        KEY_ID '{MINIO_USER}',
        SECRET '{MINIO_PASSWORD}',
        ENDPOINT '{MINIO_ENDPOINT}',
        URL_STYLE 'path',
        USE_SSL true
    );
    """)

    con.execute(f"""
    ATTACH 'ducklake:postgres:host={PG_HOST} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} port={PG_PORT}'
    AS my_lake (DATA_PATH 's3://{BUCKET_NAME}/');
    """)
    return con


def main():
    ensure_bucket()
    con = connect()
    print("Conectado a DuckLake.\n")

    # Mostrar solo tablas de usuario (no metadatos internos)
    tablas = con.execute("""
        SELECT database, schema, name
        FROM (SHOW ALL TABLES)
        WHERE database = 'my_lake'
    """).fetchall()

    print("Tablas en my_lake:")
    for db, schema, nombre in tablas:
        print(f"  {db}.{schema}.{nombre}")

    print("\n--- kunder ---")
    print(con.execute("SELECT * FROM my_lake.main.kunder LIMIT 5").fetchdf())

    print("\n--- produkter ---")
    print(con.execute("SELECT * FROM my_lake.main.produkter LIMIT 5").fetchdf())

    print("\n--- ordrar ---")
    print(con.execute("SELECT * FROM my_lake.main.ordrar LIMIT 5").fetchdf())


if __name__ == "__main__":
    main()
