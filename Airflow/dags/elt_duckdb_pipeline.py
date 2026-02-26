from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os

BASE_PATH = "/opt/airflow"
DB_PATH = os.path.join(BASE_PATH, "dw.duckdb")
STAGING_PATH = os.path.join(BASE_PATH, "staging", "*.csv")

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="elt_duckdb_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def elt_pipeline():

    @task()
    def limpiar_tablas():
        import duckdb
        import logging
        conn = duckdb.connect(DB_PATH)
        conn.execute("DROP TABLE IF EXISTS staging_raw")
        conn.execute("DROP TABLE IF EXISTS fact_finanzas_elt")
        logging.info("Tablas eliminadas")
        conn.close()

    @task()
    def cargar_staging():
        import duckdb
        import logging
        conn = duckdb.connect(DB_PATH)

        conn.execute(f"""
            CREATE TABLE staging_raw AS
            SELECT *
            FROM read_csv_auto('{STAGING_PATH}')
        """)

        total = conn.execute("SELECT COUNT(*) FROM staging_raw").fetchone()[0]
        logging.info(f"Total registros en STAGING: {total}")

        conn.close()

    @task()
    def transformar_datos():
        import duckdb
        import logging
        conn = duckdb.connect(DB_PATH)

        total_staging = conn.execute(
            "SELECT COUNT(*) FROM staging_raw"
        ).fetchone()[0]

        conn.execute("""
            CREATE TABLE fact_finanzas_elt AS
            SELECT DISTINCT
                id,
                salario,
                COALESCE(gastos, 0) AS gastos,
                CASE
                    WHEN regexp_matches(fecha, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
                        THEN CAST(fecha AS DATE)
                    WHEN regexp_matches(fecha, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$')
                        THEN STRPTIME(fecha, '%d/%m/%Y')
                    WHEN regexp_matches(fecha, '^[0-9]{2}-[0-9]{2}-[0-9]{4}$')
                        THEN STRPTIME(fecha, '%m-%d-%Y')
                    ELSE NULL
                END AS fecha,
                CASE
                    WHEN correo IS NOT NULL
                        THEN sha256(correo)
                    ELSE NULL
                END AS correo_hash,
                salario - COALESCE(gastos, 0) AS utilidad
            FROM staging_raw
            WHERE
                id IS NOT NULL
                AND fecha IS NOT NULL
                AND salario > 0
                AND COALESCE(gastos, 0) >= 0
        """)

        total_final = conn.execute(
            "SELECT COUNT(*) FROM fact_finanzas_elt"
        ).fetchone()[0]

        logging.info(f"Registros antes: {total_staging}")
        logging.info(f"Registros después: {total_final}")
        logging.info(f"Registros eliminados: {total_staging - total_final}")

        conn.close()

    @task()
    def metricas_finales():
        import duckdb
        import logging
        conn = duckdb.connect(DB_PATH)

        resumen = conn.execute("""
            SELECT
                COUNT(*) as total,
                AVG(utilidad) as promedio_utilidad,
                MAX(utilidad) as max_utilidad
            FROM fact_finanzas_elt
        """).fetchdf()

        logging.info(resumen.to_string())
        conn.close()

    limpieza = limpiar_tablas()
    staging = cargar_staging()
    transformacion = transformar_datos()
    metricas = metricas_finales()

    limpieza >> staging >> transformacion >> metricas


elt_dag = elt_pipeline()