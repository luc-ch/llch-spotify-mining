import pandas as pd
import psycopg2

from spotify_mining.configurations import Configuration


def test_db_connection():
    execute_command_postgres("SELECT 1;",)


def fetch_many_rows_from_postgres(command, amount=50):
    config = Configuration()
    with psycopg2.connect(
        host=config.db_host,
        port=config.db_port,
        database=config.db_database,
        user=config.db_user,
        password=config.db_password,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(command)
            colnames = [desc[0] for desc in cur.description]
            while True:
                records = cur.fetchmany(amount)
                if len(records) > 1:
                    yield [dict(zip(colnames, record)) for record in records]
                elif len(records) > 0:
                    yield [record[0] for record in records]
                else:
                    break


def fetch_rows_by_one_from_postgres(command,):
    config = Configuration()
    with psycopg2.connect(
        host=config.db_host,
        port=config.db_port,
        database=config.db_database,
        user=config.db_user,
        password=config.db_password,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(command)
            colnames = [desc[0] for desc in cur.description]
            while True:
                record = cur.fetchone()
                if record is not None:
                    yield dict(zip(colnames, record)) if len(record) > 1 else record[0]
                else:
                    break


def execute_command_postgres(command,):
    conn = None
    ret_val = []
    try:
        config = Configuration()
        conn = psycopg2.connect(
            host=config.db_host,
            port=config.db_port,
            database=config.db_database,
            user=config.db_user,
            password=config.db_password,
        )

        cur = conn.cursor()
        cur.execute(command)

        rows = cur.fetchall()
        if len(rows) > 0:
            if len(rows[0]) > 1:
                for row in rows:
                    ret_val.append(row)
            else:
                for row in rows:
                    ret_val.append(row[0])

        cur.close()
        conn.commit()
    except psycopg2.DatabaseError as error:
        raise error
    except Exception as error:
        print(f"ERROR: {error}")
    finally:
        if conn is not None:
            conn.close()
    return ret_val


def postgres_to_df(command):
    conn = None
    df = pd.DataFrame()
    config = Configuration()
    try:
        conn = psycopg2.connect(
            host=config.db_host,
            port=config.db_port,
            database=config.db_database,
            user=config.db_user,
            password=config.db_password,
        )
        df = pd.read_sql(command, conn)
        conn.commit()
    except psycopg2.DatabaseError as error:
        raise error
    except Exception as error:
        print(f"ERROR: {error}")
    finally:
        if conn is not None:
            conn.close()
    return df
