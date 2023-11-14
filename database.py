import os
import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs


class TrainingSetDataBase:
    def __init__(self) -> None:
        self.host: str = os.getenv("DN_DATABASE_HOST", "")
        self.port: str = os.getenv("DN_DATABASE_PORT", "")
        self.database: str = os.getenv("DN_DATABASE_NAME", "")
        self.user: str = os.getenv("DN_DATABASE_USER", "")
        self.password: str = os.getenv("DN_DATABASE_PASSWORD", "")
        self._prepare_data_base()

    def _get_connection(self):
        try:
            connection = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
        except:
            print("Erro na conexão com o banco de dados")
            raise
        return connection

    def _execute(self, query: str, params: dict, is_write: bool) -> list[dict]:
        connection = self._get_connection()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, params)
        connection.commit()
        if not is_write:
            result = [dict(result) for result in cursor.fetchall()]
            cursor.close()
            return result
        cursor.close()

    def _write(self, query: str, params: dict):
        self._execute(query, params, True)

    def _read(self, query: str, params: dict) -> list[dict]:
        return self._execute(query, params, False)

    def _create_schema(self):
        create_schema = """create schema if not exists desistenao"""
        self._write(create_schema, {})

    def _create_tables(self):
        create_table = """create table if not exists desistenao.training_set (NRO bigint)"""
        self._write(create_table, {})
        self._create_columns()

    def _create_columns(self):
        tables = {
            """SEXO smallint""",
            """DESISTENTE smallint""",
            """IDADE_INGRESSO smallint""",
            """RESIDE_UBERLANDIA smallint""",
            """ESTADO_CIVIL smallint""",
            """FORMA_INGRESSO smallint""",
            """MODALIDADE_INGRESSO smallint""",
            """cra_abaixo_60_ingresso smallint""",
            """gsi001 smallint""",
            """gsi002 smallint""",
            """gsi003 smallint""",
            """gsi004 smallint""",
            """gsi005 smallint"""
        }
        for table in tables:
            script_create_table = f"alter table desistenao.training_set add column if not exists {table}"
            self._write(script_create_table, {})

    def _prepare_data_base(self):
        self._create_schema()
        self._create_tables()
        self._create_columns()

    def get_by_nro(self, nro):
        params = {
            "nro": nro
        }
        return self._read("select * from desistenao.training_set ts  where nro = %(nro)s", params)

    def insert_aluno(self, nro, desistente, reside_uberlandia, estado_civil, forma_ingresso, sexo, modalidade_ingresso,
                     idade_ingresso, cra_abaixo_60_ingresso):
        params = {
            "nro": nro,
            "desistente": desistente,
            "reside_uberlandia": reside_uberlandia,
            "estado_civil": estado_civil,
            "forma_ingresso": forma_ingresso,
            "sexo": sexo,
            "modalidade_ingresso": modalidade_ingresso,
            "idade_ingresso": idade_ingresso,
            "cra_abaixo_60_ingresso": cra_abaixo_60_ingresso
        }
        return self._write(
            """INSERT INTO desistenao.training_set
                        (nro, desistente, reside_uberlandia, estado_civil, forma_ingresso, sexo,
                        modalidade_ingresso, idade_ingresso, cra_abaixo_60_ingresso)
                        VALUES(%(nro)s, %(desistente)s, %(reside_uberlandia)s, %(estado_civil)s,
                        %(forma_ingresso)s, %(sexo)s, %(modalidade_ingresso)s, %(idade_ingresso)s,
                         %(cra_abaixo_60_ingresso)s);""", params
        )

    def create_discipline_column_on_training_set(self, column):
        params = {
            "column": AsIs(column)
        }
        script_create_table = """alter table desistenao.training_set add column if not exists %(column)s 
        smallint"""
        self._write(script_create_table, params)

    def update_discipline(self, nro, discipline, value):
        params = {
            "nro": nro,
            "discipline": AsIs(discipline),
            "value": value
        }
        script = """UPDATE desistenao.training_set
                        set %(discipline)s = 
                        case
                            when %(discipline)s is NULL then %(value)s
                            else %(discipline)s + %(value)s
                        end
                        where nro = %(nro)s """
        self._write(script, params)
