import subprocess
import os


class DbConnector:
    def __init__(self, db, role):
        self.db = db
        self.role = role
        self.psql_path = get_psql_executable_path()

    def exec_query(self, sql):
        try:
            res = self.exec_query_throwable(sql)
        except Exception:
            res = ''
        return res

    def exec_query_throwable(self, sql):

        p = subprocess.Popen([
            self.psql_path,
            self.db,
            "-U", self.role,
            "-c", sql,
            "-F,", "-t", "-A"],
            stdout=subprocess.PIPE)
        out, _ = p.communicate()

        if p.returncode:
            raise Exception("psql failed on query: {}".format(sql))

        return out


def get_psql_executable_path():
    pg_config_cmd = os.environ.get("PG_CONFIG") \
        if "PG_CONFIG" in os.environ else "pg_config"

    p = subprocess.Popen(
        [pg_config_cmd, "--bindir"],
        stdout=subprocess.PIPE)
    out, _ = p.communicate()

    return os.path.join(out.strip(), "psql")
