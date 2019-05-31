import subprocess
import os


class DbConnector:
    def __init__(self, db, role, pgpath=None):
        self.db = db
        self.role = role
        if pgpath is None:
            self.psql_path = "psql"
        else:
            self.psql_path = os.path.join(pgpath.strip(), "psql")

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
