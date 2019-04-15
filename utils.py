import os
import six
import subprocess
import sys


# Try to get the stdout encoding. In case of failure (for example, we use piping
# or redirect output to the file), use 'ascii'.
def get_stdout_encoding():
    return sys.stdout.encoding or 'ascii'


# If the input string is a binary string, and the current version of Python uses
# the type 'str' for text strings, decode the string with the specified
# encoding. Otherwise, return the input string unchanged.
def decode_if_necessary(string, encoding):
    if (isinstance(string, six.binary_type) and issubclass(str, six.text_type)):
        return string.decode(encoding)
    else:
        return string


def run_cmd(*cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, _ = p.communicate()

    if p.returncode:
        raise Exception("command \"{}\" failed".format("\" \"".join(cmd)))

    # In both Python 2 and 3 the variable out is a binary string, but in all
    # other parts of the code we use the type 'str' which is a text type in
    # Python 3. Therefore if necessary return the decoded string so we do not
    # get for example compilation errors.
    return decode_if_necessary(out, get_stdout_encoding())


class DbConnector:
    def __init__(self, db, role):
        self.db = db
        self.role = role
        self.psql_path = get_psql_executable_path()

    def exec_query(self, sql):
        try:
            # Pass optional arguments before mandatory ones (this is important
            # for BSD and Windows).
            res = run_cmd(
                self.psql_path,
                "-U", self.role,
                "-c", sql,
                "-F,", "-t", "-A",
                self.db)
        except Exception as ex:
            print(ex)
            res = ''
        return res


def get_psql_executable_path():
    pg_config_cmd = os.environ.get("PG_CONFIG") \
        if "PG_CONFIG" in os.environ else "pg_config"

    bin_dir = run_cmd(pg_config_cmd, "--bindir")

    return os.path.join(bin_dir.strip(), "psql")
