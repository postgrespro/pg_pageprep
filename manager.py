import argparse
import utils


def install(databases):
    con = utils.DbConnector(databases[0], args.role)
    con.exec_query(
        "alter system set pg_pageprep.databases='{}'".format(','.join(databases)))
    con.exec_query(
        "alter system set pg_pageprep.role='{}'".format(args.role))

    for db in databases:
        con = utils.DbConnector(db, args.role)
        con.exec_query("create extension pg_pageprep")


def start(databases):
    for db in databases:
        local_con = utils.DbConnector(db, args.role)

        if extension_exists(local_con):
            local_con.exec_query("select start_bgworker();")


def stop(databases):
    for db in databases:
        local_con = utils.DbConnector(db, args.role)

        if extension_exists(local_con):
            local_con.exec_query("select stop_bgworker();")


def status(databases):
    con = utils.DbConnector(databases[0], args.role)
    show_workers_list(con)
    all_done = show_todo_lists(databases)

    if args.emit_error:
        exit(0 if all_done else 1)


def show_workers_list(con):
    print("Workers list:")
    res = con.exec_query("select * from get_workers_list()")
    workers_list = [x.strip() for x in res.split()]
    if (workers_list):
        for worker in workers_list:
            dbname, status = worker.split(",")
            print("\t{}: {}".format(dbname, status))
    else:
        print("\tThere are no active pg_pageprep's workers")


def show_todo_lists(databases):
    all_done = True

    for db in databases:
        print("\n'{}' database todo list:".format(db))

        con = utils.DbConnector(db, args.role)
        if not extension_exists(con):
            continue

        jobs = con.exec_query("select relname from pg_pageprep_todo")
        jobs = jobs.strip()

        if jobs:
            all_done = False
            for job in jobs.split():
                print("\t" + job)
        else:
            print("\tAll done!")

    return all_done


def extension_exists(con):
    ext_created = con.exec_query("select exists (select * from pg_extension where extname = 'pg_pageprep')")
    if ext_created.strip() != 't':
        print("ERROR: pg_pageprep extension doesn't exist in '{}' database!".format(con.db))
        return False
    return True


def restore(databases):
    stop(databases)

    con = utils.DbConnector(databases[0], args.role)
    con.exec_query(
        "alter system set pg_pageprep.databases='{}'".format(','.join(databases)))

    for db in databases:
        con = utils.DbConnector(db, args.role)
        if extension_exists(con):
            con.exec_query("select __restore_fillfactors()")


funcs = {
    "install": install,
    "start": start,
    "stop": stop,
    "status": status,
    "restore": restore
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--database", help="Database name (required)", required=True)
    parser.add_argument("-U", "--username", dest="role", help="Role", default="postgres", required=True)
    parser.add_argument("--emit_error", action="store_true", default=None, help=argparse.SUPPRESS)
    parser.add_argument("command", nargs="?", help="command (start, stop, status)")
    args = parser.parse_args()

    # databases = [x.strip() for x in args.databases.split(",")]
    con = utils.DbConnector(args.database, args.role)
    databases_str = con.exec_query("SELECT datname FROM pg_database WHERE datname != 'template0'")
    databases = databases_str.split()

    if args.command in funcs:
        funcs[args.command](databases)
    else:
        print("unknown command {}".format(args.command))
