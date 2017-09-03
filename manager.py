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
        local_con.exec_query("select start_bgworker();")


def stop(databases):
    for db in databases:
        local_con = utils.DbConnector(db, args.role)
        local_con.exec_query("select stop_bgworker();")


def status(databases):
    con = utils.DbConnector(databases[0], args.role)
    show_workers_list(con)
    show_todo_lists(databases)


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
    for db in databases:
        print("\n'{}' database todo list:".format(db))

        con = utils.DbConnector(db, args.role)
        jobs = con.exec_query("select relname from pg_pageprep_todo")
        jobs = jobs.strip()

        if jobs:
            for job in jobs.split():
                print("\t" + job)
        else:
            print("\tAll done!")


funcs = {
    'install': install,
    'start': start,
    'stop': stop,
    'status': status
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--databases', help='Coma separated databases list (required)', required=True)
    parser.add_argument('-U','--username', dest='role', help='Role', default='postgres', required=True)
    parser.add_argument('command', nargs='?', help='command (start, stop, status)')
    args = parser.parse_args()

    databases = [x.strip() for x in args.databases.split(",")]

    if args.command in funcs:
        funcs[args.command](databases)
    else:
        print("unknown command {}".format(args.command))
