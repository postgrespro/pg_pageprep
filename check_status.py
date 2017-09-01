import argparse
import utils


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


def show_todo_lists(con):
    res = con.exec_query("show pg_pageprep.databases")
    for db in [x.strip() for x in res.split()]:
        print("\n'{}' database todo list:".format(db))

        jobs = con.exec_query("select relname from pg_pageprep_todo")
        jobs = jobs.strip()

        if jobs:
            for job in jobs.split():
                print("\t" + job)
        else:
            print("\tAll done!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', help='database name', required=True)
    parser.add_argument('-U','--username', dest='role', help='Role', default='postgres', required=True)
    args = parser.parse_args()

    con = utils.DbConnector(args.database, args.role)
    show_workers_list(con)
    show_todo_lists(con)
