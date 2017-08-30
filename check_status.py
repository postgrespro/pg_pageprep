import argparse
import utils


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', help='database name', required=True)
    parser.add_argument('-U','--username', dest='role', help='Role', default='postgres', required=True)
    args = parser.parse_args()

    con = utils.DbConnector(args.database, args.role)
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

    # for db in databases:
    #     con.exec_query("create extension pg_pageprep")

    # con.exec_query(
    #     "alter system set pg_pageprep.databases='{}'".format(','.join(databases)))
    # con.exec_query(
    #     "alter system set pg_pageprep.role='{}'".format(args.role))
