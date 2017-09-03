import argparse
import utils


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--databases', help='Coma separated databases list (required)', required=True)
    parser.add_argument('-U','--username', dest='role', help='Role', default='postgres', required=True)
    args = parser.parse_args()

    databases = [x.strip() for x in args.databases.split(",")]
    con = utils.DbConnector(databases[0], args.role)

    con.exec_query(
        "alter system set pg_pageprep.databases='{}'".format(','.join(databases)))
    con.exec_query(
        "alter system set pg_pageprep.role='{}'".format(args.role))

    for db in databases:
        con = utils.DbConnector(db, args.role)
        con.exec_query("create extension pg_pageprep")
