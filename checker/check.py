#!/usr/bin/env python3
#coding: utf-8

import subprocess
import os.path
import contextlib
import getpass
import argparse

from testgres import get_new_node, configure_testgres

current_dir = os.path.abspath(os.path.dirname(__file__))
configure_cmd = 'CFLAGS="-g3 -O0" ./configure --prefix=%s --enable-depend --enable-cassert --enable-debug --enable-tap-tests'

conf = {
    'pg96_stable': {
        'branch': 'REL9_6_STABLE',
    },
    'pg10_stable': {
        'branch': 'REL_10_STABLE',
    },
    'pgpro96_standard': {
        'branch': 'PGPRO9_6',
    },
    'pgpro10_standard': {
        'branch': 'PGPRO10',
    },
    'pgpro96_enterprise': {
        'branch': 'PGPROEE9_6',
    },
    'pgpro10_enterprise': {
        'branch': 'PGPROEE10_pg_upgrade',
    }
}

import logging
import logging.config

logfile = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'tests.log')
LOG_CONFIG = {
    'version': 1,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'base_format',
            'level': logging.DEBUG,
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': logfile,
            'formatter': 'base_format',
            'level': logging.DEBUG,
        },
    },
    'formatters': {
        'base_format': {
            'format': '%(node)-5s: %(message)s',
        },
    },
    'root': {
        'handlers': ('file', ),
        'level': 'DEBUG',
    },
}
logging.config.dictConfig(LOG_CONFIG)

dest_name = 'pgpro10_enterprise'
addconf = '''
shared_preload_libraries='pg_pageprep'
pg_pageprep.role = '%s'
pg_pageprep.database = 'postgres'
pg_pageprep.enable_workers=off
pg_pageprep.per_page_delay = 1
pg_pageprep.per_relation_delay = 1
''' % getpass.getuser()

# create different types of tables that will processed by pg_pageprep
sql_fill = '''
DROP TABLE IF EXISTS two CASCADE;
DROP TABLE IF EXISTS ten;

CREATE TABLE two(a tsvector) WITH (fillfactor=100);
INSERT INTO two SELECT 'a:1 b:2 c:3'::tsvector FROM generate_series(1, 1000) i;
CREATE MATERIALIZED VIEW view_two AS SELECT * FROM two;
SELECT scan_pages('two'::REGCLASS);
SELECT scan_pages('view_two'::REGCLASS);

CREATE TABLE ten (id SERIAL, msg TEXT);
ALTER TABLE ten ALTER COLUMN msg SET STORAGE EXTERNAL;
COPY ten FROM '{0}/input/toast.csv';
'''

# just read all pages
sql_fillcheck = (
    'SELECT a FROM two',
    'SELECT a FROM view_two',
    'SELECT * FROM ten',
)

def rel(*args):
    return os.path.join(current_dir, *args)

pgpro_dir = rel('postgrespro')


@contextlib.contextmanager
def cwd(path):
    print("cwd: ", path)
    curdir = os.getcwd()
    os.chdir(path)

    try:
        yield
    finally:
        print("cwd:", curdir)
        os.chdir(curdir)


def cmd(command, env=None):
    print("run: ", command)
    subprocess.check_call(command, shell=True, env=env)


def set_environ_for(key):
    bin_dir = rel('build', key, 'bin')
    os.environ['PG_CONFIG'] = os.path.join(bin_dir, 'pg_config')
    os.environ['USE_PGXS'] = '1'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--fill-data", help="fill databases with data",
            dest='fill_data', action='store_true')
    parser.add_argument("--check-upgrade", help="fill databases with data",
            dest='check_upgrade', action='store_true')
    parser.add_argument("--branch", help="check only one branch",
            dest='branch', default=None, action='store')
    parser.add_argument("--bench-time", help="time for pgbench",
            dest='bench_time', default=60, action='store', type=int)
    args = parser.parse_args()

    configure_testgres(cache_initdb=False, cache_pg_config=False)

    build_dir = rel('build')
    if not os.path.exists(build_dir):
        os.mkdir(build_dir)

    pageprep_dir = rel('..')

    # make all
    for key, options in conf.items():
        prefix_dir = rel('build', key)

        if args.branch and key != dest_name and args.branch != key:
            continue

        if os.path.exists(prefix_dir):
            print("%s already built in '%s'. skipped" % (key, prefix_dir))
            continue

        data_dir = rel('build', key, 'data')

        with cwd(pgpro_dir):
            cmd('git clean -fdx && git checkout %s' % options['branch'])
            cmd(configure_cmd % prefix_dir)
            cmd('make install -j10')

        with cwd(prefix_dir):
            cmd('bin/initdb -D %s' % data_dir)

    # pgbench -i, and pg_upgrade
    if args.fill_data:
        for key, options in conf.items():
            prefix_dir = rel('build', key)

            if args.branch and key != args.branch and key != dest_name:
                continue

            set_environ_for(key)
            with cwd(pageprep_dir):
                cmd('make clean', env=os.environ)
                cmd('make install', env=os.environ)

            if key == dest_name:
                continue

            with get_new_node('node_%s' % key, base_dir=prefix_dir, use_logging=True) as node:
                node.default_conf(log_statement='ddl')
                node.append_conf('postgresql.conf', addconf)
                node.start()

                # add our testing tables and start pg_pageprep
                node.psql('postgres',  sql_fill.format(pageprep_dir))

                print("run: pgbench %s before upgrade for %s seconds" % (key, args.bench_time))
                node.pgbench_init(scale=10)
                p = node.pgbench(options=['--time', str(args.bench_time), '-c', '4', '-j', '8'])
                p.wait()

                node.psql('postgres', 'drop extension pg_pageprep;')
                node.psql('postgres', 'create extension pg_pageprep;')
                node.psql('postgres', 'select start_bgworker();')
                node.stop()

    if args.check_upgrade:
        for key, options in conf.items():
            if key == dest_name:
                continue

            #TODO: remove these two lines when icu error will be fixed
            if key == 'pgpro10_standard':
                continue

            if args.branch and key != args.branch:
                continue

            with cwd(rel('build', dest_name)):
                cmd('rm -rf ./data')
                cmd('bin/initdb -D ./data')

            with get_new_node('node_%s' % dest_name,
                    base_dir=rel('build', dest_name), use_logging=True) as node:
                node.default_conf(log_statement='ddl')
                node.append_conf('postgresql.conf', addconf)

            dest_conf = conf[dest_name]
            with cwd(rel('build')):
                cmd("{0}/bin/pg_upgrade -b {1}/bin -d {1}/data -B{0}/bin -D{0}/data".format(dest_name, key))
                cmd("./analyze_new_cluster.sh")

            with get_new_node('node_%s' % dest_name,
                    base_dir=rel('build', dest_name), use_logging=True) as node:
                node.default_conf(log_statement='ddl')
                node.start()
                with node.replicate(use_logging=True) as replica:
                    for sql in sql_fillcheck:
                        node.psql('postgres', sql)

                print("run: pgbench for %s after upgrade from %s" % (dest_name, key))
                p = node.pgbench(options=['--time', str(args.bench_time), '-c', '4', '-j', '8'])
                p.wait()

                print("run: check our tables")
                for sql in sql_fillcheck:
                    node.psql('postgres', sql)

                node.stop()
