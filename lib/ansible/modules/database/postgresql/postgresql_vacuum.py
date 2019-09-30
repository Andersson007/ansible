#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2019, Andrew Klychkov (@Andersson007) <aaklychkov@mail.ru>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'supported_by': 'community',
    'status': ['preview']
}

DOCUMENTATION = r'''
---
module: postgresql_vacuum
short_description: Garbage-collect and optionally analyze a PostgreSQL database
description:
- Garbage-collect and optionally analyze a PostgreSQL database.
- It allows to reduce influence of vacuum on database by using C(skip_period) to skip tables
  that were vacuumed recently.
version_added: '2.10'

options:
  tables:
    description:
    - Names of tables that needs to be vacuumed.
    type: list
  skip_period:
    description:
    - Skip tables that were vacuumed / autovacuumed / analyzed / autoanalyzed in this period (in seconds).
    - Mutually exclusive with I(full).
    type: int
  analyze_only:
    description:
    - Only analyze, no vacuum. Mutually exclusive with I(analyze).
    type: bool
    default: no
  full:
    description:
    - Rebuild a table.
    - Mutually exclusive with I(analyze_only).
    type: bool
    default: no
  additional_args:
    description:
    - String of additional parameters.
    type: str
  db:
    description:
    - Name of a database for vacuuming (used as a database to connect to).
    type: str
    aliases: [ login_db ]
  session_role:
    description:
    - Switch to session_role after connecting.
      The specified session_role must be a role that the current login_user is a member of.
    - Permissions checking for SQL commands is carried out as though
      the session_role were the one that had logged in originally.
    type: str

seealso:
- module: postgresql_db
- name: PostgreSQL vacuum reference
  description: Complete reference of the PostgreSQL vacuum documentation.
  link: https://www.postgresql.org/docs/current/sql-vacuum.html

notes:
- Supports since PostgreSQL 9.4.
- Check mode makes sense only with I(skip_period), otherwise it always returns changed True.

author:
- Andrew Klychkov (@Andersson007)

extends_documentation_fragment: postgres
'''

EXAMPLES = r'''
- name: >
    Vacuum all tables in the database acme
    that have not been vacuumed / autovacuumed during the last hour
  postgresql_vacuum:
    db: acme
    skip_period: 3600

- name: Vacuum and analyze database acme
  postgresql_vacuum:
    db: acme
    additional_args: analyze

- name: >
    Vacuum full and analyze the database foo.
    Pay attention that the database will be locked
  postgresql_vacuum:
    db: foo
    full: yes
    additional_args: analyze

- name: >
    Analyze the table mytable in the database bar
    if it has not been analyzed over the last minute
  postgresql_vacuum:
    db: bar
    tables: mytable
    analyze_only: yes
    skip_period: 60

- name: Vacuum and analyze two tables in the database acme
  postgresql_vacuum:
    db: acme
    tables: mytable, my_another_table
    analyze: yes

- name: Vacuum of mytable with freeze
  postgresql_vacuum:
    db: acme
    tables mytable
    additional_args: freeze

- name: Vacuum of mytable with disabled page skipping and analyze
  postgresql_vacuum:
    db: acme
    tables: mytable
    additional_args: disable_page_skipping analyze
'''

RETURN = r'''
queries:
  description: List of executed queries.
  returned: always
  type: str
  sample: [ "VACUUM (ANALYZE) mytable" ]
'''

from datetime import datetime

try:
    from psycopg2.extras import DictCursor
except ImportError:
    # psycopg2 is checked by connect_to_db()
    # from ansible.module_utils.postgres
    pass

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.database import pg_quote_identifier
from ansible.module_utils.postgres import (
    connect_to_db,
    exec_sql,
    get_conn_params,
    postgres_common_argument_spec,
)

DISABLE_PAGE_SKIPPING_VER = 96000


def transform_tables_representation(tbl_list):
    """Add 'public.' to names of tables where a schema identifier is absent
    and add quotes to each element.

    Args:
        tbl_list (list): List of table names.

    Returns:
        tbl_list (list): Changed list.
    """
    for i, table in enumerate(tbl_list):
        if '.' not in table:
            tbl_list[i] = pg_quote_identifier('public.%s' % table.strip(), 'table')
        else:
            tbl_list[i] = pg_quote_identifier(table.strip(), 'table')

    return tbl_list


class Vacuum():
    """Implements VACUUM [FULL] and/or ANALYZE PostgreSQL command behavior.

    args:
        module (AnsibleModule) -- object of AnsibleModule class
        cursor (cursor) -- cursor object of psycopg2 library

    attrs:
        module (AnsibleModule) -- object of AnsibleModule class
        cursor (cursor) -- cursor object of psycopg2 library
        changed (bool) --  something was changed after execution or not
        executed_queries (list) -- executed queries
        query_frag (list) -- list for making SQL query
        stat_user_tables (list) -- list contains tables statistics
    """

    def __init__(self, module, cursor):
        self.module = module
        self.cursor = cursor
        self.executed_queries = []
        self.changed = False
        self.query_frag = []
        self.stat_user_tables = self.get_user_tables_stat()

    def get_user_tables_stat(self):
        """Get tables statistics."""
        query = ("SELECT schemaname, relname, "
                 "last_vacuum AT TIME ZONE 'UTC', "
                 "last_autovacuum AT TIME ZONE 'UTC', "
                 "last_analyze AT TIME ZONE 'UTC', "
                 "last_autoanalyze AT TIME ZONE 'UTC' "
                 "FROM pg_stat_user_tables")
        return exec_sql(self, query, add_to_executed=False)

    def __check_table(self, tblname):
        """
        Check the table that need to be vacuumed/analyzed exist.
        """
        tmp = tblname.split('.')
        table_schema = tmp[-2].strip('"')
        table_name = tmp[-1].strip('"')

        query = ("SELECT 1 FROM information_schema.tables "
                 "WHERE table_schema = '%s' "
                 "AND table_name = '%s'" % (table_schema, table_name))

        if not exec_sql(self, query, add_to_executed=False):
            msg = 'table %s in schema %s does not exist' % (table_name, table_schema)
            self.module.fail_json(msg=msg)

        # If all objects exists, return True
        return True

    def do(self, tbl_list):

        tmp_tbl_list = []

        if tbl_list:
            for tbl in tbl_list:
                self.__check_table(tbl)

        opt_list = []

        if self.module.params.get('analyze_only'):
            opt_list.append('ANALYZE')

        elif self.module.params.get('full'):
            opt_list.append('VACUUM_FULL')

        else:
            opt_list.append('VACUUM')

        if self.module.params.get('additional_args'):
            opt_list.append(self.module.params['additional_args'])

        for row in self.stat_user_tables:
            schema = row[0]
            table = row[1]
            last_analyze = 0
            last_autoanalyze = 0
            last_vacuum = 0
            last_autovacuum = 0

            if tbl_list:
                tbl_name = '"%s"."%s"' % (schema, table)
                if tbl_name not in tbl_list:
                    continue

            if not self.module.params.get('full') and self.module.params.get('skip_period'):
                skip_tstamp = (datetime.utcnow() - datetime(1970,1,1)).total_seconds() - self.module.params['skip_period']

                if self.module.params.get('analyze_only'):
                    if row[5]:
                        last_analyze = (row[5] - datetime(1970,1,1)).total_seconds()

                    if row[6]:
                        last_autoanalyze = (row[6] - datetime(1970,1,1)).total_seconds()

                    if skip_tstamp > last_analyze or skip_tstamp > last_autoanalyze:
                        continue
                    else:
                        if self.module.check_mode:
                            self.changed = True
                else:
                    if row[3]:
                        last_vacuum = (row[3] - datetime(1970,1,1)).total_seconds()

                    if row[4]:
                        last_autovacuum = (row[4] - datetime(1970,1,1)).total_seconds()

                    if skip_tstamp > last_vacuum or skip_tstamp > last_autovacuum:
                        continue
                    else:
                        if self.module.check_mode:
                            self.changed = True

            if tbl_list:
                tmp_tbl_list.append('"%s"."%s"' % (schema, table))

        if tmp_tbl_list:
            opt_list.append(', '.join(tmp_tbl_list))

        query = ' '.join(opt_list)

        if self.module.check_mode:
            if not self.module.params.get('skip_period'):
                self.changed = True

            self.executed_queries.append(query)
            return None

        if exec_sql(self, query, ddl=True):
            self.changed = True

# ===========================================
# Module execution.
#


def main():
    argument_spec = postgres_common_argument_spec()
    argument_spec.update(
        tables=dict(type='list'),
        skip_period=dict(type='int'),
        analyze_only=dict(type='bool'),
        additional_args=dict(type='str'),
        db=dict(type='str', aliases=['login_db']),
        session_role=dict(type='str'),
    )
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        mutually_exclusive=[
            ['full', 'analyze_only'],
            ['full', 'skip_period'],
        ]
    )

    # Connect to DB and make cursor object:
    # (autocommit=True because VACUUM/ANALYZE cannot run inside a transaction block)
    conn_params = get_conn_params(module, module.params)
    db_connection = connect_to_db(module, conn_params, autocommit=True)

    cursor = db_connection.cursor(cursor_factory=DictCursor)

    ##############
    # Create the object and do main job:
    vacuum = Vacuum(module, cursor)

    # Note: parameters are got
    # from module object into data object of Vacuum class.
    # Therefore not need to pass args to the methods below.
    # Note: check mode is implemented inside the methods below
    # by checking passed module.check_mode arg.

    tbl_list = None
    if module.params.get('tables'):
        tbl_list = transform_tables_representation(module.params['tables'])

    vacuum.do(tbl_list)

    # Clean up:
    cursor.close()
    db_connection.close()

    # Return values:
    module.exit_json(
        changed=vacuum.changed,
        queries=vacuum.executed_queries,
    )


if __name__ == '__main__':
    main()
