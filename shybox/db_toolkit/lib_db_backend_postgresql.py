"""
Library Features:

Name:          lib_db_backend_postgresql
Author(s):     Fabio Delogu (fabio.delogu@cimafoundation.org)
Date:          '20261125'
Version:       '1.0.0'
"""

# ----------------------------------------------------------------------------------------------------------------------
# method to connect to postgreSQL
def connect(database, *, username: str, password: str):

    import psycopg

    host = database.get('host', 'localhost')
    port = database.get('port', 5432)
    timeout = database.get('connect_timeout', 15)
    try:
        return psycopg.connect(
            host=host, port=port, dbname=database['name'],
            user=username, password=password, connect_timeout=timeout,
        )
    except psycopg.errors.ConnectionTimeout as exc:
        exc.add_note(f'PostgreSQL did not respond at {host}:{port} within {timeout} seconds.')
        if host in ('127.0.0.1', 'localhost', '::1'):
            exc.add_note('If using an SSH tunnel, check the local forwarding port and the remote database address.')
        raise
# ----------------------------------------------------------------------------------------------------------------------
