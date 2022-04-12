import sqlite3, os

DEFAULT_PATH = os.path.join(os.path.dirname(__file__), 'eos_actions.db')

def db_connect(db_path=DEFAULT_PATH):
    con = sqlite3.connect(db_path)
    return con

dbcon = db_connect()
cur = dbcon.cursor()

accounts_table = '''
    CREATE TABLE IF NOT EXISTS accounts (
        account_id text PRIMARY KEY
        )'''

cur.execute(accounts_table)

actions_table = '''
    CREATE TABLE IF NOT EXISTS actions (
        global_action_seq integer PRIMARY KEY,
        account_action_seq integer NOT NULL,
        block_num integer NOT NULL,
        block_time text NOT NULL,
        trx_id text NOT NULL,
        type text NOT NULL,
        sender text,
        receiver text,
        quantity real,
        currency text,
        memo text,
        query text,
        FOREIGN KEY(query) REFERENCES accounts(account_id)
        )'''


cur.execute(actions_table)

cur.execute('''

    CREATE VIEW IF NOT EXISTS last_actions
    AS
    select query account, max(account_action_seq) last_action_seq
    from actions
    group by query
    ''')
