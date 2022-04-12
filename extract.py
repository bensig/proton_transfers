import requests
import json
import time
import pandas as pd

from models import dbcon

node_url = 'https://proton.greymass.com'

class Account_Transfers:
    def __init__(self, account):
        self.account = account

    def extract_json(self, pos, offset):
        url = '{}/v1/history/get_actions'.format(node_url)
        params = {'account_name': self.account,
                'pos': pos, 'offset': offset}#, 'filter-on':'receiver:action:actor'}

        r = requests.request("POST", url, json=params)
        d = json.loads(r.text)

        df = Account_Transfers.parse_account_json(self, d)
        #df.to_csv('test.csv', index=False)
        return df

    #store in SQLite DB
    def store(self):
        print ('{} extracting...'.format(self.account))
        #like actual from cleos
        last_action_actual = int(Account_Transfers.last_account_action(self))
        last_action_db = int(Account_Transfers.get_last_action_db(self))


        #this is where i need to fix the thing
        while last_action_db <= last_action_actual:
            if last_action_actual == last_action_db:
                print ('    no new actions')
                break

            if last_action_db == 0:
                last_action_db = 0
            else:
                last_action_db = last_action_db + 1


            #think if I adjust the extract_json last param it will go faster
            df = Account_Transfers.extract_json(self, last_action_db, +100)
            df = df.drop_duplicates(subset=['global_action_seq','trx_id', 'quantity', 'memo'])
            df.to_sql('actions', dbcon, if_exists='append', index=False)
            last_action_db = int(Account_Transfers.get_last_action_db(self))

        #make this shit better
        dbcon.cursor().execute(
            '''
            DELETE  from actions
            WHERE   rowid not in
                (
                select  min(rowid)
                from    actions
                group by trx_id, quantity, memo
                )
            ''')

        dbcon.commit()

    #Get last account action so we loop up to there
    def last_account_action(self):
        last_action = Account_Transfers.extract_json(self, -1, -1)#['actions']['account_action_seq']

        return last_action['account_action_seq'][0]


    def get_last_action_db(self):
        try:
            last_action = pd.read_sql('''SELECT last_action_seq from last_actions
                where account = "{}"'''.format(self.account), dbcon)['last_action_seq'].iloc[0]
            print ('    last_seq = {}'.format(last_action))
            return last_action

        except IndexError:
            try:
                print ('{} is a new account'.format(account))
                dbcon.cursor().execute('''INSERT INTO accounts (account_id) VALUES ('{}')
                                    '''.format(account))
                dbcon.commit()
                return 0

            except:
                return 0

    #ETL json
    def parse_account_json(self, account_json):
        data = account_json
        row_data = []
        for a in data['actions']:
            try:
                row = {
                'global_action_seq': a['global_action_seq'],
                'account_action_seq': a['account_action_seq'],
                'block_num': a['block_num'],
                'block_time': a['block_time'],
                'trx_id': a['action_trace']['trx_id'],
                'type': a['action_trace']['act']['name'],
                'sender': a['action_trace']['act']['data']['from'],
                'receiver': a['action_trace']['act']['data']['to'],
                'quantity': a['action_trace']['act']['data']['quantity'],
                'memo': a['action_trace']['act']['data']['memo'],
                }
            except (KeyError, TypeError):
                row = {
                'global_action_seq': a['global_action_seq'],
                'account_action_seq': a['account_action_seq'],
                'block_num': a['block_num'],
                'block_time': a['block_time'],
                'trx_id': a['action_trace']['trx_id'],
                'type': a['action_trace']['act']['name'],
                }

            row_data.append(row)
        df = pd.DataFrame(row_data)
        df = df.reindex(columns = ['global_action_seq', 'account_action_seq', 'block_num', 'block_time',
                'trx_id', 'type', 'sender', 'receiver', 'quantity', 'currency',  'memo', 'query'])


        df['query'] = self.account

        df['currency'] = df['quantity'].apply(lambda x: str(x).split(' ')[1]
                                                if pd.notnull(x) else x)

        def quantity_clean(row):
            if pd.notnull(row['quantity']):
                quantity = float(str(row['quantity'].split(' ')[0]))
            #return quantity
            if row['query'] == row['sender']:
                return -quantity

            elif row['query'] == row['receiver']:
                return quantity
            else:
                try:
                    return float(str(row['quantity'].split(' ')[0]))
                except:
                    pass

        df['quantity'] = df.apply(quantity_clean, axis=1)
        df = df[['global_action_seq', 'account_action_seq', 'block_num', 'block_time',
                'trx_id', 'type', 'sender', 'receiver', 'quantity', 'currency',  'memo', 'query']]

        #print (df)
        return df


    def export_to_csv(self):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        extract_df = pd.read_sql("SELECT * from actions where type='transfer' and query='{}'".format(self.account), dbcon)
        extract_df['block_time'] = pd.to_datetime(extract_df['block_time'])

        #ensures query is a receiver or sender, sometimes it includes other accounts
        extract_df['filter'] = extract_df.apply(lambda row: 1 if row['query'] == row['sender'] or row['query'] == row['receiver'] else 0, axis=1)

        extract_df = extract_df[extract_df['filter']==1]
        extract_df.to_csv('{}_extract_{}.csv'.format(self.account, timestr), index=False)
        print ('{} exported to csv'.format(self.account))

'''
test = Account_Transfers('sheos21sheos')

test.export_to_csv()
'''
