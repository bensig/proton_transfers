import sys, time
from extract import Account_Transfers


def app():
    sys.argv[0]
    
    user_input = input('enter accounts comma separated: ')
    account_lst = user_input.replace(' ','').split(',')
    print ('You selected: {} \n'.format(account_lst))

    for account in account_lst:
        target_account = Account_Transfers(account.strip())
        target_account.store()
        target_account.export_to_csv()
        time.sleep(6)
        print ('{} extracted! \n'.format(account))


try:
    app()
except IndexError:
    #raise
    print ('recheck if you entered the right account names')
