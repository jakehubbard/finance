import pandas
import warnings
import sys
import re
from sqlalchemy import create_engine

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Retrieve PostGreSQL Credentials
sqlcred = open("/home/jake/onedrive/code/database/postgres-cred.txt", "r").read().replace('\n','')
sqlcred = re.split('Host: |Database: |User: |Port: |Password: ',sqlcred)
user = sqlcred[3]
password = sqlcred[5]
host = sqlcred[1]
port = sqlcred[4]
database = sqlcred[2]

# Create Connections to Databases
postgres = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
sqlite = create_engine('sqlite://///home/jake/onedrive/code/database/finance.db')
pandas.set_option('display.max_rows',None)

def clean_income():
    warnings.simplefilter('ignore')

    # Connect to PostGreSQL Database
    try:
        postgres_income_df = pandas.read_sql_query('SELECT * FROM income ORDER BY id', postgres, index_col='id')
    except:
        print(bcolors.FAIL + bcolors.BOLD + '\nUnable to connect to PostGreSQL database. Please check your credentials in ~/onedrive/code/database/postgres-cred.txt' + bcolors.ENDC)
        if input('\nDo you wish to continue anyway? (y/n): ').lower() != 'y':
            exit()

    # Create Income DataFrames
    income_cols = ['date_id','date','name','amount','trans']
    clean_income_df = pandas.DataFrame(columns = income_cols)
    removed_income_df = pandas.DataFrame(columns = income_cols)
    sqlite_income_df = pandas.read_sql_query('SELECT * FROM income', sqlite, index_col='id')
    try:
        mint_df = pandas.read_csv(r'/home/jake/onedrive/finances/bank/raw/mint.csv')
        clean_income_df = clean_income_df.append(mint_df[income_cols])
    except FileNotFoundError:
        pass

    # Check if PostGreSQL Database equals SQLite Database
    # if sqlite_income_df.equals(postgres_income_df) == False:
    #     if input(bcolors.FAIL + bcolors.BOLD + '\nThe Income tables do NOT match. Do you wish to continue anyway? (y) '+ bcolors.ENDC).lower() != 'y':
    #         print(bcolors.FAIL + bcolors.BOLD + '\nOperation cancelled.\n' + bcolors.ENDC)
    #         exit()

    # Grab Data from Additional Files
    for i in range(1, len(sys.argv)):
        filepath = r'/home/jake/onedrive/finances/bank/raw/'+sys.argv[i]+'.csv'
        raw_df = pandas.read_csv(filepath)

        # Format columns
        for c in raw_df.columns:
            if c == 'Transaction Date' or c == 'Date' or c == 'Posting Date':
                raw_df['date'] = raw_df[c]
                continue
            if 'Description' in c and 'Merchant' not in raw_df.columns:
                raw_df['name'] = raw_df[c]
                continue
            if 'Merchant' in c:
                raw_df['name'] = raw_df[c]
                continue
            if 'Amount' in c:
                raw_df['amount'] = raw_df[c]
                continue
            if c == 'Category':
                raw_df['cat'] = raw_df[c]
                continue
            if 'Type' in c:
                raw_df['trans'] = raw_df[c]
                continue

        # Adds Cat and Subcat Column if neccessary
        if 'cat' not in raw_df.columns:
            raw_df['cat'] = 'None'
        raw_df['subcat'] = 'None'

        # Remove negative amount values
        raw_df.amount = raw_df.amount.abs()

        # Converts date column to datetime
        raw_df.date = pandas.to_datetime(raw_df.date)

        # Creates date_id from date column
        raw_df.date = raw_df.date.astype('str')
        month_converter = {'01':'Jan','02':'Feb','03':'Mar','04':'Apr','05':'May','06':'Jun','07':'Jul','08':'Aug','09':'Sep','10':'Oct','11':'Nov','12':'Dec'}
        raw_df['date_id'] = raw_df.date.str[5:7].apply(lambda x: month_converter[x])+'-'+raw_df.date.str[2:4]

        # Format Clean DataFrame Columns
        clean_income_df = clean_income_df.append(raw_df[income_cols])

    # CLEANING INCOME
    # Formatting Name Income
    clean_income_df.name[clean_income_df.name.str.contains('johnson controls',case=False)] = 'JOHNSON CONTROLS'
    clean_income_df.name[clean_income_df.name=='OKC WEB_PAY'] = 'Water Bill'

    # Formatting Transaction Income
    clean_income_df.trans[clean_income_df.name.str.contains('Reimbursement',case=False)] = 'Reimbursement'
    clean_income_df.trans[clean_income_df.trans=='ACCT_XFER'] = 'Payment/Transfer'
    clean_income_df.trans[(clean_income_df.name.str.contains('payment thank you',case=False) | clean_income_df.name.str.contains('internet transfer',case=False) | clean_income_df.name.str.contains('Future Amount:',case=False) | clean_income_df.name.str.contains('online transfer',case=False) | clean_income_df.name.str.contains('applecard gsbank payment',case=False) | clean_income_df.name.str.contains('epay',case=False) | clean_income_df.name.str.contains('overdraft transfer',case=False))] = 'Payment/Transfer'
    clean_income_df.trans[(clean_income_df.trans=='credit') & ~((clean_income_df.name.str.contains('johnson controls',case=False)) | (clean_income_df.name=='Internal Revenue Service') | (clean_income_df.name=='eCheck Deposit') | (clean_income_df.name=='Interest Paid') | (clean_income_df.name.str.contains('apple cash',case=False)) | (clean_income_df.name=='Wire Transfer') | (clean_income_df.name=='Venmo Cashout') | (clean_income_df.name.str.contains('ATM CASH DEPOSIT',case=False)))] = 'Return'
    clean_income_df.trans[clean_income_df.trans=='Sale'] = 'Purchase'
    clean_income_df.trans[clean_income_df.trans=='Withdrawal'] = 'Purchase'
    clean_income_df.trans[clean_income_df.trans=='ACH_CREDIT'] = 'Deposit'
    clean_income_df.trans[clean_income_df.trans=='credit'] = 'Deposit'
    clean_income_df.trans[clean_income_df.trans=='debit'] = 'Purchase'

    # REMOVING INCOME ROWS
    removed_income_df = removed_income_df.append(clean_income_df.loc[(clean_income_df.trans=='Payment/Transfer')])
    removed_income_df = removed_income_df.append(clean_income_df.loc[~(clean_income_df.trans=='Deposit')])
    removed_income_df = removed_income_df.append(clean_income_df.loc[clean_income_df.name.str.contains('reimbursement',case=False)])
    removed_income_df = removed_income_df.append(clean_income_df.loc[clean_income_df.name.str.contains('vanguard',case=False)])
    removed_income_df = removed_income_df.append(clean_income_df.loc[clean_income_df.name.str.contains('fid bkg svc',case=False)])
    removed_income_df = removed_income_df.append(clean_income_df.loc[clean_income_df.name.str.contains('robinhood',case=False)])

    clean_income_df = clean_income_df.loc[~(clean_income_df.trans=='Payment/Transfer')]
    clean_income_df = clean_income_df.loc[clean_income_df.trans=='Deposit']
    clean_income_df = clean_income_df.loc[~(clean_income_df.name.str.contains('reimbursement',case=False))]
    clean_income_df = clean_income_df.loc[~(clean_income_df.name.str.contains('fid bkg svc',case=False))]
    clean_income_df = clean_income_df.loc[~(clean_income_df.name.str.contains('vanguard',case=False))]
    clean_income_df = clean_income_df.loc[~(clean_income_df.name.str.contains('robinhood',case=False))]

    # RESET INDEX AND SORT DATES
    clean_income_df.sort_values(by=['date'], ascending=False, inplace=True)
    clean_income_df.reset_index(drop=True, inplace=True)
    removed_income_df.sort_values(by=['trans'], ascending=True, inplace=True)
    removed_income_df.reset_index(drop=True, inplace=True)

    # Count the amount of rows removed
    removed_income_rows = removed_income_df.name.count()

    print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- CLEANED INCOME DATA ----------------------------------------\n' + bcolors.ENDC)
    print(clean_income_df)
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n---------------------------------------- REMOVED {removed_income_rows} ROWS FROM INCOME ----------------------------------------\n' + bcolors.ENDC)
    print(removed_income_df)
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n--------------------------------------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)
    # MAKE EXCEPTIONS
    while True:
        exc = input(bcolors.BOLD + '\nMake exception? ' + bcolors.ENDC)
        if exc != '':
            try:
                removed_income_df.loc[int(exc),'trans'] = 'Deposit'
                clean_income_df = clean_income_df.append(removed_income_df.iloc[int(exc)])
                removed_income_df = removed_income_df.drop(removed_income_df.index[int(exc)])
                clean_income_df.sort_values(by=['date'], ascending=False, inplace=True)
                clean_income_df.reset_index(drop=True, inplace=True)
                removed_income_df.sort_values(by=['trans'], ascending=True, inplace=True)
                removed_income_df.reset_index(drop=True, inplace=True)
            except:
                print(bcolors.FAIL + bcolors.BOLD + 'Error: The row could not be appended to the Data.' + bcolors.ENDC)
                continue
            print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- CLEANED INCOME DATA ----------------------------------------\n' + bcolors.ENDC)
            print(clean_income_df)
            print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- REMOVED INCOME DATA ----------------------------------------\n' + bcolors.ENDC)
            print(removed_income_df)
            print(bcolors.BOLD + bcolors.OKBLUE + f'\n----------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)
        else:
            break

    # ANY OVERLAPS
    print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- NEW INCOME DATA ----------------------------------------\n' + bcolors.ENDC)
    print(clean_income_df.tail(10))
    print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- INCOME DATABASE ----------------------------------------\n' + bcolors.ENDC)
    print(sqlite_income_df.tail(10).sort_values(by=['date'], ascending=False))
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)

    while True:
        ovr = input(bcolors.BOLD + '\nAny overlap? ' + bcolors.ENDC)
        if ovr != '':
            try:
                clean_income_df = clean_income_df.drop(clean_income_df.index[int(ovr)])
                clean_income_df.sort_values(by=['date'], ascending=False, inplace=True)
                clean_income_df.reset_index(drop=True, inplace=True)
            except:
                print(bcolors.FAIL + bcolors.BOLD + 'Error: Could not remove the overlap.' + bcolors.ENDC)
            print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- NEW INCOME DATA ----------------------------------------\n' + bcolors.ENDC)
            print(clean_income_df.tail(10))
            print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- INCOME DATABASE ----------------------------------------\n' + bcolors.ENDC)
            print(sqlite_income_df.tail(10).sort_values(by=['date'], ascending=False))
            print(bcolors.BOLD + bcolors.OKBLUE + f'\n------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)
        else:
            break

    # INSERTING DATA
    print(bcolors.BOLD + bcolors.OKBLUE + '\n------------------------------ INCOME DATA TO BE INSERTED INTO DATABASE ------------------------------\n' + bcolors.ENDC)
    print(clean_income_df)
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n-----------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)

    prompt = input(bcolors.BOLD + '\nConfirm insert of income data to database? (' + bcolors.ENDC + bcolors.OKGREEN + bcolors.BOLD + 'y' + bcolors.ENDC + bcolors.BOLD + ') ' + bcolors.ENDC)
    if prompt.lower() == 'y':
        clean_income_df.sort_values(by=['date'], ascending=True, inplace=True)
        print('\nConnecting to PostGreSQL database...')
        try:
            clean_income_df.to_sql('income', postgres, if_exists='append',index=False)
        except:
            print(bcolors.FAIL + bcolors.BOLD + '\nUnable to connect to PostGreSQL database. Data write has been CANCELLED.' + bcolors.ENDC)
            exit()
        clean_income_df.to_sql('income', sqlite, if_exists='append',index=False)
        print(bcolors.OKGREEN + bcolors.BOLD + '\nIncome data inserted successfully.\n' + bcolors.ENDC)
    else:
        print(bcolors.FAIL + bcolors.BOLD + '\nData write has been CANCELLED.\n' + bcolors.ENDC)
        exit()

clean_income()
