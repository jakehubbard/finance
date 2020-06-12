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

def clean_spending():
    warnings.simplefilter('ignore')

    # Connect to PostGreSQL Database
    try:
        postgres_spending_df = pandas.read_sql_query('SELECT * FROM spending ORDER BY id', postgres, index_col='id')
    except:
        print(bcolors.FAIL + bcolors.BOLD + '\nUnable to connect to PostGreSQL database. Please check your credentials in ~/onedrive/code/database/postgres-cred.txt' + bcolors.ENDC)
        if input('\nDo you wish to continue anyway? (y/n): ').lower() != 'y':
            exit()

    # Create Spending DataFrames
    spending_cols = ['date_id','date','name','amount','cat','subcat','trans']
    clean_spending_df = pandas.DataFrame(columns = spending_cols)
    removed_spending_df = pandas.DataFrame(columns = spending_cols)
    sqlite_spending_df = pandas.read_sql_query('SELECT * FROM spending', sqlite, index_col='id')
    try:
        mint_df = pandas.read_csv(r'/home/jake/onedrive/finances/bank/raw/mint.csv')
        clean_spending_df = clean_spending_df.append(mint_df[spending_cols])
    except FileNotFoundError:
        pass

    # Check if PostGreSQL Database equals SQLite Database
    # if sqlite_spending_df.equals(postgres_spending_df) == False:
    #     if input(bcolors.FAIL + bcolors.BOLD + '\nThe Spending tables do NOT match. Do you wish to continue anyway? (y) ' + bcolors.ENDC).lower() != 'y':
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
        clean_spending_df = clean_spending_df.append(raw_df[spending_cols])

    # CLEANING SPENDING
    # Formatting Name Spending
    clean_spending_df.name[clean_spending_df.name.str.contains('VESTA')] = 'AT&T'
    clean_spending_df.name[clean_spending_df.name.str.contains('OKC WEB_PAY')] = 'WATER BILL'
    clean_spending_df.name[clean_spending_df.name.str.contains('AMZN Mktp')] = 'AMAZON'
    clean_spending_df.name[clean_spending_df.name.str.contains('OG&E')] = 'OG&E'
    clean_spending_df.name[clean_spending_df.name.str.contains('PROVIDENT FUNDIN ACH PMT')] = 'MORTGAGE PAYMENT'
    clean_spending_df.name[clean_spending_df.name.str.contains('OK NATURAL GAS UTIL PAYMT')] = 'NATURAL GAS BILL'

    # Formatting Subcategory Spending
    clean_spending_df.subcat[clean_spending_df.name.str.contains('MORTGAGE PAYMENT',case=False)] = 'Mortgage'
    clean_spending_df.subcat[clean_spending_df.name.str.contains('fid bkg svc llc moneyline',case=False)] = 'Roth IRA'
    clean_spending_df.subcat[clean_spending_df.name.str.contains('og&e',case=False)] = 'Electric Bill'
    clean_spending_df.subcat[clean_spending_df.name.str.contains('NATURAL GAS BILL',case=False)] = 'Gas Bill'
    clean_spending_df.subcat[clean_spending_df.cat=='Gas'] = 'Gas'
    clean_spending_df.subcat[clean_spending_df.cat=='Education'] = 'Education'
    clean_spending_df.subcat[clean_spending_df.name.str.contains('insurance',case=False)] = 'Insurance'
    clean_spending_df.subcat[clean_spending_df.name.str.contains('cox oklahoma',case=False)] = 'Internet Bill'
    clean_spending_df.subcat[clean_spending_df.name.str.contains('spotify',case=False)] = 'Music Bill'
    clean_spending_df.subcat[clean_spending_df.name.str.contains('water bill',case=False)] = 'Water Bill'
    clean_spending_df.subcat[clean_spending_df.name.str.contains('ymca',case=False)] = 'Gym'
    clean_spending_df.subcat[clean_spending_df.name.str.contains('at&t',case=False)] = 'Phone Bill'
    clean_spending_df.subcat[clean_spending_df.name=='Alta Pest Control OKC'] = 'Pest Control'
    clean_spending_df.subcat[clean_spending_df.cat=='fast food'] = 'Fast Food'
    clean_spending_df.subcat[clean_spending_df.cat=='restaurants'] = 'Sit Down'
    clean_spending_df.subcat[clean_spending_df.cat=='food & dining'] = 'Fast Food'
    clean_spending_df.subcat[clean_spending_df.cat=='coffee shops'] = 'Fast Food'
    clean_spending_df.subcat[clean_spending_df.cat=='mortgage & rent'] = 'Mortgage'
    clean_spending_df.subcat[clean_spending_df.cat=='alcohol & bars'] = 'Alcohol'
    clean_spending_df.subcat[clean_spending_df.cat=='hotel'] = 'Hotel'
    clean_spending_df.subcat[clean_spending_df.cat=='amusement'] = 'Games'
    clean_spending_df.subcat[clean_spending_df.cat=='federal tax'] = 'Taxes'
    clean_spending_df.subcat[clean_spending_df.cat=='check'] = 'Check'
    clean_spending_df.subcat[clean_spending_df.cat=='entertainment'] = 'Entertainment'
    clean_spending_df.subcat[clean_spending_df.cat=='clothing'] = 'Clothing'
    clean_spending_df.subcat[clean_spending_df.cat=='atm fee'] = 'Fees'
    clean_spending_df.subcat[clean_spending_df.cat=='gas & fuel'] = 'Gas'
    clean_spending_df.subcat[clean_spending_df.cat=='movies & dvds'] = 'TV'
    clean_spending_df.subcat[clean_spending_df.cat=='electronics & software'] = 'Electronics'
    clean_spending_df.subcat[clean_spending_df.cat=='taxes'] = 'Taxes'
    clean_spending_df.subcat[clean_spending_df.cat=='hair'] = 'Haircut'
    clean_spending_df.subcat[clean_spending_df.name=="Country Place DUES"] = 'Fees'
    clean_spending_df.subcat[clean_spending_df.name=="Udemy"] = 'Education'
    clean_spending_df.subcat[clean_spending_df.name=="Casey's"] = 'Gas'
    clean_spending_df.subcat[clean_spending_df.name=="State Farm"] = 'Insurance'
    clean_spending_df.subcat[clean_spending_df.name=="7-Eleven"] = 'Gas'
    clean_spending_df.subcat[clean_spending_df.name=="Wildwood Communi Wildwood C"] = 'Church'
    clean_spending_df.subcat[clean_spending_df.name=="R&J TECHNOLOGY"] = 'Electronics'
    clean_spending_df.subcat[clean_spending_df.name=="GREENSMASTER LAWN"] = 'Weed Control'
    clean_spending_df.subcat[clean_spending_df.name=="ALTA PEST CONTROL OKC"] = 'Pest Control'
    clean_spending_df.subcat[clean_spending_df.name=="COX OKLAHOMA COMM SV"] = 'Internet Bill'
    clean_spending_df.subcat[clean_spending_df.name=="Steam Games"] = 'Games'
    clean_spending_df.subcat[clean_spending_df.name=="A1 Pet Emporium"] = 'Pet'
    clean_spending_df.subcat[clean_spending_df.name=="LIVE WELL 30A"] = 'Bikes'

    # Formatting Category Spending
    clean_spending_df.cat[clean_spending_df.cat.str.contains('restaurants',case=False)] = 'Restaurant'
    clean_spending_df.cat[clean_spending_df.cat=='Education'] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.cat=='home phone'] = 'Bills'
    clean_spending_df.cat[clean_spending_df.cat=='shopping'] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.cat=='hotel'] = 'Travel'
    clean_spending_df.cat[clean_spending_df.cat=='gas & fuel'] = 'Travel'
    clean_spending_df.cat[clean_spending_df.cat=='atm fee'] = 'Misc'
    clean_spending_df.cat[clean_spending_df.cat=='check'] = 'Misc'
    clean_spending_df.cat[clean_spending_df.cat=='entertainment'] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.cat=='home services'] = 'Bills'
    clean_spending_df.cat[clean_spending_df.cat=='mortgage & rent'] = 'Bills'
    clean_spending_df.cat[clean_spending_df.cat=='internet'] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.cat=='electronics & software'] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.cat=='food & dining'] = 'Restaurant'
    clean_spending_df.cat[clean_spending_df.cat=='coffee shops'] = 'Restaurant'
    clean_spending_df.cat[clean_spending_df.cat=='fast food'] = 'Restaurant'
    clean_spending_df.cat[clean_spending_df.cat=='federal tax'] = 'Bills'
    clean_spending_df.cat[clean_spending_df.cat=='home improvement'] = 'Home'
    clean_spending_df.cat[clean_spending_df.cat=='alcohol & bars'] = 'Grocery'
    clean_spending_df.cat[clean_spending_df.cat=='Gas'] = 'Travel'
    clean_spending_df.cat[clean_spending_df.cat=='movies & dvds'] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.cat=='taxes'] = 'Bills'
    clean_spending_df.cat[clean_spending_df.cat=='travel'] = 'Travel'
    clean_spending_df.cat[clean_spending_df.cat=='Food & Drink'] = 'Restaurant'
    clean_spending_df.cat[clean_spending_df.cat=='Gifts & Donations'] = 'Charity'
    clean_spending_df.cat[clean_spending_df.cat.str.contains('Groceries',case=False)] = 'Grocery'
    clean_spending_df.cat[clean_spending_df.cat=='Bills & Utilities'] = 'Bills'
    clean_spending_df.cat[clean_spending_df.cat=='clothing'] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.cat=='hair'] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.cat=='amusement'] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.name.str.contains('MORTGAGE PAYMENT',case=False)] = 'Bills'
    clean_spending_df.cat[clean_spending_df.name.str.contains('fid bkg svc llc moneyline',case=False)] = 'Retirement'
    clean_spending_df.cat[clean_spending_df.name.str.contains('og&e',case=False)] = 'Bills'
    clean_spending_df.cat[clean_spending_df.name.str.contains('NATURAL GAS BILL',case=False)] = 'Bills'
    clean_spending_df.cat[clean_spending_df.name.str.contains('venmo',case=False)] = 'Misc'
    clean_spending_df.cat[clean_spending_df.name.str.contains('ymca',case=False)] = 'Bills'
    clean_spending_df.cat[clean_spending_df.name.str.contains('country place dues',case=False)] = 'Bills'
    clean_spending_df.cat[clean_spending_df.name.str.contains('water bill',case=False)] = 'Bills'
    clean_spending_df.cat[clean_spending_df.name=='Spotify'] = 'Bills'
    clean_spending_df.cat[clean_spending_df.name=="Casey's"] = 'Travel'
    clean_spending_df.cat[clean_spending_df.name=="ULTA"] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.name=="State Farm"] = 'Bills'
    clean_spending_df.cat[clean_spending_df.name=="Udemy"] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.name=="Young Living"] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.name=="Wal-Mart"] = 'Grocery'
    clean_spending_df.cat[clean_spending_df.name=="Compassion International"] = 'Charity'
    clean_spending_df.cat[clean_spending_df.name=="Wildwood Communi Wildwood C"] = 'Charity'
    clean_spending_df.cat[clean_spending_df.name=="7-Eleven"] = 'Travel'
    clean_spending_df.cat[clean_spending_df.name=="COX OKLAHOMA COMM SV"] = 'Bills'
    clean_spending_df.cat[clean_spending_df.name=="R&J TECHNOLOGY"] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.name=="GREENSMASTER LAWN"] = 'Bills'
    clean_spending_df.cat[clean_spending_df.name=="COLLEGE TRANSCRIPT"] = 'Misc'
    clean_spending_df.cat[clean_spending_df.name=="A1 Pet Emporium"] = 'Grocery'
    clean_spending_df.cat[clean_spending_df.name=="Dick's Sporting Goods"] = 'Shopping'
    clean_spending_df.cat[clean_spending_df.name=="LIVE WELL 30A"] = 'Travel'

    # Formatting Transaction Spending
    clean_spending_df.trans[clean_spending_df.name.str.contains('Reimbursement',case=False)] = 'Reimbursement'
    clean_spending_df.trans[clean_spending_df.trans=='ACCT_XFER'] = 'Payment/Transfer'
    clean_spending_df.trans[(clean_spending_df.name.str.contains('payment thank you',case=False) | clean_spending_df.name.str.contains('internet transfer',case=False) | clean_spending_df.name.str.contains('Future Amount:',case=False) | clean_spending_df.name.str.contains('online transfer',case=False) | clean_spending_df.name.str.contains('applecard gsbank payment',case=False) | clean_spending_df.name.str.contains('epay',case=False) | clean_spending_df.name.str.contains('overdraft transfer',case=False))] = 'Payment/Transfer'
    clean_spending_df.trans[(clean_spending_df.trans=='credit') & ~((clean_spending_df.name.str.contains('johnson controls',case=False)) | (clean_spending_df.name=='Internal Revenue Service') | (clean_spending_df.name=='eCheck Deposit') | (clean_spending_df.name=='Interest Paid') | (clean_spending_df.name.str.contains('apple cash',case=False)) | (clean_spending_df.name=='Wire Transfer') | (clean_spending_df.name=='Venmo Cashout') | (clean_spending_df.name.str.contains('ATM CASH DEPOSIT',case=False)))] = 'Return'
    clean_spending_df.trans[clean_spending_df.trans=='Sale'] = 'Purchase'
    clean_spending_df.trans[clean_spending_df.trans=='Withdrawal'] = 'Purchase'
    clean_spending_df.trans[clean_spending_df.trans=='ACH_CREDIT'] = 'Deposit'
    clean_spending_df.trans[clean_spending_df.trans=='credit'] = 'Deposit'
    clean_spending_df.trans[clean_spending_df.trans=='debit'] = 'Purchase'

    # REMOVING SPENDING ROWS
    removed_spending_df = removed_spending_df.append(clean_spending_df.loc[(clean_spending_df.trans=='Deposit') | (clean_spending_df.trans=='Payment/Transfer')])

    clean_spending_df = clean_spending_df.loc[~((clean_spending_df.trans=='Deposit') | (clean_spending_df.trans=='Payment/Transfer'))]

    # RESET INDEX AND SORT DATES
    clean_spending_df.sort_values(by=['date'], ascending=False, inplace=True)
    clean_spending_df.reset_index(drop=True, inplace=True)
    removed_spending_df.sort_values(by=['trans'], ascending=True, inplace=True)
    removed_spending_df.reset_index(drop=True, inplace=True)

    # Count the amount of rows removed
    removed_spending_rows = removed_spending_df.name.count()

    # MAKE EXCEPTIONS
    print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- CLEANED SPENDING DATA ----------------------------------------\n' + bcolors.ENDC)
    print(clean_spending_df)
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n---------------------------------------- REMOVED {removed_spending_rows} ROWS FROM SPENDING ----------------------------------------\n' + bcolors.ENDC)
    print(removed_spending_df)
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n------------------------------------------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)

    while True:
        exc = input(bcolors.BOLD + '\nMake exception? ' + bcolors.ENDC)
        if exc != '':
            try:
                clean_spending_df = clean_spending_df.append(removed_spending_df.iloc[int(exc)])
                removed_spending_df = removed_spending_df.drop(removed_spending_df.index[int(exc)])
                clean_spending_df.sort_values(by=['date'], ascending=False, inplace=True)
                clean_spending_df.reset_index(drop=True, inplace=True)
                removed_spending_df.sort_values(by=['trans'], ascending=True, inplace=True)
                removed_spending_df.reset_index(drop=True, inplace=True)
            except:
                print(bcolors.FAIL + bcolors.BOLD + 'Error: The row could not be appended to the Data.' + bcolors.ENDC)
                continue
            print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- CLEANED SPENDING DATA ----------------------------------------\n' + bcolors.ENDC)
            print(clean_spending_df)
            print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- REMOVED SPENDING DATA ----------------------------------------\n' + bcolors.ENDC)
            print(removed_spending_df)
            print(bcolors.BOLD + bcolors.OKBLUE + f'\n------------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)
        else:
            break

    # ANY OVERLAPS
    print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- NEW SPENDING DATA ----------------------------------------\n' + bcolors.ENDC)
    print(clean_spending_df.tail(10))
    print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- SPENDING DATABASE ----------------------------------------\n' + bcolors.ENDC)
    print(sqlite_spending_df.tail(10).sort_values(by=['date'], ascending=False))
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n--------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)

    while True:
        ovr = input(bcolors.BOLD + '\nAny overlap? ' + bcolors.ENDC)
        if ovr != '':
            try:
                clean_spending_df = clean_spending_df.drop(clean_spending_df.index[int(ovr)])
                clean_spending_df.sort_values(by=['date'], ascending=False, inplace=True)
                clean_spending_df.reset_index(drop=True, inplace=True)
            except:
                print(bcolors.FAIL + bcolors.BOLD + 'Error: Could not remove the overlap.' + bcolors.ENDC)
            print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- NEW SPENDING DATA ----------------------------------------\n' + bcolors.ENDC)
            print(clean_spending_df.tail(10))
            print(bcolors.BOLD + bcolors.OKBLUE + '\n---------------------------------------- SPENDING DATABASE ----------------------------------------\n' + bcolors.ENDC)
            print(sqlite_spending_df.tail(10).sort_values(by=['date'], ascending=False))
            print(bcolors.BOLD + bcolors.OKBLUE + f'\n--------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)
        else:
            break

    # INSERTING DATA
    print(bcolors.BOLD + bcolors.OKBLUE + '\n------------------------------ SPENDING DATA TO BE INSERTED INTO DATABASE ------------------------------\n' + bcolors.ENDC)
    print(clean_spending_df)
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n-------------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)

    prompt = input(bcolors.BOLD + '\nConfirm insert of spending data to database? (' + bcolors.ENDC + bcolors.OKGREEN + bcolors.BOLD + 'y' + bcolors.ENDC + bcolors.BOLD + ') ' + bcolors.ENDC)
    if prompt.lower() == 'y':
        clean_spending_df.sort_values(by=['date'], ascending=True, inplace=True)
        print('\nConnecting to PostGreSQL database...')
        try:
            clean_spending_df.to_sql('spending', postgres, if_exists='append',index=False)
        except:
            print(bcolors.FAIL + bcolors.BOLD + '\nUnable to connect to PostGreSQL database. Data write has been CANCELLED.' + bcolors.ENDC)
            exit()
        clean_spending_df.to_sql('spending', sqlite, if_exists='append',index=False)
        print(bcolors.OKGREEN + bcolors.BOLD + '\nSpending data inserted successfully.\n' + bcolors.ENDC)
    else:
        print(bcolors.FAIL + bcolors.BOLD + '\nData write has been CANCELLED.\n' + bcolors.ENDC)
        exit()

clean_spending()
