import pandas
import warnings
import sys
#from pathlib import Path
from config import host, password, database, user, port, sqlite_directory, mint_directory, raw_data_directory
from sqlalchemy import create_engine

class bcolors:
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    WARNING = '\033[93m'
    UNDERLINE = '\033[4m'

# Stores user's home directory
#home = str(Path.home())

def match(iterable,match_dict,df,column,filter):
     for i in iterable:
        if match_dict.get(i):
            df[column][df[filter]==i] = match_dict.get(i)

def match_containables(iterable,match_dict,df,column,filter):
     for i in match_dict:
        if str(iterable).find(i):
            df[column][df[filter].str.contains(i,case=False)] = i

def spending_report(sqlite_spending_df,spending_df):
    import statistics
    import datetime
    from calendar import monthrange,month_name

    def color_fn(current,median):
        if (current/median) >= 1.25:
            color = bcolors.FAIL
        elif (current/median) < 1.25 and (current/median) > 0.8:
            color = bcolors.WARNING
        else:
            color = bcolors.OKGREEN
        return color
    
    sums = []
    misc = []
    shopping = []
    restaurant = []
    grocery = []
    bills = []
    travel = []
    medical = []
    home = []
    charity = []
    retirement = []
    cat_bag = [misc,shopping,restaurant,grocery,bills,travel,medical,home,charity,retirement]
    months = sqlite_spending_df.date_id.unique()[-3:]
    for category,l in zip(sqlite_spending_df.cat.unique(),cat_bag):
        for month in months:
            l.append(
            sqlite_spending_df.amount[(sqlite_spending_df.date_id==month) & (sqlite_spending_df.cat==category) & (sqlite_spending_df.trans=='Purchase')].sum()
            - sqlite_spending_df.amount[(sqlite_spending_df.date_id==month) & (sqlite_spending_df.cat==category) & (sqlite_spending_df.trans=='Return')].sum()
            )
    for month in months:
        sums.append(
            sqlite_spending_df.amount[(sqlite_spending_df.date_id==month) & (sqlite_spending_df.trans=='Purchase')].sum()
            - sqlite_spending_df.amount[(sqlite_spending_df.date_id==month) & (sqlite_spending_df.trans=='Return')].sum()
            )
    
    print(bcolors.BOLD+bcolors.OKBLUE+bcolors.UNDERLINE+'\n\nSPENDING REPORT\n'+bcolors.ENDC)
    print(bcolors.OKBLUE+bcolors.BOLD+f'{(datetime.date.today().day/monthrange(datetime.date.today().year,datetime.date.today().month)[1])*100:.0f}%'+bcolors.ENDC+f' of {month_name[datetime.date.today().month]} has passed')

    sums = statistics.median(sums)
    total = spending_df.amount[spending_df.trans=='Purchase'].sum() - spending_df.amount[spending_df.trans=='Return'].sum()
    print(bcolors.BOLD+'\nTOTAL SPENDING ('+bcolors.ENDC+color_fn(total,sums)+bcolors.BOLD+f'{(total/sums)*100:.0f}%'+bcolors.ENDC+bcolors.BOLD+')'+bcolors.ENDC)
    print(f'Current ${total:,.0f}')
    print(f'Median ${sums:,.0f}\n')

    for category,i in zip(sqlite_spending_df.cat.unique(),cat_bag):
        cat_med = statistics.median(i)
        current = spending_df.amount[(spending_df.cat==category) & (spending_df.trans=='Purchase')].sum() - spending_df.amount[(spending_df.cat==category) & (spending_df.trans=='Return')].sum()
        if cat_med != 0:
            print(bcolors.BOLD+f'{category.upper()} ('+bcolors.ENDC+color_fn(current,cat_med)+bcolors.BOLD+f'{(current/cat_med)*100:.0f}%'+bcolors.ENDC+bcolors.BOLD+')'+bcolors.ENDC)
        else:
            print(bcolors.BOLD+f'{category.upper()} ('+bcolors.ENDC+'No data'+bcolors.BOLD+')'+bcolors.ENDC)
        print(f'Current ${current:,.0f}')
        print(f'Median ${cat_med:,.0f}\n')
    
    input(bcolors.BOLD+'\nPRESS ANY KEY TO EXIT'+bcolors.ENDC)

def exception(df,removed_df,table):
    while True:
        exc = input(bcolors.BOLD + '\nMake exception for index: ' + bcolors.ENDC)
        if exc != '':
            try:
                removed_df.loc[int(exc),'trans'] = 'Deposit'
                df = df.append(removed_df.iloc[int(exc)])
                removed_df = removed_df.drop(removed_df.index[int(exc)])
                df.sort_values(by=['date'], ascending=False, inplace=True)
                df.reset_index(drop=True, inplace=True)
                removed_df.sort_values(by=['trans'], ascending=True, inplace=True)
                removed_df.reset_index(drop=True, inplace=True)
                print(bcolors.BOLD + bcolors.OKBLUE + f'\n---------------------------------------- REMOVED ROWS FROM {table.upper()} -------------------------------\n' + bcolors.ENDC)
                print(removed_df)
                print(bcolors.BOLD + bcolors.OKBLUE + f'\n--------------------------- {table.upper()} DATA TO WRITE --------------------------------\n' + bcolors.ENDC)
                print(df)
                print(bcolors.BOLD + bcolors.OKBLUE + f'\n-----------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)
            except:
                print(bcolors.FAIL + bcolors.BOLD + 'Error: The row could not be appended to the data.' + bcolors.ENDC)
                continue
        else:
            return df,removed_df

def overlap(df,sqlite_df,table):
    while True:
        ovr = input(bcolors.BOLD + '\nRemove overlap at index: ' + bcolors.ENDC)
        if ovr != '':
            try:
                df = df.drop(df.index[int(ovr)])
                df.sort_values(by=['date'], ascending=False, inplace=True)
                df.reset_index(drop=True, inplace=True)
                print(bcolors.BOLD + bcolors.OKBLUE + f'\n--------------------------- {table.upper()} DATA TO WRITE --------------------------------\n' + bcolors.ENDC)
                print(df)
                print(bcolors.BOLD + bcolors.OKBLUE + f'\n---------------------------------------- {table.upper()} DATABASE --------------------------------------------\n' + bcolors.ENDC)
                print(sqlite_df.tail(10).sort_values(by=['date'], ascending=False))
                print(bcolors.BOLD + bcolors.OKBLUE + f'\n-----------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)
            except:
                print(bcolors.FAIL + bcolors.BOLD + 'Error: Could not remove the overlap.' + bcolors.ENDC)
        else:
            return df

def write(df,table):
    df.sort_values(by=['date'], ascending=True, inplace=True)
    try:
        df.to_sql(table, postgres, if_exists='append',index=False)
    except:
        print(bcolors.FAIL + bcolors.BOLD + '\nUnable to connect to PostGreSQL database. No data was written.' + bcolors.ENDC)
        exit()
    df.to_sql(table, sqlite, if_exists='append',index=False)
    print(bcolors.OKGREEN + bcolors.BOLD + '\nData successfully written.\n' + bcolors.ENDC)

def prompt(table,removed_rows,removed_df,df,sqlite_df):
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n---------------------------------------- REMOVED {removed_rows} ROWS FROM {table.upper()} -------------------------------------\n' + bcolors.ENDC)
    print(removed_df)
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n--------------------------------------- {table.upper()} DATA TO WRITE ---------------------------------------------\n' + bcolors.ENDC)
    print(df)
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n----------------------------------------- {table.upper()} DATABASE ------------------------------------------------\n' + bcolors.ENDC)
    print(sqlite_df.tail(10).sort_values(by=['date'], ascending=False))
    print(bcolors.BOLD + bcolors.OKBLUE + f'\n------------------------------------------------------------------------------------------------------------\n' + bcolors.ENDC)

    if table == 'spending':
        code = input('1) '+bcolors.BOLD+bcolors.OKGREEN+'Write data'+bcolors.ENDC+'\n2) '+bcolors.BOLD+bcolors.OKBLUE+'Make exception'+bcolors.ENDC+'\n3) '+bcolors.BOLD+bcolors.OKBLUE+'Remove overlap'+bcolors.ENDC+' \n4) '+bcolors.BOLD+bcolors.OKBLUE+'Spending Report'+bcolors.ENDC+'\n )'+bcolors.BOLD+bcolors.FAIL+' Cancel'+bcolors.ENDC+'\n\n ')
    else:
        code = input('1) '+bcolors.BOLD+bcolors.OKGREEN+'Write data'+bcolors.ENDC+'\n2) '+bcolors.BOLD+bcolors.OKBLUE+'Make exception'+bcolors.ENDC+'\n3) '+bcolors.BOLD+bcolors.OKBLUE+'Remove overlap'+bcolors.ENDC+' \n )'+bcolors.BOLD+bcolors.FAIL+' Cancel'+bcolors.ENDC+'\n\n ')
    if code.lower() == '1':
        write(df,table)
    elif code == '2':
        df,removed_df = exception(df,removed_df,table)
        prompt(table,removed_rows,removed_df,df,sqlite_df)
    elif code == '3':
        df = overlap(df,sqlite_df,table)
        prompt(table,removed_rows,removed_df,df,sqlite_df)
    elif code == '4' and table == 'spending':
        spending_report(sqlite_spending_df,spending_df)
        prompt(table,removed_rows,removed_df,df,sqlite_df)
    else:
        print(bcolors.FAIL+bcolors.BOLD+'\nNo data written.'+bcolors.ENDC)

# Create Connections to Databases
postgres = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
sqlite = create_engine('sqlite:////' + sqlite_directory)
pandas.set_option('display.max_rows',None)
pandas.set_option('display.max_columns', 1000)
pandas.set_option('display.width', 1000)

# Turn off warnings from Pandas
warnings.simplefilter('ignore')

raw_df_cols = ['date_id','date','name','amount','cat','subcat','trans']
raw_df = pandas.DataFrame(columns=raw_df_cols)

# Grab Data from CSV Files
for i in range(1, len(sys.argv)):
    csv_df = pandas.read_csv(raw_data_directory+sys.argv[i]+'.csv')

    # Format columns in CSV Files
    for c in csv_df.columns:
        if c == 'Transaction Date' or c == 'Date' or c == 'Posting Date':
            csv_df['date'] = csv_df[c]
            continue
        if 'Description' in c and 'Merchant' not in csv_df.columns:
            csv_df['name'] = csv_df[c]
            continue
        if 'Merchant' in c:
            csv_df['name'] = csv_df[c]
            continue
        if 'Amount' in c:
            csv_df['amount'] = csv_df[c]
            continue
        if c == 'Category':
            csv_df['cat'] = csv_df[c]
            continue
        if 'Type' in c:
            csv_df['trans'] = csv_df[c]
            continue

    # Adds Cat and Subcat Column if neccessary
    if 'cat' not in csv_df.columns:
        csv_df['cat'] = 'None'
    csv_df['subcat'] = 'None'

    # Remove negative amount values
    csv_df.amount = csv_df.amount.abs()

    # Converts date column to datetime
    csv_df.date = pandas.to_datetime(csv_df.date)

    # Creates date_id from date column
    csv_df.date = csv_df.date.astype('str')
    month_converter = {'01':'Jan','02':'Feb','03':'Mar','04':'Apr','05':'May','06':'Jun','07':'Jul','08':'Aug','09':'Sep','10':'Oct','11':'Nov','12':'Dec'}
    csv_df['date_id'] = csv_df.date.str[5:7].apply(lambda x: month_converter[x])+'-'+csv_df.date.str[2:4]
    raw_df = raw_df.append(csv_df[raw_df_cols])

# Append the Mint Data to the Raw DF
try:
    mint_df = pandas.read_csv(mint_directory)
    raw_df = raw_df.append(mint_df[raw_df_cols])
except FileNotFoundError:
    pass

# Grab raw column sets
raw_name = set(raw_df.name.unique())
raw_cat = set(raw_df.cat.unique())
raw_trans = set(raw_df.trans.unique())

# DICTIONARIES 
name_by_name = {
    'OKC WEB_PAY':'Water Bill',
    'OK NATURAL GAS UTIL PAYMT':'Natural Gas Bill',
    'FID BKG SVC LLC MONEYLINE':'Fidelity'
    }

name_by_name_containables = {'JOHNSON CONTROLS','OG&E','CASH DEPOSIT','CHASE CREDIT CRD EPAY','PAYMENT THANK YOU','INTERNET TRANSFER','APPLECARD GSBANK PAYMENT'}

cat_by_name = {
    'R&J TECHNOLOGY':'Shopping',
    'THE GREENHOUSE':'Shopping',
    'LIVE WELL 30A':'Shopping',
    'Prime Video Ms':'Shopping',
    'PetSmart':'Shopping',
    "Casey's":'Travel',
    '7-Eleven':'Travel',
    "Breck's":'Travel',
    'KwikShop':'Travel',
    'Chase Travel':'Travel',
    'Wal-Mart':'Grocery',
    'TRI OCEAN':'Grocery',
    'Compassion International':'Charity',
    'Wildwood Communi Wildwood C':'Charity',
    'Ranchwood Veterinary Hospital Inc':'Medical',
    'Fidelity':'Retirement',
    'GREENSMASTER LAWN':'Bills',
    'Greensmaster Lawn':'Bills',
    'Spotify':'Bills',
    'COX OKLAHOMA COMM SV':'Bills',
    'Venmo':'Misc',
    'A1 Pet Emporium':'Grocery',
    'Chipotle Mexican Grill':'Restaurant',
}

cat_by_cat = {
    'restaurants':'Restaurant',
    'food & dining':'Restaurant',
    'coffee shops':'Restaurant',
    'fast food':'Restaurant',
    'Food & Drink':'Restaurant',
    'education':'Shopping',
    'entertainment':'Shopping',
    'shopping':'Shopping',
    'internet':'Shopping',
    'electronics & software':'Shopping',
    'movies & dvds':'Shopping',
    'clothing':'Shopping',
    'hair':'Shopping',
    'gift':'Shopping',
    'lawn & garden':'Shopping',
    'amusement':'Shopping',
    'tuition':'Shopping',
    'personal Care':'Shopping',
    'business services':'Shopping',
    'sporting goods':'Shopping',
    'Other':'Shopping',
    'kids activities':'Shopping',
    'arts':'Shopping',
    'home phone':'Bills',
    'home services':'Bills',
    'mortgage & rent':'Bills',
    'federal tax':'Bills',
    'taxes':'Bills',
    'utilities':'Bills',
    'gym':'Bills',
    'auto insurance':'Bills',
    'hotel':'Travel',
    'gas & fuel':'Travel',
    'travel':'Travel',
    'atm fee':'Misc',
    'check':'Misc',
    'COLLEGE TRANSCRIPT':'Misc',
    'home improvement':'Home',
    'alcohol & bars':'Grocery',
    'groceries':'Grocery',
    'pet food & supplies':'Grocery',
}

subcat_by_name = {
    'OG&E':'Electric Bill',
    'Natural Gas Bill':'Gas Bill',
    'Spotify':'Music Bill',
    'Water Bill':'Water Bill',
    'YMCA':'Gym Bill',
    'AT&T':'Phone Bill',
    'COX OKLAHOMA COMM SV':'Internet Bill',
    'GREENSMASTER LAWN':'Weed Control',
    'Greensmaster Lawn':'Weed Control',
    'ALTA PEST CONTROL OKC':'Pest Control',
    'State Farm':'Insurance',
    'Wildwood Communi Wildwood C':'Church',
    'LIVE WELL 30A':'Rentals',
    'TRI OCEAN':'Fish Market',
    "Casey's":'Gas',
    '7-Eleven':'Gas',
    'KwikShop':'Gas',
    'Country Place DUES':'Fees',
    'Steam Games':'Games',
    'THE GREENHOUSE':'Plants',
    "Breck's":'Parking',
    'R&J TECHNOLOGY':'Electronics',
    'Udemy':'Education',
    'The Hall''s Kitchen LLC':'Sit Down',
    'Prime Video Ms':'TV',
    'Ranchwood Veterinary Hospital Inc':'Pet',
    'PetSmart':'Pet',
    'A1 Pet Emporium':'Pet',
    'Chipotle Mexican Grill':'Fast Food',
}

subcat_by_cat = {
    'restaurants':'Sit Down',
    'alcohol & bars':'Alcohol',
    'hotel':'Hotel',
    'check':'Check',
    'entertainment':'Entertainment',
    'clothing':'Clothing',
    'hair':'Haircut',
    'sporting goods':'Recreation',
    'kids activities':'Recreation',
    'mortgage & rent':'Mortgage',
    'food & dining':'Fast Food',
    'coffee shops':'Fast Food',
    'fast food':'Fast Food',
    'gas & fuel':'Gas',
    'atm fee':'Fees',
    'pets':'Pet',
    'pet food & supplies':'Pet',
    'amusement':'Games',
    'lawn & garden':'Plants',
    'federal tax':'Taxes',
    'taxes':'Taxes',
    'movies & dvds':'TV',
    'electronics & software':'Electronics',
    'education':'Education',
    'personal Care':'Personal Care',
    'arts':'Decor',
}

trans_by_trans = {
    'ACCT_XFER':'Payment/Transfer',
    'Sale':'Purchase',
    'debit':'Purchase',
    'Withdrawal':'Purchase',
    'ACH_CREDIT':'Deposit',
    'credit':'Deposit'
}

trans_by_name_containables = {'Reimbursement'}

# FILTER NAME BY NAME
match(raw_name,name_by_name,raw_df,'name','name')

# FILTER NAME BY NAME CONTAINABLES
match_containables(raw_name,name_by_name_containables,raw_df,'name','name')

# REMOVE ALL PAYMENTS AND TRANSFERS FROM RAW DF
raw_df = raw_df.loc[~((raw_df.name=='PAYMENT THANK YOU') | (raw_df.name=='CHASE CREDIT CRD EPAY') | (raw_df.name=='INTERNET TRANSFER') | (raw_df.name=='APPLECARD GSBANK PAYMENT'))]

# RESET NAMES FOR BETTER FILTERING
clean_name = set(raw_df.name.unique())

# FILTER CAT BY NAME
match(clean_name,cat_by_name,raw_df,'cat','name')

# FILTER SUBCAT BY NAME
match(clean_name,subcat_by_name,raw_df,'subcat','name')

# FILTER SUBCAT BY CAT
match(raw_cat,subcat_by_cat,raw_df,'subcat','cat')

# FILTER CAT BY CAT
match(raw_cat,cat_by_cat,raw_df,'cat','cat')

# TRANS BY TRANS
match(raw_trans,trans_by_trans,raw_df,'trans','trans')

# TRANS BY NAME CONTAINABLES
match_containables(clean_name,trans_by_name_containables,raw_df,'trans','name')

# RETURNS
raw_df.trans[(raw_df.trans=='Deposit') & ~((raw_df.name=='JOHNSON CONTROLS') | (raw_df.name=='Internal Revenue Service') | (raw_df.name=='eCheck Deposit') | (raw_df.name=='Interest Paid') | (raw_df.name.str.contains('apple cash',case=False)) | (raw_df.name=='Wire Transfer') | (raw_df.name=='Venmo Cashout') | (raw_df.name=='CASH DEPOSIT'))] = 'Return'

# Create Income DataFrames
income_cols = ['date_id','date','name','amount','trans']
income_df = pandas.DataFrame(columns = income_cols)
removed_income_df = pandas.DataFrame(columns = income_cols)
sqlite_income_df = pandas.read_sql_query('SELECT * FROM income', sqlite, index_col='id')
income_df = income_df.append(raw_df[income_cols])

# Create Spending DataFrames
spending_df = pandas.DataFrame(columns = raw_df_cols)
removed_spending_df = pandas.DataFrame(columns = raw_df_cols)
sqlite_spending_df = pandas.read_sql_query('SELECT * FROM spending', sqlite, index_col='id')
spending_df = spending_df.append(raw_df)


# REMOVING INCOME ROWS
removed_income_df = removed_income_df.append(income_df.loc[(income_df.name=='Fidelity') | (income_df.name.str.contains('vanguard',case=False)) | (income_df.name.str.contains('robinhood',case=False))])
removed_income_df = removed_income_df.append(income_df.loc[~(income_df.trans=='Deposit')])

income_df = income_df.loc[~((income_df.name.str.contains('reimbursement',case=False)) | (income_df.name=='Fidelity') | (income_df.name.str.contains('vanguard',case=False)) | (income_df.name.str.contains('robinhood',case=False)))]
income_df = income_df.loc[income_df.trans=='Deposit']

# RESET INDEX AND SORT DATES
income_df.sort_values(by=['date'], ascending=False, inplace=True)
income_df.reset_index(drop=True, inplace=True)
removed_income_df.sort_values(by=['trans'], ascending=True, inplace=True)
removed_income_df.reset_index(drop=True, inplace=True)

# Count the amount of rows removed
removed_income_rows = removed_income_df.name.count()

# DISPLAY
prompt('income',removed_income_rows,removed_income_df,income_df,sqlite_income_df)

## REMOVING SPENDING ROWS 
removed_spending_df = removed_spending_df.append(spending_df.loc[spending_df.trans=='Deposit'])

spending_df = spending_df.loc[~(spending_df.trans=='Deposit')]

# RESET INDEX AND SORT DATES
spending_df.sort_values(by=['date'], ascending=False, inplace=True)
spending_df.reset_index(drop=True, inplace=True)
removed_spending_df.sort_values(by=['trans'], ascending=True, inplace=True)
removed_spending_df.reset_index(drop=True, inplace=True)

# Count the amount of rows removed
removed_spending_rows = removed_spending_df.name.count()

prompt('spending',removed_spending_rows,removed_spending_df,spending_df,sqlite_spending_df)