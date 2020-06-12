import mintfork
from getpass import getpass
import sys

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

try:
    month = sys.argv[1].title()
except IndexError:
    print(bcolors.BOLD + bcolors.FAIL + '\nYou forgot to tell me what month to grab! Try again.\n' + bcolors.ENDC)
    exit(1)

username = 'Jake.Alan.Hubbard@gmail.com'
print(bcolors.OKBLUE + bcolors.BOLD + "\nMint Login" + bcolors.ENDC)
print(bcolors.OKBLUE + bcolors.BOLD + '\nUsername: ' + bcolors.ENDC + f'{username}\n')
password = getpass(bcolors.OKBLUE + bcolors.BOLD + 'Password: ' + bcolors.ENDC)
print('\nConnecting to Mint...')

mint = mintfork.Mint(
    username,
    password,
    mfa_method='sms',
    headless=False,
)

mint_df = mint.get_transactions()

# Creates date_id from date column
mint_df.date = mint_df.date.astype('str')
month_converter = {'01':'Jan','02':'Feb','03':'Mar','04':'Apr','05':'May','06':'Jun','07':'Jul','08':'Aug','09':'Sep','10':'Oct','11':'Nov','12':'Dec'}
mint_df['date_id'] = mint_df.date.str[5:7].apply(lambda x: month_converter[x])+'-'+mint_df.date.str[2:4]

# Formats columns
mint_df['name'] = mint_df['description']
mint_df['trans'] = mint_df['transaction_type']
mint_df['cat'] = mint_df['category']
mint_df['subcat'] = 'None'
col = ['date_id','date','name','amount','cat','subcat','trans']
mint_df = mint_df[col]

mint_df = mint_df.loc[mint_df.date_id==month]
mint_df.to_csv(r'/home/jake/onedrive/finances/bank/raw/mint.csv', index=False)
mint.close()
print(bcolors.OKGREEN + bcolors.BOLD + '\nMint data successfully extracted.' + bcolors.ENDC)
