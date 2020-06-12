#Jake's Financial Tools
import pandas
import numpy
import sys
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

pandas.set_option('display.max_rows',None)
sqlite = create_engine('sqlite://///home/jake/onedrive/code/database/finance.db')
dfs = pandas.read_sql_query('SELECT * FROM spending', sqlite)
dfi = pandas.read_sql_query('SELECT * FROM income', sqlite)

months = ['Jan-','Feb-','Mar-','Apr-','May-','Jun-','Jul-','Aug-','Sep-','Oct-','Nov-','Dec-']

def report(month):
    global months
    global dfs
    global dfi
    df = dfs
    cur_sum = []
    prev_sum = []
    pct_change = []
    pct_sp = []
    pct_in = []

    # Finds previous month
    prev_index = months.index(month.title()[0:4]) - 1
    if prev_index != -1:
        prev_month = months[prev_index] + month[-2:]
    else:
        prev_month = months[prev_index] + str(int(month[-2:])-1)

    # Prints Summary
    print('')
    print(bcolors.BOLD + bcolors.OKBLUE + '       ' + month.title() + ' Report\n' + bcolors.ENDC)
    ovallspend = int(df.amount[(df.date_id==month.title()) & ~((df.trans=='Return') | (df.trans=='Reimbursement'))].sum() - df.amount[(df.date_id==month.title()) & ((df.trans=='Return') | (df.trans=='Reimbursement'))].sum())
    prevovallspend = int(df.amount[(df.date_id==prev_month.title()) & ~((df.trans=='Return') | (df.trans=='Reimbursement'))].sum() - df.amount[(df.date_id==prev_month.title()) & ((df.trans=='Return') | (df.trans=='Reimbursement'))].sum())
    ovallincome = int(dfi.amount[dfi.date_id==month.title()].sum())
    prevovallincome = int(dfi.amount[dfi.date_id==prev_month.title()].sum())
    net = ovallincome - ovallspend
    prevnet = prevovallincome - prevovallspend
    spenddif = int(ovallspend - prevovallspend)
    incomedif = int(ovallincome - prevovallincome)
    netdif = int(net - prevnet)
    if spenddif > 0:
        spenddif = f'+{spenddif:.0f}'
    if incomedif > 0:
        incomedif = f'+{incomedif:.0f}'
    if netdif > 0:
        netdif = f'+{netdif:.0f}'
    dsum = {'':[ovallincome,ovallspend,net],'  ':['|','|','|'],'𝜟 ($)':[incomedif,spenddif,netdif]}
    summaryreportdf = pandas.DataFrame(dsum,index=['Income','Spending','Net'])
    #print(bcolors.BOLD + bcolors.OKBLUE + "--------------------------------------------------" + bcolors.ENDC)
    print(summaryreportdf)

    # Grabs Spending Totals for current and previous month
    for category in df.cat.unique():
        cur_sum.append(int(df.amount[(df.cat==category) & (df.date_id==month.title()) & ~((df.trans=='Return') | (df.trans=='Reimbursement'))].sum() - df.amount[(df.date_id==month.title()) & (df.cat==category) & ((df.trans=='Return') | (df.trans=='Reimbursement'))].sum()))
        prev_sum.append(df.amount[(df.cat==category) & (df.date_id==prev_month.title()) & ~((df.trans=='Return') | (df.trans=='Reimbursement'))].sum() - df.amount[(df.date_id==prev_month.title()) & (df.cat==category) & ((df.trans=='Return') | (df.trans=='Reimbursement'))].sum())
        pct_sp.append(int((cur_sum[-1]/ovallspend)*100))
        pct_in.append(int((cur_sum[-1]/ovallincome)*100))

    change = [int(num) for num in list((numpy.array(cur_sum) - numpy.array(prev_sum)))]
    numpy.seterr(divide='ignore', invalid='ignore')
    for chng,psum in zip(change,prev_sum):
        x = (numpy.array(chng)/numpy.array(psum))*100
        if numpy.isinf(x):
            x = numpy.nan
        if not numpy.isnan(x):
            pct_change.append(int(round(x)))
        else:
            pct_change.append('N/A')

    d = {'Category':df.cat.unique().tolist(),'% Income': pct_in,'% Spending':pct_sp,'Spent':cur_sum,'𝜟 $':change,'𝜟 %':pct_change}
    reportdf = pandas.DataFrame(d)
    reportdf.sort_values(by=['% Spending'], ascending=False, inplace=True)
    reportdf.set_index('Category',inplace=True)
    print(bcolors.BOLD + bcolors.OKBLUE + "--------------------------------------------------" + bcolors.ENDC)
    print(reportdf)
    print('')

report(sys.argv[1])
