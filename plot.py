#Jake's Financial Tools
import matplotlib.pyplot as plt
import pandas
import numpy
import os
import sys
import operator
import statistics
from sqlalchemy import create_engine
pandas.set_option('display.max_rows',None)
sqlite = create_engine('sqlite://///home/jake/onedrive/code/database/finance.db')
dfs = pandas.read_sql_query('SELECT * FROM spending', sqlite)
dfi = pandas.read_sql_query('SELECT * FROM income', sqlite)
if sys.argv[1].lower() == 'sp':
    df = dfs
elif sys.argv[1].lower() == 'in':
    df = dfi
else:
    print('\nError: No dataframe was specified. Please use "plot sp" or "plot in".\n')
    exit()

months = ['Jan-','Feb-','Mar-','Apr-','May-','Jun-','Jul-','Aug-','Sep-','Oct-','Nov-','Dec-']

def plot(df,by,search,op='&',by2='s',search2='',start_date='may-18',end_date='feb-20',linecolor='black',ylbl=20,xtick=12,ytick=18,title=25,lnwid=4.5,xplot=20,yplot=9,avgwid=3.5):
    """
    df: Dataframe you want to plot. (Required)
    by: Filter Dataframe according to specified column (Required)
    search: Search criteria (Required)
    op: and/or opertor for multiple search criteria (AND=&, OR = |) (Default:'&')
    by2: Filter Dataframe according to specified column for second search criteria (Default:'s')
    search2: Second search criteria (Default: blank)
    start_date: Specify when to start graph. (Default:May-18)
    end_date: Specify when to end graph. (Default Feb-20)
    linecolor: Specify HEX color of line (Default:Black)
    ylbl: Adjust size of Y-Axis font (Default:20)
    xtick: Adjust size of X-Axis ticks (Default:12)
    ytick: Adjust size of Y_Axis ticks (Default:18)
    title: Adjust size of Title (Default:25)
    lnwid: Adjust width of line plot (Default:4.5)
    xplot: Adjust horizontal size of plot (Default:20)
    yplot: Adjust vertical size of plot (Default:9)
    avgwid: Adjust Width of the average line (Default:2.5)
    """
    global months
    global dfs
    global dfi
    x_vals = []
    y_vals = []
    start_year = int(start_date[4:6])
    end_year = int(end_date[4:6])

    # Define the operator
    op_dict = {'&':operator.and_,'|':operator.or_}
    op = op_dict[op]

    # Reads the filter protocol
    by_dict = {'n':'name','c':'cat','s':'subcat','t':'trans'}
    by = by_dict[by]
    by2 = by_dict[by2]

    # Pulls data from selected dataframe and sorts by date
    if search == 'SPENDING':
        heading = 'Overall Spending'
        gdf = dfs.copy()
    elif search == 'INCOME':
        heading = 'Overall Income'
        gdf = dfi.copy()
    elif search == 'NET':
        heading = 'Net Income'
        gdf = dfs.copy()
        gdfi = dfi.copy()
    elif search2 == '':
        heading = search.title()
        gdf = df.loc[df[by].str.contains(search,case=False),{'date','date_id','amount','trans'}].copy()
    elif search2[0] == '~':
        search2 = search2[1:]
        heading = f'{search} - {search2}'.title()
        gdf = df.loc[op((df[by].str.contains(search,case=False)),(~df[by2].str.contains(search2,case=False))),{'date','date_id','amount','trans'}].copy()
    else:
        heading = f'{search} + {search2}'.title()
        gdf = df.loc[op((df[by].str.contains(search,case=False)),(df[by2].str.contains(search2,case=False))),{'date','date_id','amount','trans'}].copy()

    # Sort by Date
    gdf.sort_values(by=['date'],ascending=True,inplace=True)

    # Remove Reimbursements
    gdf = gdf.loc[~(gdf.trans=='Reimbursement')]

    # Create x_vals
    for year in range(start_year,end_year+1):
        for month in months:
            x_vals.append(month+str(year))

    # Remove Months not selected
    while x_vals[0].lower() != start_date.lower():
        x_vals.pop(0)

    while x_vals[-1].lower() != end_date.lower():
        x_vals.pop(-1)

    # Create y_vals
    if search != 'NET':
        for month in x_vals:
            y_vals.append(int(gdf.amount[(gdf.date_id==month) & ~(gdf.trans=='Return')].sum() - gdf.amount[(gdf.date_id==month) & (gdf.trans=='Return')].sum()))
    else:
        for month in x_vals:
            y_vals.append(int((gdfi.amount[(gdfi.date_id==month)].sum() - (gdf.amount[(gdf.date_id==month) & ~(gdf.trans=='Return')].sum() - gdf.amount[(gdf.date_id==month) & (gdf.trans=='Return')].sum()))))

    # Create Median line
    #mean = sum(y_vals)/len(x_vals)
    #y_mean = [mean]*len(x_vals)
    med = statistics.median(y_vals)
    y_med = [med]*len(x_vals)

    # Plot
    # Sets Figure Size
    plt.rcParams["figure.figsize"] = [xplot,yplot]
    fig = plt.figure()
    fig.canvas.set_window_title(heading)
    ax = fig.add_subplot()
    #fig, ax = plt.subplots()
    ax.plot(x_vals, y_vals,linecolor,linewidth=lnwid)
    #ax.set_facecolor("black")
    plt.title(heading, fontdict={'fontsize': title}, pad=20)
    plt.ylabel('$',rotation=0,labelpad=30,fontsize=ylbl)
    ax.yaxis.set_tick_params(labelsize=ytick)
    ax.xaxis.set_tick_params(labelsize=xtick)
    ax.spines['bottom'].set_linewidth(0.5)
    ax.spines['top'].set_linewidth(0)
    ax.spines['left'].set_linewidth(0)
    ax.spines['right'].set_linewidth(0)
    ax.yaxis.grid()
    # horizontal lines
    #ax.grid()

    # Prints Median
    if search == 'NET':
        plt.xlabel(f'Median Net Income ${med:.0f}/month', fontsize=20, labelpad=30) #\nAverage Net Income ${mean:.0f}/month'
        y_zero = [0]*len(x_vals)
        ax.plot(x_vals,y_zero,'k',linewidth=2.5)
    elif 'Deposit' not in gdf.trans.tolist():
        plt.xlabel(f'Median Cost ${med:.0f}/month', fontsize=20, labelpad=30) #\nAverage Cost ${mean:.0f}/month'
    else:
        plt.xlabel(f'Median Income ${med:.0f}/month', fontsize=20, labelpad=30) #\nAverage Income ${mean:.0f}/month'
    #ax.plot(x_vals,y_mean,'k:',linewidth=avgwid)
    ax.plot(x_vals,y_med,'r:',linewidth=avgwid)
    plt.show()

    if input('\nDo you want to see the DataFrame? (y) ').lower() == 'y':
        monthdf = pandas.DataFrame(list(zip(x_vals,y_vals)),columns=['Month','Amount'])
        monthdf.set_index('Month',inplace=True)
        print('')
        print(monthdf)
        print('')

plot(df=df,by=sys.argv[2],search=sys.argv[3],start_date=sys.argv[4],end_date=sys.argv[5])
