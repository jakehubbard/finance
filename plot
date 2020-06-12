#!/bin/bash

Help()
{
  # Help Display
  echo
  echo "Plots data based on name, category, subcategory or transaction type."
  echo
  echo "Syntax: plot {DATABASE} {BY} {SEARCH} {START_MONTH} {END_MONTH}"
  echo
  echo "Example: plot sp c restaurant jul-19 apr-20"
  echo
  echo "-h             Print this Help prompt."
  echo
  echo "{DATABASE}"
  echo "sp             Spending Database"
  echo "in             Income Database"
  echo
  echo "{BY}"
  echo "n              name"
  echo "c              category"
  echo "s              subcategory"
  echo "t              transaction type"
  echo
  echo "{SEARCH OVERRIDES}"
  echo "NET            Overall Net Income"
  echo "INCOME         Overall Gross Income"
  echo "SPENDING       Overall Gross Spending"
  echo
  echo "Data read from /home/jake/onedrive/code/database/finance.db"
  echo
}

# Get the options
while getopts ":h" option; do
   case $option in
      h) # display Help
          Help
          exit;
   esac
done

python ~/onedrive/code/bin/plot.py $@
