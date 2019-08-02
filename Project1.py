import matplotlib.pyplot as plt
import pandas as pd
from pandasql import sqldf
import numpy as np

pd.set_option('expand_frame_repr', False)

df=pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')
df1=pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv')

#1. Get the Metadata from the above files.
print('\n[01] Metadata for the given datasets...\n','-'*70,'\n')
print('\n	DataSet:-data-text.csv\n','-'*40,'\n')
print(df.info())
print('\n	DataSet:-berlin_weather_oldest.csv\n','-'*40,'\n')
print(df1.info())

#2. Get the row names from the above files.
print('\n[02] Row name for each datasets...\n','-'*70,'\n')
print('\n	RowName for DataSet:-data-text.csv\n','-'*40,'\n')
print(df.index.values)
print('\n	RowName for DataSet:-berlin_weather_oldest.csv\n','-'*50,'\n')
print(df1.index.values)

#3. Change the column name from any of the above file.
print('\n[03] Changing column name for dataset=data-text.csv from Indicator to Indicator_id \n','-'*70,'\n')
print(df.rename(columns={'Indicator':'Indicator_id'}).head(2))
	
#4. Change the column name from any of the above file and store the changes made permanently.
print('\n[04] Changing column name for dataset=data-text.csv from Indicator to Indicator_id and changes made permanently \n','-'*90,'\n')
df.rename(columns={'Indicator':'Indicator_id'},inplace=True)
print(df.head(2))
	
#5. Change the names of multiple columns.
print('\n[05] Changing multiple column names for dataset=data-text.csv and changes made permanently \n','-'*90,'\n')
df.rename(columns={'PUBLISH STATES':'Publication Status','WHO region':'WHO Region'},inplace=True)
print(df.head(2))
	
#6. Arrange values of a particular column in ascending order.
print('\n[06] Arrange value by Year column in ascending order \n','-'*50,'\n')
print(df.sort_values(by=['Year']).head())
	
#7. Arrange multiple column values in ascending order.
print('\n[07] Arrange multiple column[By Year,Country,WHO Region,Publication Status] value in ascending order \n','-'*90,'\n')
print(df.sort_values(by=['Year','Country','WHO Region','Publication Status']).head())

#8. Make country as the first column of the dataframe.
print('\n[08] Country as the first column of the dataframe \n','-'*50,'\n')
columnsReorder=['Country','Indicator_id','Publication Status','Year','WHO Region','World Bank income group','Sex','Display Value','Numeric','Low','High','Comments']
print(df.reindex(columns=columnsReorder).head())

#9. Get the column array using a variable
print('\n[09] Get the column array using a variable \n','-'*50,'\n')
print(df.as_matrix(columns=['WHO Region']))

#10. Get the subset rows 11, 24, 37
print('\n[10] Get the subset rows 11, 24, 37\n','-'*50,'\n')
print(df.loc[[11,24,37]])

#11. Get the subset rows excluding 5, 12, 23, and 56
print('\n[11] Get the subset rows excluding 5, 12, 23, and 56\n','-'*60,'\n')
print(df.drop(df.index[[5,12,23,56]]).head())


users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
sessions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
products = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
transactions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')
users['Registered']=pd.to_datetime(users['Registered'])
users['Cancelled']=pd.to_datetime(users['Cancelled'])
sessions['SessionDate']=pd.to_datetime(sessions['SessionDate'])
transactions['TransactionDate']=pd.to_datetime(transactions['TransactionDate'])

#12. Join users to transactions, keeping all rows from transactions and only matching rows from users (left join)
print('\n[12] Join users to transactions(left join)\n','-'*60,'\n')
print(pd.merge(transactions, users, on='UserID', how='left'))

#13. Which transactions have a UserID not in users?
print('\n[13] Join users to transactions(left join)\n','-'*60,'\n')
q="select * from transactions t where not exists (select 1 from users u where t.UserID=u.UserID)"
print(sqldf(q))

#14. Join users to transactions, keeping only rows from transactions and users that match via UserID (inner join)
print('\n[14] Join users to transactions, keeping only rows from transactions and users that match via UserID\n','-'*90,'\n')
print(pd.merge(transactions,users,on='UserID',how='inner'))

#15. Join users to transactions, displaying all matching rows AND all non-matching rows (full outer join)
print('\n[15] Join users to transactions, displaying all matching rows AND all non-matching rows\n','-'*90,'\n')
print(pd.merge(transactions,users,on='UserID',how='outer'))

#16. Determine which sessions occurred on the same day each user registered
print('\n[16] Sessions occurred on the same day each user registered\n','-'*50,'\n')
print(pd.merge(users,sessions,left_on=['UserID','Registered'],right_on=['UserID','SessionDate'],how='inner'))

#17. Build a dataset with every possible (UserID, ProductID) pair (cross join)
print('\n[17] Building a dataset with every possible (UserID, ProductID) pair\n','-'*90,'\n')
q="select u.UserID,p.ProductID from users u cross join products p"
print(sqldf(q))

#18. Determine how much quantity of each product was purchased by each user
print('\n[18] How much quantity of each product was purchased by each user\n','-'*90,'\n')
q="select u.UserID as UserID,p.ProductID as ProductID from users u cross join products p"
derived_df=sqldf(q)
r="select UserID,ProductID,sum(Quantity) as Quantity from transactions group by UserID,ProductID"
derived_tran=sqldf(r)
out_df=pd.merge(derived_df,derived_tran,on=['UserID','ProductID'],how='left').fillna(0)
print(out_df)

#19. For each user, get each possible pair of pair transactions (TransactionID1,TransacationID2)
print('\n[19] All possible pair of pair transactions by each user\n','-'*90,'\n')
print(pd.merge(transactions,transactions,on='UserID',how='inner', suffixes=('_x','_y')))

#20. Join each user to his/her first occuring transaction in the transactions table
print('\n[20] Join each user to his/her first occuring transaction in the transactions table\n','-'*90,'\n')
comb=pd.merge(users,transactions,on='UserID',how='left')
comb['Rank'] = comb.groupby(by=['UserID'])['TransactionDate'].rank(method='min').fillna(1)
out=comb.loc[comb['Rank']==1].drop(labels='Rank',axis=1).reset_index(drop=True)
print(out)

#21. Test to see if we can drop columns
print('\n[21] Test to see if we can drop columns\n','-'*50,'\n')
my_columns = list(comb.columns)
print(my_columns)

print(list(comb.dropna(thresh=int(comb.shape[0] * .9), axis=1).columns))

missing_info = list(comb.columns[comb.isnull().any()])
print(missing_info)

print('\nCount of missing data\n','-'*50,'\n')
for col in missing_info:
    num_missing = comb[comb[col].isnull() == True].shape[0]
    print('number missing for column {}: {}'.format(col, num_missing))

print('\nOutput of percentage missing data\n','-'*50,'\n')
for col in missing_info:
    percent_missing=comb[comb[col].isnull()==True].shape[0]/comb.shape[0]
    print('percent missing for column {}:{}'.format(col,round(percent_missing,1)))