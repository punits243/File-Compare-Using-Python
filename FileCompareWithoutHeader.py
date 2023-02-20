import datetime
import pandas as pd
from pandas.util import hash_pandas_object

Current_Date = datetime.datetime.now()
print('Current_DateTime :', Current_Date)

# Taking Input from User
fileOne = input("Enter file1 path : ")
fileTwo = input("Enter file2 path : ")
dlmtr = input("Enter Delimiter : ")
n = int(input("Enter number of Primary key : "))
prmy = list(input("Enter Primary key : "). strip(). split(','))[:n]

col_names = ["col1", "col2", "col3", "col4", "col5","col6","col7","col8","col9","col10",
             "col11","col12","col13","col14","col15","col16","col17","col18","col19","col20",
             "col21","col22""col23","col24","col25","col26","col27","col28","col29","col30",
             "col31","col32","col33","col34","col35","col36","col37","col38","col39","col40",
             "col41","col42","col43","col44","col45","col46","col47","col48","col49","col50"]

# Reading CSV File
df = pd.read_csv(fileOne, delimiter=dlmtr, index_col=None, low_memory=False, names=col_names)
df1 = pd.read_csv(fileTwo, delimiter=dlmtr, index_col=None, low_memory=False, names=col_names)

len1 = len(df.index)
len2 = len(df1.index)

#df.index += 1
#df1.index += 1

print('Source Count :', len1)
print('Target Count :', len2)

if len1 != len2:
    print("Count Mismatch", abs(len1 - len2))
else:
    print("Count Matched")

# Sorting on Primary key
sort1 = df.sort_values(prmy)
sort2 = df1.sort_values(prmy)


# Implementing Hashing for efficient comparision
h = hash_pandas_object(sort1, index=False)
h1 = hash_pandas_object(sort2, index=False)

# Checking for Rows Mismatch
r = h[~h.isin(h1)]
r1 = h1[~h1.isin(h)]

# Finding Location to know original Row by using index value of Hash Compare Result
l1 = df.loc[r.index]
l2 = df1.loc[r1.index]

# Finding Rows on the basis of Primary Key
index1 = pd.MultiIndex.from_arrays([l1[col] for col in prmy])
index2 = pd.MultiIndex.from_arrays([l2[col] for col in prmy])

s1 = l1.loc[index1.isin(index2)].reset_index()
s2 = l2.loc[index2.isin(index1)].reset_index()

# Finding Extra Rows
s3 = l1.loc[~index1.isin(index2)].reset_index()
s4 = l2.loc[~index2.isin(index1)].reset_index()

# Concat
f1=pd.concat([s1,s2], axis= 1)
f2=pd.concat([s3,s4], axis=1).reset_index()

print("Extra Rows Count:", len(f2.index))

# Write Result into CSV File ("Row_Output.csv") and for Extra Row ("Extra_Row_Output.csv")
f1.to_csv("Row_Output.csv")
f2.to_csv("Extra_Row_Output.csv")

# Finding Column Value Mismatch
result = s1.compare(s2)
res = result.rename(columns={'self': 'Source', 'other': 'Target'})

# Removing NaN by Space
rslt = res.fillna(" ")

# writing Column Value Mismatch into CSV File("Col_output.csv")
rslt.to_csv("Col_output.csv")

# Finding Cell Value Mismatch Count
s= res.count()
#count = (s.sum())/2
count = (s.sum() - (len(res.index)*2))/ 2
print("Cell Value Mismatch : ", count)
print("Please Refer For Row level value : Row_Output.csv  \nFor Extra Row value : Extra_Row_Output.csv \nFor Column level value : Col_output.csv")

#Current_Date = datetime.datetime.now()
#print('Current_DateTime :', Current_Date)
