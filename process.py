# Import Necessary Packages
import pandas as pd
from helper import Helper

# create object for the helper class 
helper1 = Helper()

# Get Connection and cursor for database
connection = helper1.connection
cursor = helper1.cursor

# read whole file at a time
with open("sampleFile.csv") as file:
    content = file.read()

file.close()

# seperate out the header and data rows
header = []
data = []
for line in content.split('\n'):
    if 'H' in line.split("|")[:2]:
        header = line.split("|")[2:]
    if 'D' in line.split("|")[:2]:
        data.append(tuple(line.split("|")[2:]))

# convert into data frame
df = pd.DataFrame(columns=header, data=data)

# validate dates and its format and convert into date object
df.Open_Date = df.Open_Date.apply(lambda x: helper1.convertToDateFormat(x, helper1.detect_date_format_with_regex(x)))
df.DOB = df.DOB.apply(lambda x: helper1.convertToDateFormat(x, helper1.detect_date_format_with_regex(x)))
df.Last_Consulted_Date = df.Last_Consulted_Date.apply(lambda x: helper1.convertToDateFormat(x, helper1.detect_date_format_with_regex(x)))

# get Distinct countries
uniqueCountries = df.Country.unique()

# check if country table already exit or not. If not then create table for country
helper1.checkAndCreateCountryTable(uniqueCountries)

# rename columns as per the database tables
df.rename(columns={
    "Customer_Name":"customerName",
    "Customer_Id": "customerId",
    "Open_Date": "openDate",
    "Last_Consulted_Date": "lastCunsultedDate",
    "Vaccination_Id": "vaccinationId",
    "Dr_Name": "drName",
    "State": "state",
    "Country": "country",
    "Is_Active": "isActive"
}, inplace=True)

newDf = helper1.getCols("customers", df, cursor)

tuple_of_tuples = helper1.toChuncks(newDf)

cols = ",".join([str(i) for i in newDf.columns.tolist()])

insertQuery = f"""INSERT INTO customers ({cols}) VALUES ({"%s, "*(newDf.shape[1]-1 )}%s) ON DUPLICATE KEY UPDATE  lastCunsultedDate=VALUES(lastCunsultedDate), vaccinationId=VALUES(vaccinationId), drName=VALUES(drName), state=VALUES(state), country=VALUES(country), DOB=VALUES(DOB), isActive=VALUES(isActive)"""

helper1.insertData(insertQuery, tuple_of_tuples)

helper1.closeConnection()