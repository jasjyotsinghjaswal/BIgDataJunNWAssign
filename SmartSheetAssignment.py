import smartsheet
import boto3
import os

# current environment variables
os.environ['SMARTSHEET_ACCESS_TOKEN'] = 'wslb0m3vnidcl7uar1qxncxkjy'
print(os.environ)

# Initialize client
smart = smartsheet.Smartsheet()
# Make sure we don't miss any error
smart.errors_as_exceptions(True)

# Find SheetID and SheetName
action = smart.Sheets.list_sheets(include_all=True)
for single_sheet in action.data:
    print(single_sheet.id, single_sheet.name)

# Retrieve the sheet using the sheet id retrieved in the above process
MySheet = smart.Sheets.get_sheet(1933393538639748)

# Create row object and make a string to store records delimited by comma(,) and row separated by newline
rowString = ""
for MyRow in MySheet.rows:
    for MyCell in MyRow.cells:
        rowString = rowString + str(MyCell.value) + ","
    rowString = rowString + "\n"

usersession = boto3.session.Session(profile_name='dev')
s3 = usersession.resource('s3')
object = s3.Object('smartsheetdemo', 'mydoc/samplesheet.txt')
object.put(Body=rowString)
