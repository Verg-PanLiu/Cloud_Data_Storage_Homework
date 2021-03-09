# Pan Liu pal81
# CS1660 Cloud Data Storage Homework
import boto3
import csv
s3 = boto3.resource('s3',
                    aws_access_key_id='AKIAQVKAFDH7OJVC5MVP',
                    aws_secret_access_key='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'
                    )

# creat the bucket "panliu314" in the Oregon data center
try:
    s3.create_bucket(Bucket='panliu314', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print("this may already exist")

# make this bucket publicly readable
bucket = s3.Bucket("panliu314")
bucket.Acl().put(ACL='public-read')

#upload a new object into the bucket
body = open('D:\data\exp1.csv', 'rb')
o = s3.Object('panliu314', 'test').put(Body=body )
s3.Object('panliu314', 'test').Acl().put(ACL='public-read')

# create the DynamoDB table
dyndb = boto3.resource('dynamodb',
                       region_name='us-west-2',
                       aws_access_key_id='AKIAQVKAFDH7OJVC5MVP',
                       aws_secret_access_key='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')
try:
    table = dyndb.create_table(
     TableName='DataTable',
     KeySchema=[
         {
         'AttributeName': 'PartitionKey',
         'KeyType': 'HASH'
         },
         {
         'AttributeName': 'RowKey',
         'KeyType': 'RANGE'
         }
     ],
     AttributeDefinitions=[
         {
         'AttributeName': 'PartitionKey',
         'AttributeType': 'S'
         },
         {
         'AttributeName': 'RowKey',
         'AttributeType': 'S'
         },
     ],
     ProvisionedThroughput={
         'ReadCapacityUnits': 5,
         'WriteCapacityUnits': 5
         }
     )
except:
    table = dyndb.Table("DataTable")

#wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

# Reading the csv file, uploading the blobs and creating the table
with open('D:\data\experiment.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf)
    for item in csvf:
        print(item)
        body = open('D:\data\\' + item[2], 'rb')
        s3.Object('panliu314', item[2]).put(Body=body)
        md = s3.Object('panliu314', item[2]).Acl().put(ACL='public-read')

        url = " https://s3-us-west-2.amazonaws.com/panliu314/" + item[2]
        metadata_item = {'PartitionKey': item[0],
                         'RowKey': item[1],
                         'description': item[4],
                         'date': item[3],
                         'url': url
                         }
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

# query used on local machine to pull the data from AWS NoSQL DB
response = table.get_item(Key={'PartitionKey': 'experiment2','RowKey': '2'})
item = response['Item']
print(item)












