import auth
import pandas as pd

client = auth.get_s3_client()

buckets = client.list_buckets()['Buckets']

files_info = []
buckets_info = []

for bucket in buckets:
    objects = client.list_objects_v2(Bucket=bucket['Name'])
    b = dict(name=bucket['Name'],
             create_date=bucket['CreationDate'].strftime("%m/%d/%Y, %H:%M:%S"),
             size_in_bytes=0)

    if 'Contents' in objects:
        objects_contents = objects['Contents']
        for obj in objects_contents:
            f = dict(bucket=bucket['Name'],
                     path=obj['Key'],
                     size_in_bytes=obj['Size'],
                     last_modified=obj['LastModified'].strftime("%m/%d/%Y, %H:%M:%S"))
            files_info.append(f)

            b['size_in_bytes'] += obj['Size']

            if 'last_modified' not in b:
                b['last_modified'] = obj['LastModified']
            elif b['last_modified'] < obj['LastModified']:
                b['last_modified'] = obj['LastModified']

    if 'last_modified' in b:
        b['last_modified'] = b['last_modified'].strftime("%m/%d/%Y, %H:%M:%S")
    buckets_info.append(b)

buckets_df = pd.DataFrame(buckets_info)
files_df = pd.DataFrame(files_info)
with pd.ExcelWriter('output.xlsx') as writer:
    buckets_df.to_excel(writer, sheet_name='Buckets')
    files_df.to_excel(writer, sheet_name='Files')
