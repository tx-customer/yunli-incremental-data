
import boto3
s3 = boto3.client("s3")


# response = s3.list_objects(
#     Bucket='example-output',
#     Prefix='yunli/json_data/0/')

# print(response)

# p = s3.delete_object(
#     Bucket='example-output',
#     Key='yunli/json_data/0/'
# )
# print(p)

key = 'yunli/json_data/2/'
o = [
    {
        "Key": key + 'hue.ini'
    }, {
        "Key": key + 'emr-ha-hue.json'
    },
    {
        "Key": key
    }
]

d = {
    'Objects': o,
    'Quiet': True
}

# print(dir(s3))
response = s3.delete_objects(
    Bucket='example-output',
    Delete=d
)
print(response)
