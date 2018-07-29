import psycopg2
from urllib.parse import urlparse
import io
import zipfile
import boto3

URL='' # SOURCE DATA URL
POSTGRE = '' # DATABASE CONNECTION

def check_connection():
	try:
		conn = psycopg2.connect(POSTGRE,connect_timeout=3)
		conn.close()
	except:
		print("connection to database failed!")
		exit(0)

def get_zip_file():
	s3 = boto3.resource('s3')
	_, bucket_name, key = urlparse(URL).path.split('/', 2)
	obj = s3.Object(
		bucket_name=bucket_name,
		key=key)
	buffer = io.BytesIO(obj.get()["Body"].read())
	z = zipfile.ZipFile(buffer)
	content = z.read('2017-12-28.json')
	print (len(content))

if __name__ == "__main__":

	with open('credential.txt') as f:
		credentials = [x.strip().split('=') for x in f.readlines()]

	for c in credentials:
		if c[0] == 's3url': URL = c[1]
		if c[0] == 'postgredatabase': POSTGRE = POSTGRE + 'dbname=' + c[1] + ' '
		if c[0] == 'user': POSTGRE = POSTGRE + 'user=' + c[1] + ' '
		if c[0] == 'password': POSTGRE = POSTGRE + 'password=' + c[1] + ' '
		if c[0] == 'host': POSTGRE = POSTGRE + 'host=' + c[1]

	check_connection()
	get_zip_file()
