from boto3 import client, resource, session
import botocore
import uuid

class S3Connection:

	def __init__(self):
		self.client = client("s3")
		self.s3 = resource('s3')
		self.session = session.Session()
		self.count = 0 # Number of files uploaded with upload_files()
		
	# Creates folder to Bucket (Need to have bucket name as parent "bucketname/dir1/dir2")
	def create_folder(self, path):
		path_arr = path.rstrip("/").split("/")
		if len(path_arr) == 1:
			return self.client.create_bucket(Bucket=path_arr[0])
		parent = path_arr[0]
		bucket = self.s3.Bucket(parent)
		status = bucket.put_object(Key="/".join(path_arr[1:]) + "/")
		return status

	# Uploads a local file to bucket. Destination filepath can have folders also.
	def upload_file(self, lclFilename, bucket_name, destFilename):
		bucket = self.s3.Bucket(bucket_name)
		with open(lclFilename, 'rb') as data:
			status = bucket.put_object(Key=destFilename, Body=data)
			self.count += 1
		return status
		
	def getBucketName(self,prefix):
		for bucket in self.s3.buckets.all(): 
			if bucket.name.startswith(prefix):
				return bucket.name
		return ''
	
	def getCurrentRegion(self):
		return self.session.region_name
		
	def getListOfBucketFiles(self, bucket, key=None):
		if key is None:
			response = self.client.list_objects_v2(Bucket=bucket)
		else:
			response = self.client.list_objects_v2(Bucket=bucket,Prefix=key)

		return response
		
	def create_bucket_s3(self, name, region):
		bucketName = self.create_bucket_name(name)[:63]
		try:
			self.client.create_bucket(
				Bucket=bucketName,
				CreateBucketConfiguration={
				'LocationConstraint': region})
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			if error_code == 'BucketAlreadyExists':
				sys.exit("ERROR: Someone already owns this bucket.")
			elif error_code == 'BucketAlreadyOwnedByYou':
				print("Bucket exists already. No need to recreate it.")
			else:
				print('Error code: ' + e.response['Error']['Code'])
				print(e)
				sys.exit("ERROR: Exiting...")

		return bucketName
		
	def create_bucket_name(self,prefix):
		return ''.join([prefix,"-",str(uuid.uuid4())])