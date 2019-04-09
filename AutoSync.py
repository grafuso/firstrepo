import hashlib
import os
import argparse
from pathlib import Path, PureWindowsPath
from S3Connection import *

def readConfFile(confFile):
	try:
		f = open(confFile, "r")
	except FileNotFoundError:
		sys.exit("ERROR: Conf file doesn't exist.")

	lines_read = []
	line = f.readline()
	while line:
		lines_read.append(line.rstrip())
		line = f.readline()
	f.close()
	return lines_read

def getArgs():
	parser = argparse.ArgumentParser()
	parser.add_argument('-b', '--Bucket', help='Name of S3 bucket where file will go', required=True)
	parser.add_argument('-f', '--File', help='Name of the local conf file', required=True)
	args = parser.parse_args()
	
	bucketName = args.Bucket
	confFileName = args.File
	if not (3 <= len(bucketName) <= 63):
		sys.exit("ERROR: Bucket name has to be 3-63 chars.")
	
	return bucketName,readConfFile(confFileName)

def getFileMd5(fname):
	hash_md5 = hashlib.md5()
	with open(fname, "rb") as f:
		for chunk in iter(lambda: f.read(4096), b""):
			hash_md5.update(chunk)
	return hash_md5.hexdigest()
	
def listDirFile(paths):
	listOfLocalFiles = {}	# List of dirs	
	listOfMd5 = {} 			# Key - Value pair of 'filename : md5sum'
	for index in paths:
		for dirpath, dirnames, filenames in os.walk(index):
			for file in filenames:
				full_path = os.path.join(dirpath, file)
				fullPathS3 = full_path.replace("\\", "/")
				listOfMd5[fullPathS3[len(index)+1:]] = getFileMd5(full_path)
				if (full_path not in listOfLocalFiles):
					fPath = Path(full_path)
					listOfLocalFiles[PureWindowsPath(fPath)] = fullPathS3[len(index)+1:]
	return listOfLocalFiles,listOfMd5
	
def getS3ObjectMd5(fileName,respJson):
	for obj in respJson.get('Contents', []):
		if obj['Key'] == fileName:
			return obj['ETag']
	return ''
	
def getUploadFiles(objList,md5List,lclFiles):
	listOfFiles = {}
	for key, value in md5List.items():
		if (value != getS3ObjectMd5(key,objList).strip("\"")):
			for local, dest in lclFiles.items():
				if dest == key:
					listOfFiles[local] = key
	return listOfFiles
"""
def uploadFilesToS3(fileList,bucketName,s3_connection):
	count = 0
	for local,dest in fileList.items():
		print("Uploading {} --> {}, to S3.".format(local,dest))
		s3_connection.upload_file(local,bucketName,dest)
		count += 1
	return count
"""

def main():
	# 1) Getting args
	myBucketName, dirsToSync = getArgs()
	
	# 2) Get local dirs, files and file md5's
	localFiles, md5s = listDirFile(dirsToSync)

	# 3) Get S3 file md5's
	s3 = S3Connection()
	targetBucket = s3.getBucketName(myBucketName)
	if targetBucket == '':
		targetBucket = s3.create_bucket_s3(myBucketName,s3.getCurrentRegion())
		print("New Bucket created: {}".format(targetBucket))		
	print("Working with: {}".format(targetBucket))
	result = s3.getListOfBucketFiles(targetBucket)
	
	# 4) Compare local file md5's to S3 md5.
	# If they differ -> Add file to upload_list. If md5's are the same -> skip file.
	uploadFileList = getUploadFiles(result,md5s,localFiles)
	
	# 5) Upload files to S3.
	for lcl,dest in uploadFileList.items():
		print("Uploading {} --> {}, to S3.".format(lcl,dest))
		s3.upload_file(lcl,targetBucket,dest)
	print("Uploaded {} files to {} bucket (S3).".format(s3.count,targetBucket))
	
	# 6) Wait user to see result and exit.
	input("Press Enter to continue...")
	
	# 7) TODO! Delete files from bucket.
	
if __name__ == "__main__":
	try:
		main()
	except Exception as e:
		print("ERROR: ",e)