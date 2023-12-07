import json
import boto3
import os
import uuid
import base64
import pathlib
import datatier

from configparser import ConfigParser

def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: final_uploadimage**")
    
    #
    # setup AWS based on config file:
    #
    config_file = 'config.ini'
    os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file
    
    configur = ConfigParser()
    configur.read(config_file)
    
    #
    # configure for S3 access:
    #
    s3_profile = 's3readwrite'
    boto3.setup_default_session(profile_name=s3_profile)
    
    bucketname = configur.get('s3', 'bucket_name')
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucketname)
    
    #
    # configure for RDS access
    #
    rds_endpoint = configur.get('rds', 'endpoint')
    rds_portnum = int(configur.get('rds', 'port_number'))
    rds_username = configur.get('rds', 'user_name')
    rds_pwd = configur.get('rds', 'user_pwd')
    rds_dbname = configur.get('rds', 'db_name')
    
    #
    # userid from event: could be a parameter
    # or could be part of URL path ("pathParameters"):
    #
    print("**Accessing event/pathParameters**")
    
    if "userid" in event:
      userid = event["userid"]
    elif "pathParameters" in event:
      if "userid" in event["pathParameters"]:
        userid = event["pathParameters"]["userid"]
      else:
        raise Exception("requires userid parameter in pathParameters")
    else:
        raise Exception("requires userid parameter in event")
        
    print("userid:", userid)
  
    #
    # the user has sent us two parameters:
    #  1. filename of their file
    #  2. raw file data in base64 encoded string
    #
    # The parameters are coming through web server 
    # (or API Gateway) in the body of the request
    # in JSON format.
    #
    print("**Accessing request body**")
    
    if "body" not in event:
      raise Exception("event has no body")
      
    body = json.loads(event["body"]) # parse the json
    
    ## SHOULD ALSO BE A PRIVACY tag in data
    if "assetname" not in body:
      raise Exception("event has a body but no filename")
    if "data" not in body:
      raise Exception("event has a body but no data")

    assetname = body["assetname"]
    datastr = body["data"]
    
    print("assetname:", assetname)
    print("datastr (first 10 chars):", datastr[0:10])

    #
    # open connection to the database:
    #
    print("**Opening connection**")
    
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)

    #
    # first we need to make sure the userid is valid:
    #
    print("**Checking if userid is valid**")
    
    sql = "SELECT bucketfolder FROM users WHERE userid = %s;"
    
    row = datatier.retrieve_one_row(dbConn, sql, [userid])
    
    if not row:
      print("Database operation failed... returning")
      return {
        'statusCode': 400,
        'body': json.dumps({"message":"database operation failed...",
                            "assetid": -1})
      }
    
    elif row == ():  # no such user
      print("**No such user, returning...**")
      return {
        'statusCode': 400,
        'body': json.dumps({"message":"no such user...",
                            "assetid": -1})
      }
    


    print(row)
    
    bucketfolder = row[0]
    
    #
    # at this point the user exists, so safe to upload to S3:
    #
    base64_bytes = datastr.encode()        # string -> base64 bytes
    bytes = base64.b64decode(base64_bytes) # base64 bytes -> raw bytes
    
    #
    # write raw bytes to local filesystem for upload:
    #
    print("**Writing local data file**")
    
    local_filename = "/tmp/data.pdf"
    
    outfile = open(local_filename, "wb")
    outfile.write(bytes)
    outfile.close()
    
    #
    # generate unique filename in preparation for the S3 upload:
    #
    print("**Uploading local file to S3**")
    
    # basename = pathlib.Path(assetname).stem 
    extension = pathlib.Path(assetname).suffix
    
    if extension != ".jpg" : 
      raise Exception("expecting filename to have .jpg extension")
    

    # CHANGE TO JPG
    bucketkey = bucketfolder + "/" + str(uuid.uuid4()) + ".jpg"
    
    print("S3 bucketkey:", bucketkey)
    
    #
    # add to database
    #
    print("**Adding asset to database**")
    
    # Change sql to add privacy tag
    sql = """
    INSERT INTO assets (userid, assetname, bucketkey) VALUES (%s, %s, %s);
    """
    
    q = datatier.perform_action(dbConn, sql, [userid, assetname, bucketkey])

    if q == -1:
      print("Database operation failed...")
      return {
        'statusCode': 400,
        'body': json.dumps({"message":"inserting asset failed",
                            "assetid": -1})
      }
    elif q == 0:
      print("Unexpected query failure...")
      return {
        'statusCode': 400,
        'body': json.dumps({"message":"inserting asset failed",
                            "assetid": -1})
      }
    
    #
    # grab the jobid that was auto-generated by mysql:
    #
    sql = "SELECT LAST_INSERT_ID();"
    
    row = datatier.retrieve_one_row(dbConn, sql)
    
    assetid = row[0]
    
    print("assetid:", assetid)
    
    #
    # finally, upload to S3:
    #
    print("**Uploading data file to S3**")

    bucket.upload_file(local_filename, 
                       bucketkey, 
                       ExtraArgs={
                         'ACL': 'public-read',
                         'ContentType': 'application/jpg' ## might be wrong here
                       })

    #
    # respond in an HTTP-like way, i.e. with a status
    # code and body in JSON format:
    #
    print("**DONE, returning assetid**")
    
    return {
          'statusCode': 200,
          'body': json.dumps({"message":"success",
                              "assetid": assetid})
        }
    
  except Exception as err:
    print("**ERROR**")
    print(str(err))
    
    return {
        'statusCode': 400,
        'body': json.dumps({"message":str(err),
                            "assetid": -1})
      }
