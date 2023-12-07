import json
import boto3
import os
import base64
import datatier

from configparser import ConfigParser

def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: final_download**")

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
    s3_profile = 's3readonly'
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
    # assetid from event: could be a parameter
    # or could be part of URL path ("pathParameters"):
    #
    if "assetid" in event:
      assetid = event["assetid"]
    elif "pathParameters" in event:
      if "assetid" in event["pathParameters"]:
        assetid = event["pathParameters"]["assetid"]
      else:
        raise Exception("requires assetid parameter in pathParameters")
    else:
        raise Exception("requires assetid parameter in event")
        
    print("assetid:", assetid)

    print("**Accessing request body**")
    
    if "body" not in event:
      raise Exception("event has no body")
      
    body = json.loads(event["body"]) # parse the json
    
    if "userid" not in body:
      raise Exception("event has a body but no userid")
    

    userid = body["userid"]
    
    print("userid:", userid)

    #
    # does the jobid exist?  What's the status of the job if so?
    #
    # open connection to the database:
    #
    print("**Opening connection**")
    
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)

    #
    # first we need to make sure the assetid is valid:
    #
    print("**Checking if assetid is valid**")
    
    # probably include privacy here in query
    sql = """
    SELECT userid,assetname,bucketkey FROM assets WHERE assetid = %s;
    """
    
    row = datatier.retrieve_one_row(dbConn, sql, [assetid])

    # error in SQL -- MAKE ERROR MESSAGES MATCH
    if not row:
      print("Database operation failed... returning")
      return {
        'statusCode': 400,
        'body': json.dumps({"message":"database operation failed...",
                            "user_id": -1,
                            "asset_name": "?",
                            "bucket_key": "?",
                            "data": []})
      }
    
    elif row == ():  # no such job
      print("**No such asset, returning...**")
      return {
        'statusCode': 400,
        'body': json.dumps({"message":"no such asset...",
                            "user_id": -1,
                            "asset_name": "?",
                            "bucket_key": "?",
                            "data": []})
      }
    
    print(row)
    
    author_userid = row[0]
    assetname = row[1]
    bucketkey = row[2]

     ## NEED LOGIC HERE SO ONLY DOWNLOAD IF HAS ACCESS

    
    print("author userid:", author_userid)
    print("assetname:", assetname)
    print("bucketkey:", bucketkey)
      
    #
    # if we get here, the job completed. So we should have results
    # to download and return to the user:
    #      
    local_filename = "/tmp/results.txt"
    
    print("**Downloading results from S3**")
    
    bucket.download_file(bucketkey, local_filename)
    
    #
    #infile = open(local_filename, "r")
    #ines = infile.readlines()
    #infile.close()
    #
    #for line in lines:
    #  print(line)
    #
  
    #
    # open the file and read as raw bytes:
    #
    infile = open(local_filename, "rb")
    bytes = infile.read()
    infile.close()
    
    #
    # now encode the data as base64. Note b64encode returns
    # a bytes object, not a string. So then we have to convert
    # (decode) the bytes -> string, and then we can serialize
    # the string as JSON for download:
    #
    data = base64.b64encode(bytes)
    datastr = data.decode()

    print("**DONE, returning results**")
    
    #
    # respond in an HTTP-like way, i.e. with a status
    # code and body in JSON format:
    #
    return {
        'statusCode': 200,
        'body': json.dumps({"message":"success",
                            "user_id": author_userid,
                            "asset_name": assetname,
                            "bucket_key": bucketkey,
                            "data": datastr})
      }
    
  except Exception as err:
    print("**ERROR**")
    print(str(err))
    
    return {
          'statusCode': 400,
          'body': json.dumps({"message":str(err),
                              "user_id": -1,
                              "asset_name": "?",
                              "bucket_key": "?",
                              "data": []})
        }
