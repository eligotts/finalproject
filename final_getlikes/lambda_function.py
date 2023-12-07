import json
import boto3
import os
import datatier

from configparser import ConfigParser

def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: final_getlikes**")
    
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
    #s3_profile = 's3readonly'
    #boto3.setup_default_session(profile_name=s3_profile)
    #
    #bucketname = configur.get('s3', 'bucket_name')
    #
    #s3 = boto3.resource('s3')
    #bucket = s3.Bucket(bucketname)
    
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
  
    #
    # the user has sent us two parameters:
    #  1. userid of who is logged in
    #
    # The parameters are coming through web server 
    # (or API Gateway) in the body of the request
    # in JSON format.
    #
    print("**Accessing request body**")
    
    if "body" not in event:
      raise Exception("event has no body")
      
    body = json.loads(event["body"]) # parse the json
    
    if "userid" not in body:
      raise Exception("event has a body but no userid")

    
    userid = body["userid"]
    
    print("userid:", userid)

    #
    # open connection to the database:
    #
    print("**Opening connection**")
    
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)

    #
    # first we need to make sure the assetid is valid:
    #
    print("**Checking if userid is valid**")
    
    # probably include privacy here in query
    sql = """
    SELECT userid FROM assets WHERE assetid = %s;
    """   


    row = datatier.retrieve_one_row(dbConn, sql, [assetid])
    
    if not row:
      print("Database operation failed... returning")
      return {
        'statusCode': 400,
        'body': json.dumps({"message":"database operation failed...",
                            "data": []})
      }
    
    elif row == ():  # no such asset
      print("**No such asset, returning...**")
      return {
        'statusCode': 400,
        'body': json.dumps({"message":"no such asset...",
                            "data": []})
      }
    
    ## NEED LOGIC HERE SO ONLY DOWNLOAD IF HAS ACCESS

    print(row)
    
    author_userid = row[0]
    #privacy = row[1]

    
    #
    # now retrieve all the likes:
    #
    print("**Retrieving data**")
    
    sql = "SELECT * FROM likes WHERE assetid = %s;" ## I think this is right?
    
    rows = datatier.retrieve_all_rows(dbConn, sql, [assetid])
    
    for row in rows:
      print(row)

    #
    # respond in an HTTP-like way, i.e. with a status
    # code and body in JSON format:
    #
    print("**DONE, returning rows**")
    
    return {
        'statusCode': 200,
        'body': json.dumps({"message":"success",
                            "data": rows})
      }
    
  except Exception as err:
    print("**ERROR**")
    print(str(err))
    
    return {
        'statusCode': 400,
        'body': json.dumps({"message":str(err),
                            "data": []})
      }