import json
import boto3
import os
import datatier

from configparser import ConfigParser

def lambda_handler(event, context):
  try:
    print("**STARTING**")
    print("**lambda: final_assets**")
    
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
    # open connection to the database:
    #
    print("**Opening connection**")
    
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
    
    #
    # now retrieve all the users in our users table:
    #
    print("**Retrieving data**")
    
    # could change SQL to not get password
    sql = """
    SELECT * FROM users ORDER BY userid DESC;
    """ 
    
    rows = datatier.retrieve_all_rows(dbConn, sql)
    
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