#
# Client-side python app for benford app, which is calling
# a set of lambda functions in AWS through API Gateway.
# The overall purpose of the app is to process a PDF and
# see if the numeric values in the PDF adhere to Benford's
# law.
#
# Authors:
#   Prof. Joe Hummel
#   Northwestern University
#   CS 310, Project 03
#
#   Dilan Nair
#   Northwestern University
#   CS 310, Project 04
#

import requests
import json

import uuid
import pathlib
import logging
import sys
import os
import base64

from configparser import ConfigParser

import matplotlib.pyplot as plt
import matplotlib.image as img

############################################################
#
# classes
#


class User:

  def __init__(self, row):
    self.userid = row[0]
    self.email = row[1]
    self.lastname = row[2]
    self.firstname = row[3]
    self.bucketfolder = row[4]
    self.username = row[5]
    self.pwdhash = row[6]


class Image:

  def __init__(self, row):
    self.assetid = row[0]
    self.userid = row[1]
    self.assetname = row[2]
    self.bucketkey = row[3]
    self.assettype = row[4]


class Like:

  def __init__(self, row):
    self.likeid = row[0]
    self.userid = row[1]
    self.assetid = row[2]


class Comment:

  def __init__(self, row):
    self.commentid = row[0]
    self.userid = row[1]
    self.assetid = row[2]
    self.comment = row[3]


############################################################
#
# globals
#

sessions = {}


def load_sessions():
  """
  Loads the previous sessions from the sessions.json file
  """

  global sessions
  if os.path.exists("sessions.json"):
    with open("sessions.json", "r") as f:
      sessions = json.load(f)


def update_session(username, token):
  """
  Updates the session with the given username and token
  """

  global sessions
  sessions[username] = {"token": token, "active": False}

  use_session(username)


def get_active_session():
  """
  Returns the active session
  """

  global sessions
  for username in sessions:
    if sessions[username]["active"]:
      return username, sessions[username]["token"]
  return None, None


def use_session(username):
  """
  Sets the session with the given username to active
  """

  global sessions
  for session in sessions:
    sessions[session]["active"] = False
  sessions[username]["active"] = True
  with open("sessions.json", "w") as f:
    json.dump(sessions, f, indent=2)


def clear_sessions():
  """
  Clears all sessions
  """

  global sessions
  sessions = {}
  with open("sessions.json", "w") as f:
    json.dump(sessions, f, indent=2)


def handle_error(url, res):
  """
  Handles an error from a request
  """

  print("Failed with status code:", res.status_code)
  print("  url:", url)
  print("  message:", res.json()["message"])


############################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number

  Parameters
  ----------
  None

  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """
  print()
  print(">> Enter a command:")
  print("   0 => quit")
  print("")
  print("   1 => get all users")
  print("   2 => add user")
  print("   3 => log in")
  print("   4 => view and switch sessions")
  print("")
  print("   5 => get [public and private] photos for this user")
  print("   6 => upload image")
  print("   7 => download image")
  print("")
  print("   8 => log out all")
  print("   9 => like post")
  print("   10 => comment on post")
  print("   11 => get likes on post")
  print("   12 => get comments on post")

  cmd = input()

  if cmd == "":
    cmd = -1
  elif not cmd.isnumeric():
    cmd = -1
  else:
    cmd = int(cmd)

  return cmd


############################################################
#
# get_users
#
def get_users(baseurl):
  """
  Prints out all the users in the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  #
  # call the web service:
  #
  api = '/final_users'
  url = baseurl + api

  res = requests.get(url)

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # deserialize and extract users:
  #
  body = res.json()["data"]

  #
  # let's map each row into a User object:
  #
  users = []
  for row in body:
    user = User(row)
    users.append(user)
  #
  # Now we can think OOP:
  #
  if len(users) == 0:
    print("no users...")
    return

  for user in users:
    print(user.userid)
    print(" ", user.username)

  return


############################################################
#
# add_user
#
def add_user(baseurl):
  """
  Adds a new user to the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  #
  # get username and password from user:
  #
  print("Enter username>")
  username = input()

  print("Enter password>")
  password = input()

  email = input('Enter user\'s email>\n')
  last_name = input('Enter user\'s last (family) name>\n')
  first_name = input('Enter user\'s first (given) name>\n')
  bucket_name = str(uuid.uuid4())
  #
  # build the data packet:
  #
  data = {
    "username": username,
    "password": password,
    "email": email,
    "firstname": first_name,
    "lastname": last_name,
    "bucketfolder": bucket_name
  }

  #
  # call the web service:
  #
  api = '/final_adduser'
  url = baseurl + api

  res = requests.post(url, json=data)

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # success, extract userid:
  #
  body = res.json()

  userid = body["userid"]

  print("User added, id =", userid)

  return


############################################################
#
# login
#
def login(baseurl):
  """
  Log in as a user

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  #
  # get username and password from user:
  #
  print("Enter username>")
  username = input()

  print("Enter password>")
  password = input()

  #
  # build the data packet:
  #
  data = {"username": username, "password": password}

  #
  # call the web service:
  #
  api = '/auth'
  url = baseurl + api

  res = requests.post(url, json=data)

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # success, extract token:
  #
  body = res.json()

  token = body["access_token"]

  print("New user logged in, username = ", username)

  #
  # update sessions:
  #
  update_session(username, token)

  return


############################################################
#
# switch_user
#
def switch_user(baseurl):
  """
  Switch user

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  print("Current sessions:")
  if len(sessions) == 0:
    print("  none")

  for session in sessions:
    print(" ", session, " => active =", sessions[session]["active"])

  print("Enter username of session or leave blank to skip>")
  username = input()

  if username == "":
    return

  if username not in sessions:
    print("No session with that username...")
    return

  use_session(username)

  print("Switched session, username =", username)

  return


############################################################
#
# get_assets
#
def get_assets(baseurl):
  """
  Prints out all the jobs in the database

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """
  username, token = get_active_session()

  if username is None:
    print("no action session, will only get public images...")

  #
  # call the web service:
  #
  api = '/final_assets'
  url = baseurl + api

  # passing in None for token indicates no active session and only public images should be returned
  res = None
  if username is not None:
    res = requests.get(url, headers={"Authorization": "Bearer " + token})
  else:
    res = requests.get(url)

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # deserialize and extract images:
  #
  body = res.json()
  #
  # let's map each row into an Job object:
  #
  images = []
  for row in body:
    image = Image(row)
    images.append(image)
  #
  # Now we can think OOP:
  #
  if len(images) == 0:
    print("no images...")
    return

  for image in images:
    print(image.assetid)
    print(" ", image.userid)
    print(" ", image.assetname)
    print(" ", image.bucketkey)
    print(" ", image.assettype)

  return


############################################################
#
# like_post
#


def like_post(baseurl):
  try:
    username, token = get_active_session()

    if username is None:
      print("No active session...")
      return

    print("Liking post as user:", username)
    print("Enter asset id to like>")
    asset_id = 0
    try:
      asset_id = int(input())
    except ValueError:
      print("Please enter a number for the assetid to like")
      return
    #
    # call the web service:
    #
    api = '/final_like'
    url = baseurl + api + '/' + asset_id
    res = requests.post(url, headers={"Authorization": "Bearer " + token})
    #
    # let's look at what we got back:
    #
    if not res.ok:
      handle_error(url, res)
      return
    else:
      print(f"assetid {assetid} liked!!")

  except Exception as e:
    logging.error("download_image failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


def comment(baseurl):
  try:
    username, token = get_active_session()
    if username is None:
      print("No active session...")
      return

    print("Commenting on post as user:", username)
    print("Enter asset id to comment on>")
    asset_id = 0
    try:
      asset_id = int(input())
    except ValueError:
      print("Please enter a number for the asset id to comment on")
      return
    #
    # call the web service:
    #
    print("enter comment")
    comment_val = input()
    data = {"comment": comment_val}
    api = '/final_comment'
    url = baseurl + api + '/' + asset_id
    res = requests.post(url,
                        json=data,
                        headers={"Authorization": "Bearer " + token})
    #
    # let's look at what we got back:
    #
    if not res.ok:
      handle_error(url, res)
      return
    else:
      print(f"assetid {assetid} commented on!!")

  except Exception as e:
    logging.error("comment failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


def get_likes(baseurl):
  try:
    username, token = get_active_session()

    if username is None:
      print("No active session. can only view the likes of public images...")
    else:
      print("viewing likes as user:", username)

    print("Enter asset id>")
    asset_id = 0
    try:
      asset_id = int(input())
    except ValueError:
      print("Please enter a number for the assetid to see likes for")
      return
    #
    # call the web service:
    #
    api = '/final_getlikes'
    url = baseurl + api + '/' + asset_id
    res = None
    if username is not None:
      res = requests.get(url, headers={"Authorization": "Bearer " + token})
    else:
      res = requests.get(url)

    #
    # let's look at what we got back:
    #
    if not res.ok:
      handle_error(url, res)
      return

    body = res.json()

    likes = []
    for row in body:
      like = Like(row)
      likes.append(like)
  #
  # Now we can think OOP:
  #
    if len(likes) == 0:
      print("no likes...")
      return

    for like in likes:
      print(f"likeid: ", like.likeid)
      print(f"userid: ", like.userid)
    return

  except Exception as e:
    logging.error("get_likes failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


def get_comments(baseurl):
  try:
    username, token = get_active_session()

    if username is None:
      print(
        "No active session. can only view the comments of public images...")
    else:
      print("viewing comments as user:", username)

    print("Enter asset id>")
    asset_id = 0
    try:
      asset_id = int(input())
    except ValueError:
      print("Please enter a number for the assetid to see comments for")
      return
    #
    # call the web service:
    #
    api = '/final_getcomments'
    url = baseurl + api + '/' + asset_id
    res = None
    if username is not None:
      res = requests.get(url, headers={"Authorization": "Bearer " + token})
    else:
      res = requests.get(url)

    #
    # let's look at what we got back:
    #
    if not res.ok:
      handle_error(url, res)
      return

    body = res.json()

    comments = []
    for row in body:
      comment = Comment(row)
      comments.append(comment)
  #
  # Now we can think OOP:
  #
    if len(comments) == 0:
      print("no comments...")
      return

    for comment in comments:
      print(f"likeid: ", comment.commentid)
      print(f"userid: ", comment.userid)
      print(f"comment: ", comment.comment)
      print()
    return

  except Exception as e:
    logging.error("getcomments failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


def upload_image(baseurl):
  username, token = get_active_session()

  if username is None:
    print("No active session...")
    return

  print("Uploading as user:", username)

  print("Enter filename of picture to upload>")
  local_filename = input()

  if not pathlib.Path(local_filename).is_file():
    print("PDF file '", local_filename, "' does not exist...")
    return

  #
  # build the data packet:
  #
  infile = open(local_filename, "rb")
  bytes = infile.read()
  infile.close()
  print("Should this image be public (enter '1') or private (enter '0') ?")
  val = 0
  try:
    val = int(input())
  except ValueError:
    print("please enter a number...")
    return

  #
  # now encode the pdf as base64. Note b64encode returns
  # a bytes object, not a string. So then we have to convert
  # (decode) the bytes -> string, and then we can serialize
  # the string as JSON for upload to server:
  #
  data = base64.b64encode(bytes)
  datastr = data.decode()
  stri = ""
  if val == 0:
    stri = "private"
  else:
    stri = "public"

  data = {"filename": local_filename, "data": datastr, "assettype": stri}

  #
  # call the web service:
  #
  api = '/final_uploadimage'
  url = baseurl + api

  res = requests.post(url,
                      json=data,
                      headers={"Authorization": "Bearer " + token})

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # success, extract jobid:
  #
  body = res.json()

  asset = body["assetid"]

  print("image uploaded, assetid =", asset)
  return


############################################################
#
# download
#
def download_image(baseurl):
  """
  Prompts the user for the job id, and downloads
  that asset (PDF).

  Asset must belong to the authenticated user.

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """
  try:
    username, token = get_active_session()

    if username is None:
      print("No active session. can only download public images...")
    else:
      print("Downloading as user:", username)

    print("Enter asset id>")
    asset_id = 0
    try:
      asset_id = int(input())
    except ValueError:
      print("Please enter a number for the assetid to download")
      return
    #
    # call the web service:
    #
    api = '/final_download'
    url = baseurl + api + '/' + assetid
    res = None
    if username is not None:
      res = requests.get(url, headers={"Authorization": "Bearer " + token})
    else:
      res = requests.get(url)

    #
    # let's look at what we got back:
    #
    if not res.ok:
      handle_error(url, res)
      return

    body = res.json()
    img_data = base64.b64decode(body["data"])
    with open(body["asset_name"], "wb") as outfile:
      outfile.write(img_data)
    print(f"userid: {body['user_id']}")
    print(f"asset name: {body['asset_name']}")
    print(f"bucket key: {body['bucket_key']}")
    print(f"Downloaded from S3 and saved as \' {body['asset_name']} \'")

  except Exception as e:
    logging.error("download_image failed:")
    logging.error("url: " + url)
    logging.error(e)
    return


############################################################
#
# reset_sessions
#
def reset_sessions(baseurl):
  """
  Clears all sessions

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  clear_sessions()

  print("Sessions cleared")

  return


############################################################
#
# reset_everything
#
def reset_everything(baseurl):
  """
  Resets the database back to initial state and clears all sessions.

  Parameters
  ----------
  baseurl: baseurl for web service

  Returns
  -------
  nothing
  """

  #
  # clear sessions:
  #
  clear_sessions()

  #
  # call the web service:
  #
  api = '/reset'
  url = baseurl + api

  res = requests.delete(url)

  #
  # let's look at what we got back:
  #
  if not res.ok:
    handle_error(url, res)
    return

  #
  # deserialize and print message
  #
  body = res.json()

  msg = body

  print(msg)
  return


############################################################
# main
#
try:
  print('** Welcome to our augmented photoapp **')
  print()

  # eliminate traceback so we just get error message:
  sys.tracebacklimit = 0

  #
  # what config file should we use for this session?
  #
  config_file = 'final_config.ini'

  print("Config file to use for this session?")
  print("Press ENTER to use default, or")
  print("enter config file name>")
  s = input()

  if s == "":  # use default
    pass  # already set
  else:
    config_file = s

  #
  # does config file exist?
  #
  if not pathlib.Path(config_file).is_file():
    print("**ERROR: config file '", config_file, "' does not exist, exiting")
    sys.exit(0)

  #
  # setup base URL to web service:
  #
  configur = ConfigParser()
  configur.read(config_file)
  baseurl = configur.get('client', 'webservice')

  #
  # make sure baseurl does not end with /, if so remove:
  #
  if len(baseurl) < 16:
    print("**ERROR: baseurl '", baseurl, "' is not nearly long enough...")
    sys.exit(0)

  if baseurl == "https://YOUR_GATEWAY_API.amazonaws.com":
    print(
      "**ERROR: update benfordapp-client-config.ini file with your gateway endpoint"
    )
    sys.exit(0)

  lastchar = baseurl[len(baseurl) - 1]
  if lastchar == "/":
    baseurl = baseurl[:-1]

  #
  # load previous sessions:
  #
  load_sessions()

  #
  # main processing loop:
  #
  cmd = prompt()

  fns = [
    None, get_users, add_user, login, switch_user, get_assets, upload_image,
    download_image, reset_sessions, like_post, comment, get_likes, get_comments
  ]

  try:
    while cmd != 0:
      if cmd < 0 or cmd >= len(fns):
        print("** Unknown command, try again...")
        cmd = prompt()
        continue
      fn = fns[cmd]
      if fn is None:
        break
      fn(baseurl)
      cmd = prompt()
  except Exception as e:
    logging.error(fn.__name__ + "() failed:")
    logging.error(e)

  #
  # done
  #
  print()
  print('** done **')
  sys.exit(0)

except Exception as e:
  logging.error("**ERROR: main() failed:")
  logging.error(e)
  sys.exit(0)
