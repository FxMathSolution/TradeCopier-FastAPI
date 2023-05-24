import xml.etree.ElementTree as ET
import hashlib
import datetime

# Import include.php
from include import *

action = ""
if "action" in request.form:
    action = request.form["action"]

password = ""
if "password" in request.form:
    password = request.form["password"]

username = ""
if "username" in request.form:
    username = request.form["username"]

email = ""
if "email" in request.form:
    email = request.form["email"]

period = ""
if "period" in request.form:
    period = request.form["period"]

account = {
    "username": username,
    "password": password,
    "period": period,
    "email": email,
}


def add_account(account):
    username = account["username"]
    password = hashlib.md5(account["password"].encode()).hexdigest()
    period = account["period"]
    email = account["email"]

    global now
    expire = datetime.datetime.now() + datetime.timedelta(months=int(period))

    db = db()

    db.non_select(f"insert into clients (name, password, full_name, email, expire) values ('{username}', '{password}', ' ', '{email}', '{expire}')")
    record = db.last_insert()
    if record:
        root = ET.Element("fxturn")
        ET.SubElement(root, "action").text = "addaccount"
        ET.SubElement(root, "result").text = "success"
        print(ET.tostring(root))
    else:
        root = ET.Element("fxturn")
        ET.SubElement(root, "action").text = "addaccount"
        ET.SubElement(root, "result").text = "error"
        ET.SubElement(root, "message").text = "Username is not valid"
        print(ET.tostring(root))
    db.close()


def renew_account(account):
    username = account["username"]
    period = account["period"]

    global now
    expire = datetime.datetime.now() + datetime.timedelta(months=int(period))

    db = db()

    db.non_select(f"update clients set expire='{expire}' where name='{username}'")
    record = db.last_insert()

    db.close()

    if record:
        root = ET.Element("fxturn")
        ET.SubElement(root, "action").text = "renewaccount"
        ET.SubElement(root, "result").text = "success"
        print(ET.tostring(root))
    else:
        root = ET.Element("fxturn")
        ET.SubElement(root, "action").text = "renewaccount"
        ET.SubElement(root, "result").text = "error"
        ET.SubElement(root, "message").text = "Failed"
        print(ET.tostring(root))
    db.close()


if action == "addaccount":
    add_account(account)
elif action == "renewaccount":
    renew_account(account)
