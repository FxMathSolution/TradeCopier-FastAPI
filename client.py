import config
from db import db


def add_client_deal(client, broker, account_no, prd_id, ticket, symbol, type, lot, balance, equity, open_time, open_price, sl, tp, ip, computer, delay):
    ip = ip[:50]
    db = db()
    error = ""
    id = db.add_client_deal(client, broker, account_no, prd_id, ticket, symbol, type, lot, balance, equity, open_time, open_price, sl, tp, ip, computer, delay)
    if id < 0:
        return id, error
    else:
        return id


def close_client_deal(client, broker, account_no, prd_id, ticket, close_price, profit, commission, swap):
    db = db()
    error = ""
    id = db.close_client_deal(client, broker, account_no, prd_id, ticket, close_price, profit, commission, swap)
    if id < 0:
        return id, error
    else:
        return id


def delete_client_deal(client, broker, account_no, prd_id, ticket):
    db = db()
    error = ""
    id = db.delete_client_deal(client, broker, account_no, prd_id, ticket)
    if id < 0:
        return id, error
    else:
        return id


def modify_client_deal(client, broker, account_no, prd_id, ticket, open_price, sl, tp):
    db = db()
    error = ""
    id = db.modify_client_deal(client, broker, account_no, prd_id, ticket, open_price, sl, tp)
    if id < 0:
        return id, error
    else:
        return id


def suborder_client_deal(client, broker, account_no, prd_id, old_ticket, ticket, old_lot, lot):
    db = db()
    error = ""
    id = db.suborder_client_deal(client, broker, account_no, prd_id, old_ticket, ticket, old_lot, lot)
    if id < 0:
        return id, error
    else:
        return id
