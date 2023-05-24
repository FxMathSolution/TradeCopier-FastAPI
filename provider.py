import config
from db import db


def add_provider_deal(provider, broker, account_no, ticket, symbol, type, lot, balance, equity, open_time, open_price, sl, tp, ip, computer, delay):
    ip = ip[:50]
    db = db()
    error = ""
    id = db.add_provider_deal(provider, broker, account_no, ticket, symbol, type, lot, balance, equity, open_time, open_price, sl, tp, ip, computer, delay)
    if id < 0:
        return id, error
    else:
        return id


def close_provider_deal(provider, broker, account_no, ticket, close_price, profit, commission, swap):
    db = db()
    error = ""
    id = db.close_provider_deal(provider, broker, account_no, ticket, close_price, profit, commission, swap)
    if id < 0:
        return id, error
    else:
        return id


def delete_provider_deal(provider, broker, account_no, ticket):
    db = db()
    error = ""
    id = db.delete_provider_deal(provider, broker, account_no, ticket)
    if id < 0:
        return id, error
    else:
        return id


def modify_provider_deal(provider, broker, account_no, ticket, open_price, sl, tp):
    db = db()
    error = ""
    id = db.modify_provider_deal(provider, broker, account_no, ticket, open_price, sl, tp)
    if id < 0:
        return id, error
    else:
        return id


def suborder_provider_deal(provider, broker, account_no, old_ticket, ticket, old_lot, lot):
    db = db()
    error = ""
    id = db.suborder_provider_deal(provider, broker, account_no, old_ticket, ticket, old_lot, lot)
    if id < 0:
        return id, error
    else:
        return id
