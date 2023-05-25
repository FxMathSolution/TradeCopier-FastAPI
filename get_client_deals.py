import config
from db import db
import datetime
from fastapi import FastAPI, Request, Depends


async def get_client_deals(request: Request):
    client = request.query_params.get("client")
    account_no = request.query_params.get("account_no")
    delay = request.query_params.get("delay")
    exceptions = request.query_params.get("exceptions")

    if not (client and account_no and delay):
        return "-1<br>invalid parameter format"

    if not exceptions:
        exceptions = "0"

    db = db()
    deals = []
    deals_count = 0
    error = ""
    id = db.get_client_deals(client, account_no, delay, exceptions, deals, deals_count, error)

    if id < 0:
        return f"{id}<br>{error}"

    response = f"{deals_count}<br>{deals_count} deals found<br>"

    for deal in deals:
        date = datetime.datetime.now()
        date2 = deal.catch_time
        late = date.timestamp() - date2.timestamp()
        deal_str = ""

        if deal.status == config.new_order:
            deal_str = f"{config.new_order},{deal.id},{late},{deal.type},"
            deal_str += f"{deal.lot},{deal.open_price},{deal.sl},"
            deal_str += f"{deal.tp},{deal.symbol},{deal.balance},"
            deal_str += f"{deal.equity}<br>"
        elif deal.status == config.close_order:
            deal_str = f"{config.close_order},{deal.id},{late},{deal.cld_ticket},"
            deal_str += f"{deal.close_price}<br>"
        elif deal.status == config.delete_order:
            deal_str = f"{config.delete_order},{deal.id},{late},{deal.cld_ticket}<br>"
        elif deal.status == config.modify_order:
            deal_str = f"{config.modify_order},{deal.id},{late},{deal.cld_ticket},"
            deal_str += f"{deal.open_price},{deal.sl},{deal.tp}<br>"
        elif deal.status == config.sub_order:
            deal_str = f"{config.sub_order},{deal.id},{late},{deal.cld_ticket},"
            deal_str += f"{deal.lot},{deal.old_lot}<br>"

        response += deal_str

    return response
