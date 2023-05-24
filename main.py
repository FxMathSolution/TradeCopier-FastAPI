from fastapi import FastAPI, Request
from client import *
from provider import *
from get_client_deals import *

app = FastAPI()

@app.get("/")
def home():
    return "Welcome to TradeCopier-FastAPI"

@app.get("/client")
def client(request: Request):

    # Get the parameters from the request
    status = request.query_params.get("status", type=int)
    client = request.query_params.get("client")
    broker = request.query_params.get("broker")
    account_no = request.query_params.get("account_no")
    computer = request.query_params.get("computer")
    ip = request.query_params.get("ip")
    prd_id = request.query_params.get("prd_id")
    ticket = request.query_params.get("ticket")
    delay = request.query_params.get("delay", type=int)

    # Validate the parameters
    if not all([status, client, broker, account_no, ticket, ip, computer, delay]):
        return "-1<br>invalid parameter format"

    # Handle different status cases
    if status == config.new_order:
        symbol = request.query_params.get("symbol")
        type = request.query_params.get("type")
        lot = request.query_params.get("lot")
        balance = request.query_params.get("balance")
        equity = request.query_params.get("equity")
        open_time = request.query_params.get("open_time")
        open_price = request.query_params.get("open_price")
        sl = request.query_params.get("sl")
        tp = request.query_params.get("tp")

        # Validate the parameters
        if not all([symbol, type, lot, balance, equity, open_time, open_price, sl, tp]):
            return "-1<br>invalid parameter format"

        # Return the order details
        return {
            "status": status,
            "client": client,
            "broker": broker,
            "account_no": account_no,
            "computer": computer,
            "ip": ip,
            "prd_id": prd_id,
            "ticket": ticket,
            "delay": delay,
            "open_time": open_time,
            "open_price": open_price,
            "sl": sl,
            "tp": tp
        }

    else:
        return "-1<br>invalid status"


    if status == config.new_order:

        # Call the client function
        id, error = add_client_deal(
            client, broker, account_no, prd_id, ticket, symbol, type, lot,
            balance, equity, open_time, open_price, sl, tp, ip, computer, delay
        )

        # Return the result
        if id < 0:
            return f"{id}<br>{error}"
        else:
            return f"{id}<br>succeeded"

    elif status == config.close_order:

        # Get the parameters from the request
        close_price = request.query_params.get("close_price")
        profit = request.query_params.get("profit")
        commission = request.query_params.get("commission")
        swap = request.query_params.get("swap")

        # Validate the parameters
        if not all([close_price, profit, commission, swap]):
            return "-1<br>invalid parameter format"

        # Call the client function
        id, error = close_client_deal(client, broker, account_no, prd_id, ticket, close_price, profit, commission, swap)

        # Return the result
        if id < 0:
            return f"{id}<br>{error}"
        else:
            return f"{id}<br>succeeded"

    elif status == config.delete_order:

        # Get the parameters from the request
        ticket = request.query_params.get("ticket")

        # Call the client function
        id, error = delete_client_deal(client, broker, account_no, prd_id, ticket)

        # Return the result
        if id < 0:
            return f"{id}<br>{error}"
        else:
            return f"{id}<br>succeeded"

    elif status == config.modify_order:

        # Get the parameters from the request
        open_price = request.query_params.get("open_price")
        sl = request.query_params.get("sl")
        tp = request.query_params.get("tp")

        # Validate the parameters
        if not all([open_price, sl, tp]):
            return "-1<br>invalid parameter format"

        # Call the client function
        id, error = modify_client_deal(client, broker, account_no, prd_id, ticket, open_price, sl, tp)

        # Return the result
        if id < 0:
            return f"{id}<br>{error}"
        else:
            return f"{id}<br>succeeded"
        

@app.get("/provider")
def provider(request: Request):

    # Get the parameters from the request
    status = request.query_params.get("status", type=int)
    provider = request.query_params.get("provider")
    broker = request.query_params.get("broker")
    account_no = request.query_params.get("account_no")
    ticket = request.query_params.get("ticket")
    ip = request.query_params.get("ip")
    computer = request.query_params.get("computer")
    delay = request.query_params.get("delay", type=int)

    # Validate the parameters
    if not all([status, provider, broker, account_no, ticket, ip, computer, delay]):
        return "-1<br>invalid parameter format"

    # Handle different status cases
    if status == config.new_order:

        # Get the parameters from the request
        symbol = request.query_params.get("symbol")
        type = request.query_params.get("type")
        lot = request.query_params.get("lot")
        balance = request.query_params.get("balance")
        equity = request.query_params.get("equity")
        open_time = request.query_params.get("open_time")
        open_price = request.query_params.get("open_price")
        sl = request.query_params.get("sl")
        tp = request.query_params.get("tp")

        # Validate the parameters
        if not all([symbol, type, lot, balance, equity, open_time, open_price, sl, tp]):
            return "-1<br>invalid parameter format"

        # Call the provider function
        id, error = add_provider_deal(
            provider, broker, account_no, ticket, symbol, type, lot,
            balance, equity, open_time, open_price, sl, tp, ip, computer, delay
        )

        # Return the result
        if id < 0:
            return f"{id}<br>{error}"
        else:
            return f"{id}<br>succeeded"

    elif status == config.close_order:

        # Get the parameters from the request
        close_price = request.query_params.get("close_price")
        profit = request.query_params.get("profit")
        commission = request.query_params.get("commission")
        swap = request.query_params.get("swap")

        # Validate the parameters
        if not all([close_price, profit, commission, swap]):
            return "-1<br>invalid parameter format"

        # Call the provider function
        id, error = close_provider_deal(
            provider, broker, account_no, ticket, close_price, profit, commission, swap
        )

        # Return the result
        if id < 0:
            return f"{id}<br>{error}"
        else:
            return f"{id}<br>succeeded"
        
    
    elif status == config.delete_order:

        # Call the provider function
        id, error = delete_provider_deal(provider, broker, account_no, ticket)

        # Return the result
        if id < 0:
            return f"{id}<br>{error}"
        else:
            return f"{id}<br>succeeded"

    elif status == config.modify_order:

        # Get the parameters from the request
        open_price = request.query_params.get("open_price")
        sl = request.query_params.get("sl")
        tp = request.query_params.get("tp")

        # Validate the parameters
        if not all([open_price, sl, tp]):
            return "-1<br>invalid parameter format"

