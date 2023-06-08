import databases
import hashlib
import datetime
import asyncio


# Define database URL
DATABASE_URL = "sqlite:///./test.db"


class DB:

    def __init__(self):
        self.database = databases.Database(DATABASE_URL)

    async def connect(self):
        try:
            await self.database.connect()
            print("Connected to the database")
            await self.create_tables()
        except Exception as e:
            print("Failed to connect to the database:", e)

    async def create_tables(self):
        clients = """
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY,
                name TEXT,
                password TEXT,
                full_name TEXT,
                email TEXT,
                expire INTEGER
            )
        """

        client_deals = """
            CREATE TABLE IF NOT EXISTS client_deals (
                id INTEGER PRIMARY KEY,
                prd_id INTEGER,
                cli_id INTEGER,
                prd_ticket BIGINT,
                status SMALLINT,
                account_no BIGINT,
                broker TEXT,
                ticket BIGINT,
                type SMALLINT,
                symbol TEXT,
                lot REAL,
                equity REAL,
                balance REAL,
                open_time BIGINT,
                open_price REAL,
                sl REAL,
                tp REAL,
                close_time BIGINT,
                close_price REAL,
                order_profit REAL,
                order_commission REAL,
                order_swap REAL,
                catch_time DATETIME,
                register_time DATETIME,
                register_ip TEXT,
                computer_name TEXT,
                cld_id INTEGER
            )
        """

        providers = """
            CREATE TABLE IF NOT EXISTS providers (
                id INTEGER PRIMARY KEY,
                name TEXT,
                password TEXT,
                full_name TEXT,
                email TEXT
            )
        """

        provider_clients = """
            CREATE TABLE IF NOT EXISTS provider_clients (
                id INTEGER PRIMARY KEY,
                pro_id INTEGER,
                cli_id INTEGER,
                account_no BIGINT,
                from_date DATE,
                to_date DATE
            )
        """

        provider_deals = """
            CREATE TABLE IF NOT EXISTS provider_deals (
                id INTEGER PRIMARY KEY, 
                pro_id INTEGER, 
                status SMALLINT, 
                account_no BIGINT, 
                broker TEXT, 
                ticket BIGINT, 
                type SMALLINT, 
                symbol TEXT, 
                lot REAL, 
                equity REAL, 
                balance REAL, 
                open_time BIGINT, 
                open_price REAL, 
                sl REAL, 
                tp REAL, 
                close_time BIGINT, 
                close_price REAL, 
                order_profit REAL, 
                order_commission REAL, 
                order_swap REAL, 
                catch_time DATETIME, 
                register_time DATETIME, 
                register_ip TEXT, 
                computer_name TEXT, 
                prd_id INTEGER, 
                old_ticket BIGINT, 
                old_lot REAL
            )
        """

        users = """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY, 
                username TEXT, 
                password TEXT, 
                full_name TEXT, 
                email TEXT
            )
        """

        async with self.database.connection() as conn:
            async with conn.transaction():
                # Execute the SQL statements using a transaction object
                await conn.execute(clients)
                await conn.execute(client_deals)
                await conn.execute(providers)
                await conn.execute(provider_clients)
                await conn.execute(provider_deals)
                await conn.execute(users)

    # Define a method to add a provider
    async def add_provider(self, name: str, password: str, full_name: str, email: str):
        # Hash the password using md5
        password = hashlib.md5(password.encode()).hexdigest()
        # Create a query to insert a provider
        query = "INSERT INTO providers (name, password, full_name, email) VALUES (:name, :password, :full_name, :email)"
        values = {"name": name, "password": password, "full_name": full_name, "email": email}
        # Try to execute the query and get the last record id
        try:
            last_record_id = await self.database.execute(query=query, values=values)
            return last_record_id
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    

    # Define a method to authenticate a provider
    async def authenticate_provider(self, name: str, password: str):
        # Hash the password using md5
        password = hashlib.md5(password.encode()).hexdigest()
        # Create a query to select the id of the provider with the given name and password
        query = "SELECT id FROM providers WHERE name = :name AND password = :password"
        values = {"name": name, "password": password}
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            if row is None:
                print("Authentication failed")
                return self.authentication_failed
            else:
                return row["id"]
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    
    # Define a method to add a client
    async def add_client(self, name: str, password: str, full_name: str, email: str, expire: datetime.datetime):
        # Hash the password using md5
        password = hashlib.md5(password.encode()).hexdigest()
        # Create a query to insert a client
        query = "INSERT INTO clients (name, password, full_name, email, expire) VALUES (:name, :password, :full_name, :email, :expire)"
        values = {"name": name, "password": password, "full_name": full_name, "email": email, "expire": expire}
        # Try to execute the query and get the last record id
        try:
            last_record_id = await self.database.execute(query=query, values=values)
            return last_record_id
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    
    # Define a method to authenticate a client
    async def authenticate_client(self, name: str, password: str):
        # Hash the password using md5
        password = hashlib.md5(password.encode()).hexdigest()
        # Create a query to select the id of the client with the given name and password
        query = "SELECT id FROM clients WHERE name = :name AND password = :password"
        values = {"name": name, "password": password}
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            if row is None:
                print("Authentication failed")
                return self.authentication_failed
            else:
                return row["id"]
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    

    # Define a method to add a provider deal
    async def add_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int, symbol: str, type: int,
                                lot: float, balance: float, equity: float, open_time: int, open_price: float,
                                sl: float, tp: float, ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = "SELECT id FROM providers WHERE name = :provider"
        values = {"provider": provider}
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            if row is None:
                print("Provider not found")
                return self.provider_not_found
            else:
                pro_id = row["id"]
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order
        query = "SELECT id FROM provider_deals WHERE account_no = :account_no AND ticket = :ticket AND status = :status"
        values = {"account_no": account_no, "ticket": ticket, "status": self.new_order}
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query, values=values)
            if row is not None:
                print("Ticket already exists")
                return self.ticket_already_exist
            else:
                # Get the current date and time and subtract the delay in seconds
                date = datetime.datetime.now()
                date2 = date - datetime.timedelta(seconds=delay)
                msd = date.strftime("%Y-%m-%d %H:%M:%S")
                msd2 = date2.strftime("%Y-%m-%d %H:%M:%S")
                # Create a query to insert a provider deal with the given values
                query = """
                    INSERT INTO provider_deals (pro_id, status, account_no, broker, ticket, type, symbol, lot, equity, balance, open_time,
                    open_price, sl, tp, catch_time, register_time, register_ip, computer_name) 
                    VALUES (:pro_id, :status, :account_no, :broker, :ticket, :type, :symbol, :lot, :equity, :balance, :open_time,
                    :open_price, :sl, :tp, :catch_time, :register_time, :register_ip, :computer_name)
                """
                values = {
                    "pro_id": pro_id,
                    "status": self.new_order,
                    "account_no": account_no,
                    "broker": broker,
                    "ticket": ticket,
                    "type": type,
                    "symbol": symbol,
                    "lot": lot,
                    "equity": equity,
                    "balance": balance,
                    "open_time": open_time,
                    "open_price": open_price,
                    "sl": sl,
                    "tp": tp,
                    "catch_time": msd2,
                    "register_time": msd,
                    "register_ip": ip,
                    "computer_name": computer
                }
                # Try to execute the query and get the last record id
                try:
                    last_record_id = await self.database.execute(query=query, values=values)
                    return last_record_id
                # Handle any errors
                except Exception as e:
                    print("Invalid query:", query, values, e)
                    return -1
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
        
    # Define a method to modify a provider deal
    async def modify_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int, open_price: float, sl: float, tp: float, ip: str,
                                computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = "SELECT id FROM providers WHERE name = :provider"
        values = {"provider": provider}
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            if row is None:
                print("Provider not found")
                return self.provider_not_found
            else:
                pro_id = row["id"]
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status modify_order and open_price and sl and tp
        query = """
            SELECT id FROM provider_deals WHERE account_no = :account_no AND ticket = :ticket AND status = :status AND
            open_price = :open_price AND sl = :sl AND tp = :tp
        """
        values = {
            "account_no": account_no,
            "ticket": ticket,
            "status": self.modify_order,
            "open_price": open_price,
            "sl": sl,
            "tp": tp
        }
        try:
            row = await self.database.fetch_one(query=query, values=values)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order or sub_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = :account_no AND ticket = :ticket AND
            (status = :new_order OR status = :sub_order)
        """
        values = {
            "account_no": account_no,
            "ticket": ticket,
            "new_order": self.new_order,
            "sub_order": self.sub_order
        }
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            if row is None:
                print("Parent ticket not found")
                return self.parent_ticket_not_found
            else:
                prd_id = row["id"]
                # Get the current date and time and subtract the delay in seconds
                date = datetime.datetime.now()
                date2 = date - datetime.timedelta(seconds=delay)
                msd = date.strftime("%Y-%m-%d %H:%M:%S")
                msd2 = date2.strftime("%Y-%m-%d %H:%M:%S")
            
                # Create a query to insert a provider deal with the given values
                query = """
                    INSERT INTO provider_deals (pro_id, status, account_no, broker, ticket, type, symbol, lot, equity, balance, open_time,
                    open_price, sl, tp, catch_time, register_time, register_ip, computer_name, prd_id) 
                    VALUES (:pro_id, :status, :account_no, :broker, :ticket, :type, :symbol, :lot, :equity, :balance, :open_time,
                    :open_price, :sl, :tp, :catch_time, :register_time, :register_ip, :computer_name, :prd_id)
                """
                values = {
                    "pro_id": pro_id,
                    "status": self.modify_order,
                    "account_no": account_no,
                    "broker": broker,
                    "ticket": ticket,
                    "type": 0,
                    "symbol": "",
                    "lot": 0.0,
                    "equity": 0.0,
                    "balance": 0.0,
                    "open_time": 0,
                    "open_price": open_price,
                    "sl": sl,
                    "tp": tp,
                    "catch_time": msd2,
                    "register_time": msd,
                    "register_ip": ip,
                    "computer_name": computer,
                    "prd_id": prd_id
                }
                # Try to execute the query and get the last record id
                try:
                    last_record_id = await self.database.execute(query=query, values=values)
                    return last_record_id
                # Handle any errors
                except Exception as e:
                    print("Invalid query:", query, values, e)
                    return -1
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
        
    
    # Define a method to close a provider deal
    async def close_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int,
                                  close_price: float, profit: float, commission: float,
                                  swap: float, ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = "SELECT id FROM providers WHERE name = :provider"
        values = {"provider": provider}
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            
            if row is None:
                print("Provider not found")
                return self.provider_not_found
                
            pro_id = row["id"]
            
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status close_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = :account_no AND ticket = :ticket AND status = :status
        """
        values = {
            "account_no": account_no,
            "ticket": ticket,
            "status": self.close_order
        }
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order or sub_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = :account_no AND ticket = :ticket AND
            (status = :new_order OR status = :sub_order)
        """
        values = {
            "account_no": account_no,
            "ticket": ticket,
            "new_order": self.new_order,
            "sub_order": self.sub_order
        }
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            
            if row is None:
                print("Parent ticket not found")
                return self.parent_ticket_not_found
                
            prd_id = row["id"]
            
            # Get the current date and time and subtract the delay in seconds
            date = datetime.datetime.now()
            date2 = date - datetime.timedelta(seconds=delay)
            msd = date.strftime("%Y-%m-%d %H:%M:%S")
            msd2 = date2.strftime("%Y-%m-%d %H:%M:%S")

            # Create a query to insert a provider deal with the given values
            query = """
                INSERT INTO provider_deals (pro_id, status, account_no, broker, ticket, type, symbol, lot, equity, balance, open_time,
                open_price, sl, tp, close_time, close_price, order_profit, commission, order_swap, catch_time, register_time,
                register_ip, computer_name, prd_id) 
                VALUES (:pro_id, :status, :account_no, :broker, :ticket, :type, :symbol, :lot, :equity, :balance, :open_time,
                :open_price, :sl, :tp, :close_time, :close_price, :order_profit, :commission, :order_swap, :catch_time, :register_time,
                :register_ip, :computer_name, :prd_id)
            """
            
            values = {
                "pro_id": pro_id,
                "status": self.close_order,
                "account_no": account_no,
                "broker": broker,
                "ticket": ticket,
                "type": 0,
                "symbol": "",
                "lot": 0.0,
                "equity": 0.0,
                "balance": 0.0,
                "open_time": 0,
                "open_price": 0.0,
                "sl": 0.0,
                "tp": 0.0,
                "close_time": msd2,
                "close_price": close_price,
                "order_profit": profit,
                "commission": commission,
                "order_swap": swap,
                "catch_time": msd2,
                "register_time": msd,
                "register_ip": ip,
                "computer_name": computer,
                "prd_id": prd_id
            }
            
            # Try to execute the query and get the last record id
            try:
                last_record_id = await self.database.execute(query=query, values=values)
                return last_record_id
                
            # Handle any errors
            except Exception as e:
                print("Invalid query:", query, values, e)
                return -1
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
        
    # Define a method to delete a provider deal
    async def delete_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int,
                                   ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = "SELECT id FROM providers WHERE name = :provider"
        values = {"provider": provider}
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            
            if row is None:
                print("Provider not found")
                return self.provider_not_found
                
            pro_id = row["id"]
            
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status delete_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = :account_no AND ticket = :ticket AND status = :status
        """
        values = {
            "account_no": account_no,
            "ticket": ticket,
            "status": self.delete_order
        }
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = :account_no AND ticket = :ticket AND status = :status
        """
        values = {
            "account_no": account_no,
            "ticket": ticket,
            "status": self.new_order
        }
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            
            if row is None:
                print("Parent ticket not found")
                return self.parent_ticket_not_found
                
            prd_id = row["id"]
            
            # Get the current date and time and subtract the delay in seconds
            date = datetime.datetime.now()
            date2 = date - datetime.timedelta(seconds=delay)
            msd = date.strftime("%Y-%m-%d %H:%M:%S")
            msd2 = date2.strftime("%Y-%m-%d %H:%M:%S")

            # Create a query to insert a provider deal with the given values
            query = """
                INSERT INTO provider_deals (pro_id, status, account_no, broker, ticket, type, symbol, lot, equity, balance, open_time,
                open_price, sl, tp, close_time, close_price, order_profit, commission, order_swap, catch_time, register_time,
                register_ip, computer_name, prd_id) 
                VALUES (:pro_id, :status, :account_no, :broker, :ticket, :type, :symbol, :lot, :equity, :balance, :open_time,
                :open_price, :sl, :tp, :close_time, :close_price, :order_profit, :commission, :order_swap, :catch_time, :register_time,
                :register_ip, :computer_name, :prd_id)
            """
            
            values = {
                "pro_id": pro_id,
                "status": self.delete_order,
                "account_no": account_no,
                "broker": broker,
                "ticket": ticket,
                "type": 0,
                "symbol": "",
                "lot": 0.0,
                "equity": 0.0,
                "balance": 0.0,
                "open_time": 0,
                "open_price": 0.0,
                "sl": 0.0,
                "tp": 0.0,
                "close_time": msd2,
                "close_price": 0.0,
                "order_profit": 0.0,
                "commission": 0.0,
                "order_swap": 0.0,
                "catch_time": msd2,
                "register_time": msd,
                "register_ip": ip,
                "computer_name": computer,
                "prd_id": prd_id
            }
            
            # Try to execute the query and get the last record id
            try:
                last_record_id = await self.database.execute(query=query, values=values)
                return last_record_id
                
            # Handle any errors
            except Exception as e:
                print("Invalid query:", query, values, e)
                return -1
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    
    # Define a method to insert a provider deal as a sub order
    async def suborder_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int,
                                    new_ticket: int, old_lot: float, new_lot: float,
                                    ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = "SELECT id FROM providers WHERE name = :provider"
        values = {"provider": provider}
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            if row is None:
                print("Provider not found")
                return self.provider_not_found
            else:
                pro_id = row["id"]
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and new_ticket and status sub_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = :account_no AND new_ticket = :new_ticket AND status = :status
        """
        values = {
            "account_no": account_no,
            "new_ticket": new_ticket,
            "status": self.sub_order
        }
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order or sub_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = :account_no AND ticket = :ticket AND (status = :new_order OR status = :sub_order)
        """
        values = {
            "account_no": account_no,
            "ticket": ticket,
            "new_order": self.new_order,
            "sub_order": self.sub_order
        }
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query=query, values=values)
            if row is None:
                print("Parent ticket not found")
                return self.parent_ticket_not_found
            else:
                prd_id = row["id"]
                
            # Get the current date and time and subtract the delay in seconds
            date = datetime.datetime.now()
            date2 = date - datetime.timedelta(seconds=delay)
            msd = date.strftime("%Y-%m-%d %H:%M:%S")
            msd2 = date2.strftime("%Y-%m-%d %H:%M:%S")
            
            # Create a query to insert a provider deal with the given values
            query = """
                INSERT INTO provider_deals (pro_id, status, account_no, broker, ticket, type, symbol, lot, equity, balance, open_time,
                open_price, sl, tp, close_time, close_price, order_profit, commission, order_swap, catch_time, register_time,
                register_ip, computer_name, prd_id, new_ticket, old_ticket, old_lot) 
                VALUES (:pro_id, :status, :account_no, :broker, :new_ticket, :type, :symbol, :lot, :equity, :balance, :open_time,
                :open_price, :sl, :tp, :close_time, :close_price, :order_profit, :commission, :order_swap, :catch_time, :register_time,
                :register_ip, :computer_name, :prd_id, :new_ticket, :ticket, :old_lot)
            """
            
            values = {
                "pro_id": pro_id,
                "status": self.sub_order,
                "account_no": account_no,
                "broker": broker,
                "new_ticket": new_ticket,
                "type": 0,
                "symbol": "",
                "lot": new_lot,
                "equity": 0.0,
                "balance": 0.0,
                "open_time": 0,
                "open_price": 0.0,
                "sl": 0.0,
                "tp": 0.0,
                "close_time": msd2,
                "close_price": 0.0,
                "order_profit": 0.0,
                "commission": 0.0,
                "order_swap": 0.0,
                "catch_time": msd2,
                "register_time": msd,
                "register_continuation": ip,
                "computer_name": computer,
                "prd_id": prd_id,
                "ticket": ticket,
                "old_lot": old_lot
            }
            
            # Try to execute the query and get the last record id
            try:
                last_record_id = await self.database.execute(query=query, values=values)
                return last_record_id
                
            # Handle any errors
            except Exception as e:
                print("Invalid query:", query, values, e)
                return -1
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
        
    
    
