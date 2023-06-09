import asyncpg
import asyncio
import hashlib
import datetime


class DB:

    def __init__(self):
        self.connection = None

    async def connect(self):
        try:
            self.connection = await asyncpg.connect(
                user="your_username",
                password="your_password",
                database="your_database",
                host="your_host",
                port="your_port"
            )
            print("Connected to the database")
            await self.create_tables()
        except Exception as e:
            print("Failed to connect to the database:", e)

    async def create_tables(self):
        clients = """
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                name TEXT,
                password TEXT,
                full_name TEXT,
                email TEXT,
                expire TIMESTAMP
            )
        """

        client_deals = """
            CREATE TABLE IF NOT EXISTS client_deals (
                id SERIAL PRIMARY KEY,
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
                catch_time TIMESTAMP,
                register_time TIMESTAMP,
                register_ip TEXT,
                computer_name TEXT,
                cld_id INTEGER
            )
        """

        providers = """
            CREATE TABLE IF NOT EXISTS providers (
                id SERIAL PRIMARY KEY,
                name TEXT,
                password TEXT,
                full_name TEXT,
                email TEXT
            )
        """

        provider_clients = """
            CREATE TABLE IF NOT EXISTS provider_clients (
                id SERIAL PRIMARY KEY,
                pro_id INTEGER,
                cli_id INTEGER,
                account_no BIGINT,
                from_date DATE,
                to_date DATE
            )
        """

        provider_deals = """
            CREATE TABLE IF NOT EXISTS provider_deals (
                id SERIAL PRIMARY KEY, 
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
                catch_time TIMESTAMP, 
                register_time TIMESTAMP, 
                register_ip TEXT, 
                computer_name TEXT, 
                prd_id INTEGER, 
                old_ticket BIGINT, 
                old_lot REAL
            )
        """

        users = """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY, 
                username TEXT, 
                password TEXT, 
                full_name TEXT, 
                email TEXT
            )
        """

        async with self.connection.transaction():
            # Execute the SQL statements using a transaction object
            await self.connection.execute(clients)
            await self.connection.execute(client_deals)
            await self.connection.execute(providers)
            await self.connection.execute(provider_clients)
            await self.connection.execute(provider_deals)
            await self.connection.execute(users)

    async def authenticate_provider(self, name: str, password: str):
        # Hash the password using md5
        password = hashlib.md5(password.encode()).hexdigest()
        # Create a query to select the id of the provider with the given name and password
        query = "SELECT id FROM providers WHERE name = $1 AND password = $2"
        values = (name, password)
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
            if row is None:
                print("Authentication failed")
                return self.authentication_failed
            else:
                return row["id"]
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

    async def add_client(self, name: str, password: str, full_name: str, email: str, expire: datetime.datetime):
        # Hash the password using md5
        password = hashlib.md5(password.encode()).hexdigest()
        # Create a query to insert a client
        query = "INSERT INTO clients (name, password, full_name, email, expire) VALUES ($1, $2, $3, $4, $5) RETURNING id"
        values = (name, password, full_name, email, expire)
        # Try to execute the query and get the last record id
        try:
            last_record_id = await self.connection.fetchval(query, *values)
            return last_record_id
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
        
    
    async def authenticate_client(self, name: str, password: str):
        # Hash the password using md5
        password = hashlib.md5(password.encode()).hexdigest()
        # Create a query to select the id of the client with the given name and password
        query = "SELECT id FROM clients WHERE name = $1 AND password = $2"
        values = (name, password)
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
            if row is None:
                print("Authentication failed")
                return self.authentication_failed
            else:
                return row["id"]
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    
    async def add_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int, symbol: str, type: int,
                            lot: float, balance: float, equity: float, open_time: int, open_price: float,
                            sl: float, tp: float, ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = "SELECT id FROM providers WHERE name = $1"
        values = (provider,)
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
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
        query = "SELECT id FROM provider_deals WHERE account_no = $1 AND ticket = $2 AND status = $3"
        values = (account_no, ticket, self.new_order)
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
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
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                    RETURNING id
                """
                values = (
                    pro_id,
                    self.new_order,
                    account_no,
                    broker,
                    ticket,
                    type,
                    symbol,
                    lot,
                    equity,
                    balance,
                    open_time,
                    open_price,
                    sl,
                    tp,
                    msd2,
                    msd,
                    ip,
                    computer
                )
                # Try to execute the query and get the last record id
                try:
                    last_record_id = await self.connection.fetchval(query, *values)
                    return last_record_id
                # Handle any errors
                except Exception as e:
                    print("Invalid query:", query, values, e)
                    return -1
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    


    async def modify_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int, open_price: float, sl: float, tp: float, ip: str,
                                computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = "SELECT id FROM providers WHERE name = $1"
        values = (provider,)
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
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
            SELECT id FROM provider_deals WHERE account_no = $1 AND ticket = $2 AND status = $3 AND
            open_price = $4 AND sl = $5 AND tp = $6
        """
        values = (account_no, ticket, self.modify_order, open_price, sl, tp)
        try:
            row = await self.connection.fetchrow(query, *values)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order or sub_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = $1 AND ticket = $2 AND
            (status = $3 OR status = $4)
        """
        values = (account_no, ticket, self.new_order, self.sub_order)
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
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
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                """
                values = (
                    pro_id,
                    self.modify_order,
                    account_no,
                    broker,
                    ticket,
                    0,
                    "",
                    0.0,
                    0.0,
                    0.0,
                    0,
                    open_price,
                    sl,
                    tp,
                    msd2,
                    msd,
                    ip,
                    computer,
                    prd_id
                )
                # Try to execute the query and get the last record id
                try:
                    await self.connection.execute(query, *values)
                    return ticket
                # Handle any errors
                except Exception as e:
                    print("Invalid query:", query, values, e)
                    return -1
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    

    async def close_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int,
                              close_price: float, profit: float, commission: float,
                              swap: float, ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = "SELECT id FROM providers WHERE name = $1"
        values = (provider,)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
            
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
            SELECT id FROM provider_deals WHERE account_no = $1 AND ticket = $2 AND status = $3
        """
        values = (account_no, ticket, self.close_order)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
            
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order or sub_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = $1 AND ticket = $2 AND
            (status = $3 OR status = $4)
        """
        values = (account_no, ticket, self.new_order, self.sub_order)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
            
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
                open_price, sl, tp, close_time, close_price, order_profit, order_commission, order_swap, catch_time, register_time,
                register_ip, computer_name, prd_id) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
                RETURNING id
            """
            
            values = (
                pro_id,
                self.close_order,
                account_no,
                broker,
                ticket,
                0,
                "",
                0.0,
                0.0,
                0.0,
                0,
                0.0,
                0.0,
                0.0,
                msd2,
                close_price,
                profit,
                commission,
                swap,
                msd2,
                msd,
                ip,
                computer,
                prd_id
            )
            
            # Try to execute the query and get the last record id
            try:
                last_record_id = await self.connection.fetchval(query, *values)
                return last_record_id
                
            # Handle any errors
            except Exception as e:
                print("Invalid query:", query, values, e)
                return -1
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    

    async def delete_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int,
                                   ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = "SELECT id FROM providers WHERE name = $1"
        values = (provider,)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
            
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
            SELECT id FROM provider_deals WHERE account_no = $1 AND ticket = $2 AND status = $3
        """
        values = (account_no, ticket, self.delete_order)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
            
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = $1 AND ticket = $2 AND status = $3
        """
        values = (account_no, ticket, self.new_order)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
            
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
                open_price, sl, tp, close_time, close_price, order_profit, order_commission, order_swap, catch_time, register_time,
                register_ip, computer_name, prd_id) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
            """
            
            values = (
                pro_id,
                self.delete_order,
                account_no,
                broker,
                ticket,
                0,
                "",
                0.0,
                0.0,
                0.0,
                0,
                0.0,
                0.0,
                0.0,
                msd2,
                0.0,
                0.0,
                0.0,
                0.0,
                msd2,
                msd,
                ip,
                computer,
                prd_id
            )
            
            # Try to execute the query and get the last record id
            try:
                last_record_id = await self.connection.execute(query, *values)
                return last_record_id
                
            # Handle any errors
            except Exception as e:
                print("Invalid query:", query, values, e)
                return -1
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    

    async def suborder_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int,
                                 new_ticket: int, old_lot: float, new_lot: float,
                                 ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = "SELECT id FROM providers WHERE name = $1"
        values = (provider,)

        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
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
            SELECT id FROM provider_deals WHERE account_no = $1 AND new_ticket = $2 AND status = $3
        """
        values = (account_no, new_ticket, self.sub_order)

        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated

        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order or sub_order
        query = """
            SELECT id FROM provider_deals WHERE account_no = $1 AND ticket = $2 AND (status = $3 OR status = $4)
        """
        values = (account_no, ticket, self.new_order, self.sub_order)

        # Try to execute the query and fetch one row
        try:
            row = await self.connection.fetchrow(query, *values)
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
                open_price, sl, tp, close_time, close_price, order_profit, order_commission, order_swap, catch_time, register_time,
                register_ip, computer_name, prd_id, new_ticket, old_ticket, old_lot) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
            """

            values = (
                pro_id,
                self.sub_order,
                account_no,
                broker,
                new_ticket,
                0,
                "",
                new_lot,
                0.0,
                0.0,
                0,
                0.0,
                0.0,
                0.0,
                msd2,
                0.0,
                0.0,
                0.0,
                0.0,
                msd2,
                ip,
                computer,
                prd_id,
                new_ticket,
                ticket,
                old_lot
            )

            # Try to execute the query and get the last record id
            try:
                last_record_id = await self.connection.execute(query, *values)
                return last_record_id

            # Handle any errors
            except Exception as e:
                print("Invalid query:", query, values, e)
                return -1

        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
    

    async def add_client_deal(self, client: str, broker: str, account_no: int, prd_id: int, ticket: int, symbol: str, type: int,
                          lot: float, balance: float, equity: float, open_time: int, open_price: float,
                          sl: float, tp: float, ip: str, computer: str, delay: int):
        # Create a query to select the id of the client with the given name
        query = "SELECT id FROM clients WHERE name = $1"
        values = (client,)
        try:
            row = await self.connection.fetchrow(query, *values)
            if row is None:
                print("Client not found")
                return self.client_not_found
            else:
                cli_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the ticket of the provider deal with the given id
        query = "SELECT ticket FROM provider_deals WHERE id = $1"
        values = (prd_id,)
        try:
            row = await self.connection.fetchrow(query, *values)
            if row is None:
                print("Provider deal not found")
                return self.provider_deal_not_found
            else:
                prd_ticket = row["ticket"]
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status new_order
        query = "SELECT id FROM client_deals WHERE account_no = $1 AND ticket = $2 AND status = $3"
        values = (account_no, ticket, self.new_order)
        try:
            row = await self.connection.fetchrow(query, *values)
            if row is not None:
                print("Ticket already exists")
                return self.ticket_already_exist
            else:
                # Get the current date and time and subtract the delay in seconds
                date = datetime.datetime.now()
                date2 = date - datetime.timedelta(seconds=delay)
                msd = date.strftime("%Y-%m-%d %H:%M:%S")
                msd2 = date2.strftime("%Y-%m-%d %H:%M:%S")
                # Create a query to insert a client deal with the given values
                query = """
                    INSERT INTO client_deals (prd_id, cli_id, prd_ticket, status, account_no, broker, ticket, type, symbol, lot, equity, balance, open_time, open_price, sl, tp, catch_time, register_time, register_ip, computer_name) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                    RETURNING id
                """
                values = (
                    prd_id, cli_id, prd_ticket, self.new_order, account_no, broker,
                    ticket, type, symbol, lot,
                    equity, balance, open_time,
                    open_price, sl, tp,
                    msd2, msd, ip,
                    computer
                )
                # Try to execute the query and get the last record id
                try:
                    last_record_id = await self.connection.fetchval(query, *values)
                    return last_record_id
                except Exception as e:
                    print("Invalid query:", query, values, e)
                    return -1
        except Exception as e:
            print("Invalid query:", query, values, e)
            return -1
        
    
    

    async def modify_client_deal(self, client, broker, account_no, prd_id, ticket, open_price, sl, tp, ip, computer, delay):
        # Create a query to select the id of the client with the given name
        query = """
            SELECT id FROM clients WHERE name = %s
        """
        try:
            row = await self.database.fetch_one(query, client)
            if row is None:
                print("Client not found")
                return self.client_not_found
            else:
                cli_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status modify_order
        # and open_price and sl and tp
        query = """
            SELECT id FROM client_deals WHERE 
            account_no = %s AND 
            ticket = %s AND 
            status = %s AND 
            open_price = %s AND 
            sl = %s AND 
            tp = %s
        """
        try:
            row = await self.database.fetch_one(query, account_no, ticket, self.modify_order, open_price, sl, tp)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status new_order or sub_order
        query = """
            SELECT id FROM client_deals WHERE 
            account_no = %s AND 
            ticket = %s AND 
            (status = %s OR status = %s)
        """
        try:
            row = await self.database.fetch_one(query, account_no, ticket, self.new_order, self.sub_order)
            if row is None:
                print("Parent ticket not found")
                return self.parent_ticket_not_found
            else:
                cld_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Get the current date and time and subtract the delay in seconds
        date = datetime.datetime.now()
        date2 = date - datetime.timedelta(seconds=delay)
        msd = date.strftime("%Y-%m-%d %H:%M:%S")
        msd2 = date2.strftime("%Y-%m-%d %H:%M:%S")

        # Create a query to insert a client deal with the given values
        query = """
            INSERT INTO client_deals (prd_id, cli_id, status, account_no, broker, ticket, open_price, sl, tp, catch_time, register_time, register_ip, computer_name, cld_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            await self.database.execute(query, 
                                        prd_id, cli_id, self.modify_order, account_no, broker, ticket, open_price, sl, tp, 
                                        msd2, msd, ip, computer, cld_id)
            return True
        except Exception as e:
            print("Invalid query:", query, e)
            return False
    

    

    async def close_client_deal(self, client, broker, account_no, prd_id, ticket, close_price, profit, commission, swap, ip, computer, delay):
        # Create a query to select the id of the client with the given name
        query = """
            SELECT id FROM clients WHERE name = %s
        """
        try:
            row = await self.database.fetch_one(query, client)
            if row is None:
                print("Client not found")
                return self.client_not_found
            else:
                cli_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status close_order
        query = """
            SELECT id FROM client_deals WHERE 
            account_no = %s AND 
            ticket = %s AND 
            status = %s
        """
        try:
            row = await self.database.fetch_one(query, account_no, ticket, self.close_order)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status new_order or sub_order
        query = """
            SELECT id FROM client_deals WHERE 
            account_no = %s AND 
            ticket = %s AND 
            (status = %s OR status = %s)
        """
        try:
            row = await self.database.fetch_one(query, account_no, ticket, self.new_order, self.sub_order)
            if row is None:
                print("Parent ticket not found")
                return self.parent_ticket_not_found
            else:
                cld_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Get the current date and time and subtract the delay in seconds
        date = datetime.datetime.now()
        date2 = date - datetime.timedelta(seconds=delay)
        msd = date.strftime("%Y-%m-%d %H:%M:%S")
        msd2 = date2.strftime("%Y-%m-%d %H:%M:%S")

        # Create a query to insert a client deal with the given values
        query = """
            INSERT INTO client_deals (prd_id, cli_id, status, account_no, broker, ticket, close_price, order_profit, order_commission, order_swap, catch_time, register_time, register_ip, computer_name, cld_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            await self.database.execute(query, 
                                        prd_id, cli_id, self.close_order, account_no, broker, ticket, close_price, profit, commission, swap, 
                                        msd2, msd, ip, computer, cld_id)
            return True
        except Exception as e:
            print("Invalid query:", query, e)
            return False
        
    
   

    async def delete_client_deal(self, client, broker, account_no, prd_id, ticket, ip, computer, delay):
        # Create a query to select the id of the client with the given name
        query = """
            SELECT id FROM clients WHERE name = %s
        """
        try:
            row = await self.database.fetch_one(query, client)
            if row is None:
                print("Client not found")
                return self.client_not_found
            else:
                cli_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status delete_order
        query = """
            SELECT id FROM client_deals WHERE 
            account_no = %s AND 
            ticket = %s AND 
            status = %s
        """
        try:
            row = await self.database.fetch_one(query, account_no, ticket, self.delete_order)
            if row is None:
                print("Deal not found")
                return self.deal_not_found
            else:
                cld_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status new_order
        query = """
            SELECT id FROM client_deals WHERE 
            account_no = %s AND 
            ticket = %s AND 
            status = %s
        """
        try:
            row = await self.database.fetch_one(query, account_no, ticket, self.new_order)
            if row is None:
                print("Parent ticket not found")
                return self.parent_ticket_not_found
            else:
                parent_cld_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Get the current date and time and subtract the delay in seconds
        date = datetime.datetime.now()
        date2 = date - datetime.timedelta(seconds=delay)
        msd = date.strftime("%Y-%m-%d %H:%M:%S")
        msd2 = date2.strftime("%Y-%m-%d %H:%M:%S")

        # Create a query to delete a client deal with the given values
        query = """
            DELETE FROM client_deals WHERE
            prd_id = %s AND
            cli_id = %s AND
            status = %s AND
            account_no = %s AND
            broker = %s AND
            ticket = %s AND
            catch_time = %s AND
            register_time = %s AND
            register_ip = %s AND
            computer_name = %s AND
            cld_id = %s
        """
        try:
            await self.database.execute(query, 
                                        prd_id, cli_id, self.delete_order, account_no, broker, ticket, 
                                        msd2, msd, ip, computer, parent_cld_id)
            return True
        except Exception as e:
            print("Invalid query:", query, e)
            return False
    

    

    async def suborder_client_deal(self, client, broker, account_no, prd_id, ticket, new_ticket, old_lot, new_lot, ip, computer, delay):
        # Create a query to select the id of the client with the given name
        query = """
            SELECT id FROM clients WHERE name = %s
        """
        try:
            row = await self.database.fetch_one(query, client)
            if row is None:
                print("Client not found")
                return self.client_not_found
            else:
                cli_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status sub_order
        query = """
            SELECT id FROM client_deals WHERE 
            account_no = %s AND 
            ticket = %s AND 
            status = %s
        """
        try:
            row = await self.database.fetch_one(query, account_no, new_ticket, self.sub_order)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
            else:
                cld_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the ticket of the provider deal with the given id
        query = """
            SELECT ticket FROM provider_deals WHERE id = %s
        """
        try:
            row = await self.database.fetch_one(query, prd_id)
            prd_ticket = row["ticket"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status new_order or sub_order
        query = """
            SELECT id FROM client_deals WHERE 
            account_no = %s AND 
            ticket = %s AND 
            (status = %s OR status = %s)
        """
        try:
            row = await self.database.fetch_one(query, account_no, ticket, self.new_order, self.sub_order)
            if row is None:
                print("Parent ticket not found")
                return self.parent_ticket_not_found
            else:
                parent_cld_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Get the current date and time and subtract the delay in seconds
        date = datetime.datetime.now()
        date2 = date - datetime.timedelta(seconds=delay)
        msd = date.strftime("%Y-%m-%d %H:%M:%S")
        msd2 = date2.strftime("%Y-%m-%d %H:%M:%S")

        # Create a query to insert a client deal with the given values
        query = """
            INSERT INTO client_deals (
                prd_id, 
                cli_id, 
                status, 
                account_no, 
                broker, 
                ticket, 
                prd_ticket, 
                lot, 
                catch_time, 
                register_time, 
                register_ip, 
                computer_name, 
                cld_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        try:
            await self.database.execute(query, 
                                        prd_id, cli_id, self.sub_order, account_no, broker, new_ticket, 
                                        prd_ticket, new_lot, msd2, msd, ip, computer, parent_cld_id)
            return True
        except Exception as e:
            print("Invalid query:", query, e)
            return False
        
    

    async def get_client_deals(self, client: str, account_no: int, delay: int, exceptions: str) -> Union[int, List[Dict[str, Union[int, str, float]]]]:
        # Create a query to select the id of the client with the given name.
        query = """
            SELECT id FROM clients WHERE name = $1
        """
        try:
            row = await self.database.fetch_one(query, client)
            if row is None:
                print("Client not found")
                return self.client_not_found
            else:
                cli_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Get the current date.
        date = datetime.date.today()

        # Create a query to select the pro_id of the provider clients with the given cli_id and account_no and valid dates.
        query = """
            SELECT pro_id FROM provider_clients WHERE 
            cli_id = $1 AND 
            (account_no = $2 OR account_no IS NULL) AND 
            (from_date IS NULL OR from_date <= $3) AND 
            (to_date IS NULL OR to_date >= $3)
        """
        try:
            rows = await self.database.fetch_all(query, cli_id, account_no, date)
            providers = ""
            for row in rows:
                providers += str(row["pro_id"]) + ","
            if providers != "":
                providers = providers[:-1]
            if providers == "":
                print("No provider found for", client)
                return self.client_no_provider_found
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Get the current date and time and subtract the delay in seconds.
        date2 = datetime.datetime.now()
        date2 -= datetime.timedelta(seconds=delay)
        d = date2.strftime("%Y-%m-%d %H:%M:%S")

        # Create a query to select all columns from provider deals with the given pro_id and catch_time and not in exceptions.
        query = """
            SELECT * FROM provider_deals WHERE 
            id NOT IN ({}) AND 
            pro_id IN ({}) AND 
            catch_time >= '{}'
            ORDER BY catch_time, status
        """.format(exceptions, providers, d)

        # Try to execute the query and fetch all rows.
        try:
            rows = await self.database.fetch_all(query)
            i = -1
            deals = []
            for row in rows:
                i += 1
                deal = {}
                deal["id"] = row["id"]
                deal["prd_id"] = row["prd_id"]
                deal["status"] = row["status"]
                deal["ticket"] = row["ticket"]
                deal["type"] = row["type"]
                deal["symbol"] = row["symbol"]
                deal["lot"] = row["lot"]
                deal["equity"] = row["equity"]
                deal["balance"] = row["balance"]
                deal["open_price"] = row["open_price"]
                deal["sl"] = row["sl"]
                deal["tp"] = row["tp"]
                deal["close_price"] = row["close_price"]
                deal["catch_time"] = row["catch_time"]
                if row["status"] != self.new_order:
                    if row["status"] == self.sub_order:
                        deal["cld_ticket"] = await self.get_client_ticket(cli_id, account_no, row["old_ticket"])
                    else:
                        deal["cld_ticket"] = await self.get_client_ticket(cli_id, account_no, row["ticket"])
                    if deal["cld_ticket"] == -1:
                        deal["id"] = -1
                    else:
                        deal["cld_ticket"] = -1
                    deal["old_lot"] = row["old_lot"]
                    deal["old_ticket"] = row["old_ticket"]
                    deals.append(deal)
                prd_ids = ""
                if i == -1:
                    print("No new deal")
                    return self.no_new_deal
                for j in range(i + 1):
                    prd_ids += str(deals[j]["id"]) + ","
                if prd_ids != "":
                    prd_ids = prd_ids[:-1]
                else:
                    prd_ids = "0"

        # Handle any errors.
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
        # Create a query to select the prd_id of the client deals with the given cli_id and account_no and prd_id in prd_ids.
        query = """
            SELECT prd_id FROM client_deals WHERE 
            cli_id = $1 AND 
            account_no = $2 AND 
            prd_id IN ({})
        """.format(prd_ids)

        # Try to execute the query andfetch all rows.
        try:
            rows = await self.database.fetch_all(query, cli_id, account_no)
            for row in rows:
                for j in range(i + 1):
                    if row["prd_id"] == deals[j]["id"]:
                        deals[j]["id"] = -1
                        break
                for k in range(i + 1):
                    if deals[k]["status"] == self.delete_order or deals[k]["status"] == self.close_order:
                        for j in range(k):
                            if deals[j]["id"] != -1 and deals[j]["ticket"] == deals[k]["ticket"] and deals[j]["status"] == self.new_order:
                                break
                        if j != k:
                            deals[k]["id"] = -1
                            deals[j]["id"] = -1
                deals_count = 0
                final_deals = []
                for j in range(i + 1):
                    if deals[j]["id"] != -1:
                        final_deals.append(deals[j])
                    deals_count += 1
                if deals_count == 0:
                    print("No new deal")
                    return self.no_new_deal
                else:
                    print(deals_count, "deal(s) found")
                    return final_deals

        # Handle any errors.
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    
    def get_client_ticket(self, client_id: int, account_no: int, ticket: int) -> int:
        # Create a query to select the ticket of the client deal with the given cli_id, account_no, prd_ticket and status
        query = """
            SELECT ticket FROM client_deals WHERE 
            cli_id = $1 AND 
            account_no = $2 AND 
            prd_ticket = $3 AND 
            (status = $4 OR status = $5)
        """
        try:
            row = self.database.fetch_one(query, client_id, account_no, ticket, self.new_order, self.sub_order)
            if row is None:
                print("Client ticket not found")
                return -1
            else:
                return row["ticket"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
