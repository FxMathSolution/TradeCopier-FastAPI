# Import required modules
import databases
import sqlalchemy
import hashlib
import datetime 

# Define database URL
DATABASE_URL = "sqlite:///./test.db"

# Create a table object for clients
clients = sqlalchemy.Table(
    "clients",
    sqlalchemy.MetaData(),
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("password", sqlalchemy.String),
    sqlalchemy.Column("full_name", sqlalchemy.String),
    sqlalchemy.Column("email", sqlalchemy.String),
    sqlalchemy.Column("expire", sqlalchemy.Integer),
)

# Create a table object for client_deals
client_deals = sqlalchemy.Table(
    "client_deals",
    sqlalchemy.MetaData(),
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("prd_id", sqlalchemy.Integer),
    sqlalchemy.Column("cli_id", sqlalchemy.Integer),
    sqlalchemy.Column("prd_ticket", sqlalchemy.BigInteger),
    sqlalchemy.Column("status", sqlalchemy.SmallInteger),
    sqlalchemy.Column("account_no", sqlalchemy.BigInteger),
    sqlalchemy.Column("broker", sqlalchemy.String),
    sqlalchemy.Column("ticket", sqlalchemy.BigInteger),
    sqlalchemy.Column("type", sqlalchemy.SmallInteger),
    sqlalchemy.Column("symbol", sqlalchemy.String),
    sqlalchemy.Column("lot", sqlalchemy.Float),
    sqlalchemy.Column("equity", sqlalchemy.Float),
    sqlalchemy.Column("balance", sqlalchemy.Float),
    sqlalchemy.Column("open_time", sqlalchemy.BigInteger),
    sqlalchemy.Column("open_price", sqlalchemy.Float),
    sqlalchemy.Column("sl", sqlalchemy.Float),
    sqlalchemy.Column("tp", sqlalchemy.Float),
    sqlalchemy.Column("close_time", sqlalchemy.BigInteger),
    sqlalchemy.Column("close_price", sqlalchemy.Float),
    sqlalchemy.Column("order_profit", sqlalchemy.Float),
    sqlalchemy.Column("order_commission", sqlalchemy.Float),
    sqlalchemy.Column("order_swap", sqlalchemy.Float),
    sqlalchemy.Column("catch_time", sqlalchemy.DateTime),
    sqlalchemy.Column("register_time", sqlalchemy.DateTime),
    sqlalchemy.Column("register_ip", sqlalchemy.String),
    sqlalchemy.Column("computer_name", sqlalchemy.String),
    sqlalchemy.Column("cld_id", sqlalchemy.Integer)
)



# Create a table object for providers
providers = sqlalchemy.Table(
    "providers",
    sqlalchemy.MetaData(),
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("password", sqlalchemy.String),
    sqlalchemy.Column("full_name", sqlalchemy.String),
    sqlalchemy.Column("email", sqlalchemy.String)
)

# Create a table object for provider_clients
provider_clients = sqlalchemy.Table(
    "provider_clients",
    sqlalchemy.MetaData(),
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("pro_id", sqlalchemy.Integer),
    sqlalchemy.Column("cli_id", sqlalchemy.Integer),
    sqlalchemy.Column("account_no", sqlalchemy.BigInteger),
    sqlalchemy.Column("from_date", sqlalchemy.Date),
    sqlalchemy.Column("to_date", sqlalchemy.Date)
)

# Create a table object for provider_deals
provider_deals = sqlalchemy.Table(
    "provider_deals",
    sqlalchemy.MetaData(),
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("pro_id", sqlalchemy.Integer),
    sqlalchemy.Column("status", sqlalchemy.SmallInteger),
    sqlalchemy.Column("account_no", sqlalchemy.BigInteger),
    sqlalchemy.Column("broker", sqlalchemy.String),
    sqlalchemy.Column("ticket", sqlalchemy.BigInteger),
    sqlalchemy.Column("type", sqlalchemy.SmallInteger),
    sqlalchemy.Column("symbol", sqlalchemy.String),
    sqlalchemy.Column("lot", sqlalchemy.Float),
    sqlalchemy.Column("equity", sqlalchemy.Float),
    sqlalchemy.Column("balance", sqlalchemy.Float),
    sqlalchemy.Column("open_time", sqlalchemy.BigInteger),
    sqlalchemy.Column("open_price", sqlalchemy.Float),
    sqlalchemy.Column("sl", sqlalchemy.Float),
    sqlalchemy.Column("tp", sqlalchemy.Float),
    sqlalchemy.Column("close_time", sqlalchemy.BigInteger),
    sqlalchemy.Column("close_price", sqlalchemy.Float),
    sqlalchemy.Column("order_profit", sqlalchemy.Float),
    sqlalchemy.Column("order_commission", sqlalchemy.Float),
    sqlalchemy.Column("order_swap", sqlalchemy.Float),
    sqlalchemy.Column("catch_time", sqlalchemy.DateTime),
    sqlalchemy.Column("register_time", sqlalchemy.DateTime),
    sqlalchemy.Column("register_ip", sqlalchemy.String),
    sqlalchemy.Column("computer_name", sqlalchemy.String),
    sqlalchemy.Column("prd_id", sqlalchemy.Integer),
    sqlalchemy.Column("old_ticket", sqlalchemy.BigInteger),
    sqlalchemy.Column("old_lot", sqlalchemy.Float)
)

# Create a table object for users
users = sqlalchemy.Table(
    "users",
    sqlalchemy.MetaData(),
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True), 
    sqlalchemy.Column("username", sqlalchemy.String), 
    sqlalchemy.Column("password", sqlalchemy.String), 
    sqlalchemy.Column("full_name", sqlalchemy.String), 
    sqlalchemy.Column("email", sqlalchemy.String)
)

# Create a class for the database
class DB:
    # Define the constructor method
    def __init__(self):
        # Create a database object
        self.database = databases.Database(DATABASE_URL)
        # Create a metadata object
        self.metadata = sqlalchemy.MetaData()
        # Add the table objects to the metadata object
        self.metadata.create_all(clients, providers, provider_clients, provider_deals, users)

    # Define a method to connect to the database
    async def connect(self):
        try:
            # Try to connect to the database
            await self.database.connect()
            print("Connected to the database")
        except Exception as e:
            # Handle any errors
            print("Failed to connect to the database:", e)

    
    

    # Define a method to add a provider
    async def add_provider(self, name: str, password: str, full_name: str, email: str):
        # Hash the password using md5
        password = hashlib.md5(password.encode()).hexdigest()
        # Create a query to insert a provider
        query = providers.insert().values(name=name, password=password, full_name=full_name, email=email)
        # Try to execute the query and get the last record id
        try:
            last_record_id = await self.database.execute(query)
            return last_record_id
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    # Define a method to authenticate a provider
    async def authenticate_provider(self, name: str, password: str):
        # Hash the password using md5
        password = hashlib.md5(password.encode()).hexdigest()
        # Create a query to select the id of the provider with the given name and password
        query = providers.select().where(providers.c.name == name and providers.c.password == password)
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Authentication failed")
                return self.authentication_failed
            else:
                return row["id"]
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    
    # Define a method to authenticate a client
    async def authenticate_client(self, name: str, password: str):
        # Hash the password using md5
        password = hashlib.md5(password.encode()).hexdigest()
        # Create a query to select the id of the client with the given name and password
        query = clients.select().where(clients.c.name == name and clients.c.password == password)
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Authentication failed")
                return self.authentication_failed
            else:
                return row["id"]
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    
    # Define a method to add a provider deal
    async def add_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int, symbol: str, type: int,
                                lot: float, balance: float, equity: float, open_time: int, open_price: float,
                                sl: float, tp: float, ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = providers.select().where(providers.c.name == provider)
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Provider not found")
                return self.provider_not_found
            else:
                pro_id = row["id"]
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order
        query = provider_deals.select().where(provider_deals.c.account_no == account_no and provider_deals.c.ticket == ticket and provider_deals.c.status == self.new_order)
        # Try to execute the query and fetch one row
   
        try:
            row = await self.database.fetch_one(query)
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
                query = provider_deals.insert().values(pro_id=pro_id, status=self.new_order,
                                                        account_no=account_no, broker=broker,
                                                        ticket=ticket, type=type,
                                                        symbol=symbol, lot=lot,
                                                        equity=equity, balance=balance,
                                                        open_time=open_time,
                                                        open_price=open_price,
                                                        sl=sl, tp=tp,
                                                        catch_time=msd2,
                                                        register_time=msd,
                                                        register_ip=ip,
                                                        computer_name=computer)
                # Try to execute the query and get the last record id
                try:
                    last_record_id = await self.database.execute(query)
                    return last_record_id
                # Handle any errors
                except Exception as e:
                    print("Invalid query:", query, e)
                    return -1
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    
    # Define a method to modify a provider deal
    async def modify_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int, open_price: float, sl: float, tp: float, ip: str,
                                computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = providers.select().where(providers.c.name == provider)
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Provider not found")
                return self.provider_not_found
            else:
                pro_id = row["id"]
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status modify_order and open_price and sl and tp
        query = provider_deals.select().where(provider_deals.c.account_no == account_no and provider_deals.c.ticket == ticket and provider_deals.c.status == self.modify_order and provider_deals.c.open_price == open_price and provider_deals.c.sl == sl and provider_deals.c.tp == tp)
        
        try:
            row = await self.database.fetch_one(query)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
            # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order or sub_order
        query = provider_deals.select().where(provider_deals.c.account_no == account_no and provider_deals.c.ticket == ticket and (provider_deals.c.status == self.new_order or provider_deals.c.status == self.sub_order))
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
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
                query = provider_deals.insert().values(pro_id=pro_id, status=self.modify_order,
                                                        account_no=account_no, broker=broker,
                                                        ticket=ticket,
                                                        open_price=open_price,
                                                        sl=sl, tp=tp,
                                                        catch_time=msd2,
                                                        register_time=msd,
                                                        register_ip=ip,
                                                        computer_name=computer,
                                                        prd_id=prd_id)
                # Try to execute the query and get the last record id
                try:
                    last_record_id = await self.database.execute(query)
                    return last_record_id
                # Handle any errors
                except Exception as e:
                    print("Invalid query:", query, e)
                    return -1
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    
    async def close_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int,
                             close_price: float, profit: float, commission: float,
                             swap: float, ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = providers.select().where(providers.c.name == provider)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Provider not found")
                return self.provider_not_found
            else:
                pro_id = row["id"]
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status close_order
        query = provider_deals.select().where(provider_deals.c.account_no == account_no and provider_deals.c.ticket == ticket and provider_deals.c.status == self.close_order)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
                
        # Handle anyerrors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order or sub_order
        query = provider_deals.select().where(provider_deals.c.account_no == account_no and provider_deals.c.ticket == ticket and (provider_deals.c.status == self.new_order or provider_deals.c.status == self.sub_order))
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
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
            query = provider_deals.insert().values(pro_id=pro_id, status=self.close_order,
                                                    account_no=account_no, broker=broker,
                                                    ticket=ticket,
                                                    close_price=close_price,
                                                    order_profit=profit,
                                                commission=commission,
                                                    order_swap=swap,
                                                    catch_time=msd2,
                                                    register_time=msd,
                                                    register_ip=ip,
                                                    computer_name=computer,
                                                    prd_id=prd_id)
                                                    
            # Try to execute the query and get the last record id
            try:
                last_record_id = await self.database.execute(query)
                return last_record_id
                
            # Handle any errors
            except Exception as e:
                print("Invalid query:", query, e)
                return -1
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    
    async def delete_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int,
                               ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = providers.select().where(providers.c.name == provider)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Provider not found")
                return self.provider_not_found
            else:
                pro_id = row["id"]
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status delete_order
        query = provider_deals.select().where(provider_deals.c.account_no == account_no and provider_deals.c.ticket == ticket and provider_deals.c.status == self.delete_order)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order
        query = provider_deals.select().where(provider_deals.c.account_no == account_no and provider_deals.c.ticket == ticket and provider_deals.c.status == self.new_order)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
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
            query = provider_deals.insert().values(pro_id=pro_id, status=self.delete_order,
                                                account_no=account_no, broker=broker,
                                                ticket=ticket,
                                                catch_time=msd2,
                                                register_time=msd,
                                                register_ip=ip,
                                                computer_name=computer,
                                                prd_id=prd_id)
                                                
            # Try to execute the query and getthe last record id
            try:
                last_record_id = await self.database.execute(query)
                return last_record_id
                
            # Handle any errors
            except Exception as e:
                print("Invalid query:", query, e)
                return -1
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    
    async def suborder_provider_deal(self, provider: str, broker: str, account_no: int, ticket: int,
                                 new_ticket: int, old_lot: float, new_lot: float,
                                 ip: str, computer: str, delay: int):
        # Create a query to select the id of the provider with the given name
        query = providers.select().where(providers.c.name == provider)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Provider not found")
                return self.provider_not_found
            else:
                pro_id = row["id"]
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and new_ticket and status sub_order
        query = provider_deals.select().where(provider_deals.c.account_no == account_no and provider_deals.c.new_ticket == new_ticket and provider_deals.c.status == self.sub_order)
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
                
        #Continuation:

        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the provider deal with the given account_no and ticket and status new_order or sub_order
        query = provider_deals.select().where(provider_deals.c.account_no == account_no and provider_deals.c.ticket == ticket and (provider_deals.c.status == self.new_order or provider_deals.c.status == self.sub_order))
        
        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
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
            query = provider_deals.insert().values(pro_id=pro_id, status=self.sub_order,
                                                account_no=account_no, broker=broker,
                                                ticket=new_ticket,
                                                old_ticket=ticket,
                                                lot=new_lot,
                                                old_lot=old_lot,
                                                catch_time=msd2,
                                                register_time=msd,
                                                register_ip=ip,
                                                computer_name=computer,
                                                prd_id=prd_id)
                                                
            # Try to execute the query and get the last record id
            try:
                last_record_id = await self.database.execute(query)
                return last_record_id
                
            # Handle any errors
            except Exception as e:
                print("Invalid query:", query, e)
                return -1
                
        # Handle any errors
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    
    async def add_client_deal(self, client: str, broker: str, account_no: int, prd_id: int, ticket: int, symbol: str, type: int,
    lot: float, balance: float, equity: float, open_time: int, open_price: float,
    sl: float, tp: float, ip: str, computer: str, delay: int):

        # Create a query to select the id of the client with the given name
        query = clients.select().where(clients.c.name == client)

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Client not found")
                return self.client_not_found
            else:
                cli_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the ticket of the provider deal with the given id
        query = provider_deals.select().where(provider_deals.c.id == prd_id)

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Provider deal not found")
                return self.provider_deal_not_found
            else:
                prd_ticket = row["ticket"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status new_order
        query = client_deals.select().where(client_deals.c.account_no == account_no and client_deals.c.ticket == ticket and client_deals.c.status == self.new_order)

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
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
                query = client_deals.insert().values(prd_id=prd_id, cli_id=cli_id, prd_ticket=prd_ticket, status=self.new_order,
                    account_no=account_no, broker=broker,
                    ticket=ticket, type=type,
                    symbol=symbol, lot=lot,
                    equity=equity, balance=balance,
                    open_time=open_time,
                    open_price=open_price,
                    sl=sl, tp=tp,
                    catch_time=msd2,
                    register_time=msd,
                    register_ip=ip,
                    computer_name=computer)
                # Try to execute the query and get the last record id
                try:
                    last_record_id = await self.database.execute(query)
                    return last_record_id
                except Exception as e:
                    print("Invalid query:", query, e)
                    return -1
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    

    async def modify_client_deal(self, client: str, broker: str, account_no: int, prd_id: int, ticket: int, open_price: float,
        sl: float, tp: float, ip: str, computer: str, delay: int):

        # Create a query to select the id of the client with the given name
        query = clients.select().where(clients.c.name == client)

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
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
        query = client_deals.select().where(
            client_deals.c.account_no == account_no,
            client_deals.c.ticket == ticket,
            client_deals.c.status == self.modify_order,
            client_deals.c.open_price == open_price,
            client_deals.c.sl == sl,
            client_deals.c.tp == tp,
        )

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status new_order or sub_order
        query = client_deals.select().where(
            client_deals.c.account_no == account_no,
            client_deals.c.ticket == ticket,
            (client_deals.c.status == self.new_order or client_deals.c.status == self.sub_order),
        )

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
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
        query = client_deals.insert().values(
            prd_id=prd_id,
            cli_id=cli_id,
            status=self.modify_order,
            account_no=account_no,
            broker=broker,
            ticket=ticket,
            open_price=open_price,
            sl=sl,
            tp=tp,
            catch_time=msd2,
            register_time=msd,
            register_ip=ip,
            computer_name=computer,
            cld_id=cld_id,
        )

        # Try to execute the query and get the last record id
        try:
            last_record_id = await self.database.execute(query)
            return last_record_id
        except Exception as e:
            print("Invalid query:", query, e)
            return -1



    async def close_client_deal(self, client: str, broker: str, account_no: int, prd_id: int, ticket: int, close_price: float,
        profit: float, commission: float, swap: float, ip: str, computer: str, delay: int):

        # Create a query to select the id of the client with the given name
        query = clients.select().where(clients.c.name == client)

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Client not found")
                return self.client_not_found
            else:
                cli_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status close_order
        query = client_deals.select().where(
            client_deals.c.account_no == account_no,
            client_deals.c.ticket == ticket,
            client_deals.c.status == self.close_order,
        )

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status new_order or sub_order
        query = client_deals.select().where(
            client_deals.c.account_no == account_no,
            client_deals.c.ticket == ticket,
            (client_deals.c.status == self.new_order or client_deals.c.status == self.sub_order),
        )

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
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
        query = client_deals.insert().values(
            prd_id=prd_id,
            cli_id=cli_id,
            status=self.close_order,
            account_no=account_no,
            broker=broker,
            ticket=ticket,
            close_price=close_price,
            order_profit=profit,
            order_commission=commission,
            order_swap=swap,
            catch_time=msd2,
            register_time=msd,
            register_ip=ip,
            computer_name=computer,
            cld_id=cld_id,
        )

        # Try to execute the query and get the last record id
        try:
            last_record_id = await self.database.execute(query)
            return last_record_id
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    
    async def delete_client_deal(self, client: str, broker: str, account_no: int, prd_id: int, ticket: int, ip: str, computer: str, delay: int):

        # Create a query to select the id of the client with the given name
        query = clients.select().where(clients.c.name == client)

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Client not found")
                return self.client_not_found
            else:
                cli_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status delete_order
        query = client_deals.select().where(
            client_deals.c.account_no == account_no,
            client_deals.c.ticket == ticket,
            client_deals.c.status == self.delete_order,
        )

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
            else:
                cld_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status new_order
        query = client_deals.select().where(
            client_deals.c.account_no == account_no,
            client_deals.c.ticket == ticket,
            client_deals.c.status == self.new_order,
        )

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
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
        query = client_deals.delete().where(
            prd_id=prd_id,
            cli_id=cli_id,
            status=self.delete_order,
            account_no=account_no,
            broker=broker,
            ticket=ticket,
            catch_time=msd2,
            register_time=msd,
            register_ip=ip,
            computer_name=computer,
            cld_id=cld_id,
        )

        # Try to execute the query and get the last record id
        try:
            last_record_id = await self.database.execute(query)
            return last_record_id
        except Exception as e:
            print("Invalid query:", query, e)
            return -1
        
    
    async def suborder_client_deal(self, client: str, broker: str, account_no: int, prd_id: int, ticket: int, new_ticket: int, old_lot: float, new_lot: float, ip: str, computer: str, delay: int):

        # Create a query to select the id of the client with the given name
        query = clients.select().where(clients.c.name == client)

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Client not found")
                return self.client_not_found
            else:
                cli_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status sub_order
        query = client_deals.select().where(
            client_deals.c.account_no == account_no,
            client_deals.c.ticket == new_ticket,
            client_deals.c.status == self.sub_order,
        )

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is not None:
                print("Deal is repeated")
                return self.deal_is_repeated
            else:
                cld_id = row["id"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the ticket of the provider deal with the given id
        query = provider_deals.select().where(provider_deals.c.id == prd_id)

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            prd_ticket = row["ticket"]
        except Exception as e:
            print("Invalid query:", query, e)
            return -1

        # Create a query to select the id of the client deal with the given account_no and ticket and status new_order or sub_order
        query = client_deals.select().where(
            client_deals.c.account_no == account_no,
            client_deals.c.ticket == ticket,
            (client_deals.c.status == self.new_order or client_deals.c.status == self.sub_order),
        )

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
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
        query = client_deals.insert().values(
            prd_id=prd_id,
            cli_id=cli_id,
            status=self.sub_order,
            account_no=account_no,
            broker=broker,
            ticket=new_ticket,
            prd_ticket=prd_ticket,
            lot=new_lot,
            catch_time=msd2,
            register_time=msd,
            register_ip=ip,
            computer_name=computer,
            cld_id=cld_id,
        )

    

    async def get_client_deals(self, client: str, account_no: int, delay: int, exceptions: str) -> int:

        # Create a query to select the id of the client with the given name.
        query = clients.select().where(clients.c.name == client)

        # Try to execute the query and fetch one row.
        try:
            row = await self.database.fetch_one(query)
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
        query = provider_clients.select().where(
            provider_clients.c.cli_id == cli_id
            and (provider_clients.c.account_no == account_no or provider_clients.c.account_no.is_(None))
            and (provider_clients.c.from_date.is_(None) or provider_clients.c.from_date <= date)
            and (provider_clients.c.to_date.is_(None) or provider_clients.c.to_date >= date)
        )

        # Try to execute the query and fetch all rows.
        try:
            rows = await self.database.fetch_all(query)
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
        query = provider_deals.select().where(
            provider_deals.c.id.notin_(exceptions) and provider_deals.c.pro_id.in_(providers)
            and provider_deals.c.catch_time >= d).order_by(provider_deals.c.catch_time, provider_deals.c.status.asc())

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
                        deal["cld_ticket"] = self.get_client_ticket(cli_id, account_no, row["old_ticket"])
                    else:
                        deal["cld_ticket"] = self.get_client_ticket(cli_id, account_no, row["ticket"])
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
        query = client_deals.select().where(
            client_deals.c.cli_id == cli_id and client_deals.c.account_no == account_no and client_deals.c.prd_id.in_(prd_ids)
        )

        # Try to execute the query and fetch all rows.
        try:
            rows = await self.database.fetch_all(query)
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
        
    
    async def get_client_ticket(self, client_id: int, account_no: int, ticket: int):

        # Create a query to select the ticket of the client deal with the given cli_id, account_no, prd_ticket and status
        query = client_deals.select().where(
            client_deals.c.cli_id == client_id and
            client_deals.c.account_no == account_no and
            client_deals.c.prd_ticket == ticket and
            (client_deals.c.status == self.new_order or client_deals.c.status == self.sub_order)
        )

        # Try to execute the query and fetch one row
        try:
            row = await self.database.fetch_one(query)
            if row is None:
                print("Client ticket not found")
                return -1
            else:
                return row["ticket"]

        # Handle any errors.
        except Exception as e:
            print("Invalid query:", query, e)
            return -1


       
