# Import required modules
#import databases
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
        



    
