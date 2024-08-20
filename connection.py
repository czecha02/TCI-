from sqlalchemy import create_engine


user = "support"
db = "test"
host = '51.143.218.200'
user = "support"
password = "Welcome2023!"
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db}")
