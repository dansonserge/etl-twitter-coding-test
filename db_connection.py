import os
from sqlalchemy import create_engine

# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}:{port}/{db}"
				.format(port=3306,host=os.getenv('DB_HOST'), db='twitter_db', user=os.getenv('DB_USER'), pw=os.getenv('DB_PWD')))

db_connection = engine.connect()