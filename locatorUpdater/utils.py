from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import string

def contains_whitespace(s):
    # A helper function to see if a string contains any type of whitespace
    for c in s:
        if c in string.whitespace: return True
    return False

def printIfVerbose(s, verbosity_threshold, config):
    if config.verbosity >= verbosity_threshold:
        print s

def executeIfNotDebug(session, sql_statement, config):
    if sql_statement and config.debug:
        printIfVerbose(sql_statement,
            verbosity_threshold=0,
            config=config)
    elif sql_statement:
        session.execute(sql_statement)
        session.commit()

def initial_member_query(session):
    sql = """
Lengthy SQL block, removed by the Developer for my Client's privacy.
"""
    results = session.execute(sql)
    session.commit()
    return results

def initial_locator_data_query(session):
    sql = """
SELECT
    *
FROM
    ____________
"""
    results  = session.execute(sql)
    session.commit()
    return results

def initialize_session(config):
    # Create an engine to communicate with the database
    engine = create_engine("mysql://{user}:{password}@{host}/{database}".format(
        user=config.db_user,
        password=config.db_password,
        host=config.db_address,
        database=config.db_name))

    # Set up connection with the database
    Session = sessionmaker(bind=engine)
    return Session()

