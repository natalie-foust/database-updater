from databaseUpdater.objects import MemberRow
from databaseUpdater.utils import (printIfVerbose, executeIfNotDebug,
                    initial_members_query, initialize_session,
                    initial_database_data_query)

def update_all_database_data(config):
    """
    This mode is the most comprehensive update of the table so far.
        1) It deletes all data from the database_data table.
        2) It updates all missing data from the database_address_coord table.
        3) It inserts all data gathered from the initial sql_query into the
           database_data table.
    """

    sql_session = initialize_session(config)
    # Select the data of all the Practioners we work with.
    members = initial_members_query(sql_session)
    member = MemberRow(members.fetchone(), config)

    # Deletes ALL the data in the database_data table and starts anew. This is
    # really dangerous.
    sql_delete_statement = "DELETE FROM database_data;"
    executeIfNotDebug(sql_session, sql_delete_statement, config)
    row_index=1
    while not member.isEmpty():
        if row_index%5 == 0:
            printIfVerbose("Inserting Row {row_no}".format(row_no=row_index),
                verbosity_threshold=1,
                config=config)
        member.prepareAllRowData(sql_session)
        member.updateGPSInformation(sql_session)
        member.insertRow(sql_session)

        member = MemberRow(members.fetchone(), config)
        row_index += 1

    sql_session.close()

def update_all_database_data_safely(config):
    sql_session = initialize_session(config)
    # Select the data of all the Practioners we work with.
    members = initial_members_query(sql_session)
    member = MemberRow(members.fetchone(), config)

    row_index = 1
    while not member.isEmpty():
        if row_index%5 == 0:
            printIfVerbose("Parsing Row {row_no}".format(row_no=row_index),
                verbosity_threshold=1,
                config=config)

        member.prepareAllRowData(sql_session)
        existing_data = member.previousLocatorDataExists(sql_session)
        if not existing_data:
            member.insertRow(sql_session)
        elif member.existingLocatorDataDiffers(sql_session, existing_data):
            member.updateExistingRow(sql_session, existing_data)

        member = MemberRow(members.fetchone(), config)
        row_index += 1

    sql_session.close()

def update_only_gps_data(config):
    """
    This mode ONLY updates the database_address_coord table.
    """
    sql_session = initialize_session(config)

    # Select the data of all the Practioners we work with.
    members = initial_members_query(sql_session)
    member = MemberRow(members.fetchone(), config)

    row_index=1
    while not member.isEmpty():
        if row_index%5 == 0:
            printIfVerbose("Parsing Row {row_no}".format(row_no=row_index),
                verbosity_threshold=1,
                config=config)
        member.prepareJustGeoData(sql_session)
        member.updateGPSInformation(sql_session)

        member = MemberRow(members.fetchone(), config)
        row_index += 1

    sql_session.close()


def clear_members_marked_for_deletion(config):

    sql_session = initialize_session(config)

    # Select the data of all the Practioners we work with.
    members = initial_database_data_query(sql_session)
    member = MemberRow(members.fetchone(), config)

    row_index = 1
    while not member.isEmpty():
        if row_index%5 == 0:
            printIfVerbose("Checking Row {row_no}".format(row_no=row_index),
                verbosity_threshold=1,
                config=config)
        row_marked = member.isRowMarkedForDeletion(sql_session)
        if row_marked:
            member.deleteRow(sql_session)

        member = MemberRow(members.fetchone(), config)
        row_index += 1

    sql_session.close()

