import argparse

parser = argparse.ArgumentParser(
        description="Update the database for the web app")
mode = parser.add_mutually_exclusive_group()
mode.add_argument("-R", "--RECREATE-ALL",
        help="""\
Run the script in update mode. Will delete the table and recreate it from scratch.
This is the default mode to run the script in.""",
        const="RECREATE-ALL", dest="mode", action="store_const")
mode.add_argument("-U", "--UPDATE-ALL",
        help="""\
Run the script in safe update mode. Will insert any rows that should exist but
do not, and will update any rows who's existing information does not match up
with the rest of the database.""",
        const="UPDATE-ALL", dest="mode", action="store_const")
mode.add_argument("-C", "--CLEAR-DELETED",
        help="""\
Run the script in clear deleted mode. Will delete any rows whose members have
the remove_from_database column set.""",
        const="CLEAR-DELETED", dest="mode", action="store_const")
mode.add_argument("--UPDATE-GPS",
        help="""\
Run the script in update GPS mode. Will not make any changes to the to the primary
database table, only to the locator_address_coord table""",
        const="UPDATE-GPS", dest="mode", action="store_const")
parser.add_argument("-D", "--DEBUG",
        help="""\
Run the script in debug mode. Will make no changes to the database. Instead, the
script prints all INSERT and UPDATE statements to STDOUT""",
        dest="debug", action="store_true")
parser.add_argument("-v","--verbose", action="count", default=0)
