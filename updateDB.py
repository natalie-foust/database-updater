#!/usr/bin/python
from databaseUpdater import config
from databaseUpdater.app import (update_all_database_data, update_only_gps_data,
                                update_all_database_data_safely,
                                clear_rows_marked_for_deletion)
from databaseUpdater.arguments import parser


if __name__=="__main__":

    args = parser.parse_args()

    config.debug = args.debug
    config.verbosity = args.verbose

    if args.mode == None:
        parser.print_help()
        print """
You must select a mode to run the script in out of:
    --RECREATE-ALL
    --UPDATE-ALL
    --UPDATE-GPS"""
        exit()
    if args.mode == "RECREATE-ALL":
        update_all_database_data(config)
    elif args.mode == "UPDATE-GPS":
        update_only_gps_data(config)
    elif args.mode == "UPDATE-ALL":
        update_all_database_data_safely(config)
    elif args.mode == "CLEAR-DELETED":
        clear_rows_marked_for_deletion(config)
