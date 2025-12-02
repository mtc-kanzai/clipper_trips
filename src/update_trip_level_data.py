from mtcpy.aws import execute_redshift_cmds

def main() -> None:
    query_name_list = ['anonymized_fare_transaction_subset', 'tagons', 'tagoffs', 'complete_trips', 'tagoffs_only', 'tagons_only', 'trips', 'transfers', 'clipper_trips_2024_to_2026', 'clipper_trips']

    for query_suffix in query_name_list:
        drop_table_query = 'drop table if exists baypass.' + query_suffix + ';'
        execute_redshift_cmds([drop_table_query ], dbname = 'clipper_ods')

        trip_level_query_file_path = 'sql/' + query_suffix + '.sql'
        with open(trip_level_query_file_path, 'r') as file:
            trip_level_query = file.read()
        execute_redshift_cmds([trip_level_query ], dbname = 'clipper_ods')


    for query_suffix in query_name_list:
        if query_suffix not in {'clipper_trips', 'institution_level_data'}:
            drop_table_query = 'drop table if exists baypass.' + query_suffix + ';'
            execute_redshift_cmds([drop_table_query ], dbname = 'clipper_ods')

    grant_permissions_query = 'grant select on baypass.clipper_trips to eps_user, eps_admin, kanzai, tableau_baypass;'
    execute_redshift_cmds([grant_permissions_query ], dbname = 'clipper_ods')

if __name__ == "__main__":
    main()