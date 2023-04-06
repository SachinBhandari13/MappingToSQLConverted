import pandas as pd
from util_mapping_doc_to_sql import *


if __name__ == '__main__':
    input_path_excel = r'G:/Multimedia/Study Material/Python/accelerators/input.xlsx'
    output_path_sql = 'G:/Multimedia/Study Material/Python/accelerators/sqls/'
    query_type_dict = {'insert': 'INSERT INTO TABLE \n(\nSELECT ', 'select': 'SELECT\n'}

    print("############# SQL Generator started ################\n")
    mapping_df = pd.read_excel(input_path_excel)
    mapping_df.fillna('empty', inplace=True)

    # metadata_df = pd.read_excel(input_path_excel, skiprows=0, nrows=1, usecols=[1,2], engine="openpyxl")


    sql_txt = query_type_dict[str(mapping_df.iloc[0]['Query Type']).lower()]
    sql_ddl = 'CREATE OR REPLACE TABLE\n(\n'
    prev_src_full_tbl = mapping_df.iloc[0][0] + '.' + mapping_df.iloc[0][1] + '.' + mapping_df.iloc[0][2]

    for index, row in mapping_df.iterrows():
        src_full_tbl = row['Project'] + '.' + row['Source Dataset'] + '.' + row['Table']

        if src_full_tbl == prev_src_full_tbl:  # if row of similar table
            sql_txt += col_logic_builder(row) + ',' + '\n'
            sql_ddl += ddl_builder(row)

        if src_full_tbl != prev_src_full_tbl:  # write to file
            dst_full_tbl = mapping_df.iloc[index - 1, 0] + '.' + mapping_df.iloc[index - 1, 4] + '.' + mapping_df.iloc[
                index - 1, 5]

            filter_condition = str(mapping_df.iloc[index - 1]['Filter Condition'])
            write_to_sql_file(output_path_sql, sql_txt, prev_src_full_tbl, dst_full_tbl, filter_condition)
            write_to_sql_ddl(output_path_sql, sql_txt, dst_full_tbl, sql_ddl)

            # reset values for next table
            sql_txt = query_type_dict[str(mapping_df.iloc[index]['Query Type']).lower()]
            sql_txt += col_logic_builder(row) + ',' + '\n'
            sql_ddl = 'CREATE OR REPLACE TABLE\n(\n'
            sql_ddl += ddl_builder(row)
            prev_src_full_tbl = src_full_tbl

    # write last table
    dst_full_tbl = mapping_df.iloc[- 1, 0] + '.' + mapping_df.iloc[- 1, 4] + '.' + mapping_df.iloc[- 1, 5]
    filter_condition = str(mapping_df.iloc[-1]['Filter Condition'])
    write_to_sql_file(output_path_sql, sql_txt, prev_src_full_tbl, dst_full_tbl, filter_condition)
    write_to_sql_ddl(output_path_sql, sql_txt, dst_full_tbl, sql_ddl)
