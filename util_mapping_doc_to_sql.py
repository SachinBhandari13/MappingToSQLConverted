import re

def col_sanitizer(column, casing):
    column = str(column).lower() if re.sub(r'\s*','',casing).lower() == 'lowercase' else str(column).upper()

    column_sanitized = re.sub(r'^\d+|[^\w]', '_', str(column))
    if column_sanitized != column:
        column_sanitized += '--add comment'

    return column_sanitized


def col_logic_builder(row_temp):
    logic = row_temp['Logic']
    src_col = col_sanitizer(row_temp['Source Columns'], row_temp['Column Casing'])
    dst_col = col_sanitizer(row_temp['Destination Column'], row_temp['Column Casing'])

    if str(row_temp[6]) == 'empty':
        col_value = logic + ' AS ' + src_col if logic != 'direct' else col_sanitizer(row_temp['Source Columns'],
                                                                                     row_temp['Column Casing'])
    else:
        col_value = logic + ' AS ' + dst_col if logic != 'direct' else src_col + ' AS ' + dst_col

    return col_value


def write_to_sql_file(path, sql_txt, src_tbl, dst_tbl, filter_condition):
    print("File processed : " + dst_tbl.split('.')[2] + '')
    sql_file_name = 'dml_' + dst_tbl.split('.')[2]

    with open(path + sql_file_name + '.sql', mode='w') as o_file:
        comment = ',\t\t--Column Sanitized. Reason: column name had special characters  '
        content = sql_txt.replace('--add comment,', comment).replace(comment, comment[1:]) if sql_txt.endswith(
            '--add comment,\n') else sql_txt.replace('--add comment,', comment)

        content = content[0:-2]  # remove , from last column
        content = content.replace('INSERT INTO TABLE', 'INSERT INTO TABLE ' + dst_tbl) + '\n\nFROM ' + src_tbl + '\n'+ filter_condition +'\n);'
        o_file.write(content)


def ddl_builder(row):
    col = col_sanitizer(row['Source Columns'], row['Column Casing']) + ' ' + row['Data Type'] + ',\n' \
        if row['Destination Column'] == 'empty' \
        else col_sanitizer(row['Destination Column'], row['Column Casing']) + ' ' + row['Data Type'] + ',\n'
    col = col.replace('--add comment', '')
    return col


def write_to_sql_ddl(path, sql_txt, dst_full_tbl, sql):
    sql_file_name = 'ddl_' + dst_full_tbl.split('.')[2]
    with open(path + sql_file_name + '.sql', mode='w') as o_file:
        comment = ',\t\t--Column Sanitized. Reason: column name had special characters  '
        content = sql.replace('--add comment,', comment).replace(comment, comment[1:]) if sql_txt.endswith(
            '--add comment,\n') else sql.replace(',--add comment', comment)

        content = content[0:-2]  # remove , from last column
        content = content.replace('CREATE OR REPLACE TABLE\n(', 'CREATE OR REPLACE TABLE ' + dst_full_tbl + '(') + '\n);'
        o_file.write(content)