import sys
import csv
import sqlparse

# List of table names
table_names = []

# List of columns in each table.
table_columns = [[None for x in range(10)] for y in range (10)]

# Table data
table_data = []

query = sys.argv[1]

if len(sys.argv) > 2:
    print('ERROR: Please enter one query at a time.')
    quit()

def load_metadata() :
    '''
    Metadata read and relevant content
    is loaded in tables and table_columns.
    '''
    metadata = open("files/metadata.txt","r")
    content = metadata.readlines()
    table_count, col_count, i = -1,0,0
    while (i < len(content)) :
        if content[i] == '<begin_table>\n':
           table_names.append(str.strip(content[i+1]))
           table_count+=1
           i+=2
           col_count = 0
           continue

        if str.strip(content[i]) != '<end_table>':
           table_columns[table_count][col_count] = str.strip(content[i])
           col_count +=1
        
        i+=1

def load_data() :
    '''
    load data from CSV files
    '''
    for i in table_names:
        table = []
        csv_file = open('files/'+i+'.csv','r')
        rows = csv.reader(csv_file, delimiter='\n')
        for row in rows:
            table.append(row)
        table_data.append(table)

def process_query():
    '''
    read query and understand it.
    '''
    x = sqlparse.parse(query)
    q = x[0]
    q_length = len(q.tokens)

    # Handle error cases later on

    # q[0] will always be select
    # q[1] will always be a whitespace
    # q[2] will be comma-separated list of columns to select
    # q[3] will always be a whitespace
    # q[4] will always be from
    # q[5] will always be a whitespace
    # q[6] will always be the table to choose from
    # q[n-1] will always be a semi-colon

    if ((str(q[q_length-1]) == ";" and q_length == 8) \
    or (str(q[q_length-1]) != ";" and q_length == 7)) \
    and 1:
    # Instead of 1, check that token q.tokens[2] is either a variable or a varable list.
        query1(q_length, q)

def query1(q_length, q) :
    '''
    Queries of the form:
    SELECT <column list>
    FROM <table list>;
    '''
    # print(q.tokens[2].name)
    pass


load_metadata()
load_data()
process_query()

