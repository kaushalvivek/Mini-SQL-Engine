
import csv

# List of table names
table_names = []

# List of columns in each table.
table_columns = [[None for x in range(10)] for y in range (10)]

# Table data
table_data = []

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
    print(table_data)


def input_query() :
    '''
    take input of query
    '''

def query() :
    '''
    execute query
    '''

load_metadata()
load_data()
# print("table_names" + str(table_names)+"\n\n")
# print("table_names Columns: "+str(table_columns)+"\n\n")

