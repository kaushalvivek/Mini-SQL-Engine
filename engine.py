import csv
import sys
import re
from collections import OrderedDict


def main():
    dictionary = {}
    readMetadata(dictionary)
    processQuery(str(sys.argv[1]),dictionary)

def printData(fileData,columnNames,tableNames,dictionary):
    for data in fileData:
        for col in range(len(columnNames)):
            print data[dictionary[tableNames[0]].index(columnNames[col])],
        print

def readMetadata(dictionary):
    f = open('./metadata.txt','r')
    check = 0
    for line in f:
        if line.strip() == "<begin_table>":
            check = 1
            continue
        if check == 1:
            tableName = line.strip()
            dictionary[tableName] = [];
            check = 0
            continue
        if not line.strip() == '<end_table>':
            dictionary[tableName].append(line.strip());		

def readFile(tName,fileData):
        with open(tName,'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                fileData.append(row)

def processWhere(whereStr,columnNames,tableNames,dictionary):
    a = whereStr.split(" ")

    if(len(columnNames) == 1 and columnNames[0] == '*'):
        columnNames = dictionary[tableNames[0]]

    printHeader(columnNames,tableNames,dictionary)

    tName = tableNames[0] + '.csv'
    fileData = []
    readFile(tName,fileData)

    check = 0
    for data in fileData:
        string = evaluate(a,tableNames,dictionary,data)
        for col in columnNames:
            if eval(string):
                check = 1
                print data[dictionary[tableNames[0]].index(col)],
        if check == 1:
            check = 0
            print

def processQuery(query,dictionary):
    query_initial = (re.sub(' +',' ',query)).strip();
    query_final = query_initial.split(';')
    query = query_final[0]

    if "from" not in query:
        sys.exit("Incorrect Syntax")
    else:
        obj1 = query.split('from');

    obj1[0] = (re.sub(' +',' ',obj1[0])).strip();

    objects = []
    objects.append(0)

    if "select" not in obj1[0].lower():
        sys.exit("Incorrect Syntax")
    objects.append(obj1[0][7:])

    objects[1] = (re.sub(' +',' ',objects[1])).strip();
    l = []
    l.append("select")

    if "distinct" in objects[1] and "distinct(" not in objects[1]:
        objects[1] = objects[1][9:]
        l.append("distinct")

    l.append(objects[1])
    objects[1] = l 

    object3 = ""
    if "distinct" in objects[1][1] and "distinct(" not in objects[1][1]:
        object3 = objects[1][1];
        object3 = (re.sub(' +',' ',object3)).strip()
        objects[1][1] = objects[1][2]

    colStr = objects[1][1];
    colStr = (re.sub(' +',' ',colStr)).strip()
    columnNames = colStr.split(',');
    for i in range(len(columnNames)):
        columnNames[columnNames.index(columnNames[i])] = (re.sub(' +',' ',columnNames[i])).strip();
    obj1[1] = (re.sub(' +',' ',obj1[1])).strip();
    temp = obj1[1].split('where');
    objects.append(temp)
    
    tableStr = objects[2][0]
    tableStr = (re.sub(' +',' ',tableStr)).strip();
    tableNames = tableStr.split(',')
    for i in range(len(tableNames)):
        tableNames[tableNames.index(tableNames[i])] = (re.sub(' +',' ',tableNames[i])).strip();
    for i in range(len(tableNames)):
        if tableNames[i] not in dictionary.keys():
            sys.exit("Table not found")

    if len(objects[2]) > 1 and len(tableNames) == 1:
        objects[2][1] = (re.sub(' +',' ',objects[2][1])).strip();
        processWhere(objects[2][1],columnNames,tableNames,dictionary)
        return
    elif len(objects[2]) > 1 and len(tableNames) > 1:
        objects[2][1] = (re.sub(' +',' ',objects[2][1])).strip();
        processWhereJoin(objects[2][1],columnNames,tableNames,dictionary)
        return

    if(len(tableNames) > 1):
        join(columnNames,tableNames,dictionary)
        return

    if object3 == "distinct":
        distinctMany(columnNames,tableNames,dictionary)
        return
    
    if len(columnNames) == 1:
        for col in columnNames:
            if '(' in col and ')' in col:
                names = []
                names.append("")
                names.append("")
                a1 = col.split('(');
                names[0] = (re.sub(' +',' ',a1[0])).strip()
                names[1] = (re.sub(' +',' ',a1[1].split(')')[0])).strip()
                aggregate(names[0],names[1],tableNames[0],dictionary)
                return

            elif '(' in col or ')' in col:
                sys.exit("Syntax error")

    selectColumns(columnNames,tableNames,dictionary);

def selectColumns(columnNames,tableNames,dictionary):
    
    fileData = []
    if len(columnNames) == 1 and columnNames[0] == '*':
        columnNames = dictionary[tableNames[0]]

    for i in columnNames:
        if i not in dictionary[tableNames[0]]:
            sys.exit("error")

    printHeader(columnNames,tableNames,dictionary)
    tName = tableNames[0] + '.csv'
    readFile(tName,fileData)
    printData(fileData,columnNames,tableNames,dictionary)

def processWhereJoin(whereStr,columnNames,tableNames,dictionary):
    l1 = []
    l2 = []
    tableNames.reverse()
    fileData = []
    readFile(tableNames[0] + '.csv',l1)
    readFile(tableNames[1] + '.csv',l2)
    for item1 in l1:
        for item2 in l2:
            fileData.append(item2 + item1)
    dictionary["sample"] = []
    for i in range(len(dictionary[tableNames[1]])):
        dictionary["sample"].append(tableNames[1] + '.' + dictionary[tableNames[1]][i])
    for i in range(len(dictionary[tableNames[0]])):
        dictionary["sample"].append(tableNames[0] + '.' + dictionary[tableNames[0]][i])

    dictionary["test"] = dictionary[tableNames[1]] + dictionary[tableNames[0]]

    tableNames.remove(tableNames[0])
    tableNames.remove(tableNames[0])
    tableNames.insert(0,"sample")

    if(len(columnNames) == 1 and columnNames[0] == '*'):
        columnNames = dictionary[tableNames[0]]

    # print header
    for i in columnNames:
        print i,
    print

    a = whereStr.split(" ")

    check = 0
    for data in fileData:
        string = evaluate(a,tableNames,dictionary,data)
        for col in columnNames:
            if eval(string):
                check = 1
                if '.' in col:
                    print data[dictionary[tableNames[0]].index(col)],
                else:
                    print data[dictionary["test"].index(col)],
        if check == 1:
            check = 0
            print

    del dictionary['sample']

def aggregate(func,columnName,tableName,dictionary):
    
    if columnName not in dictionary[tableName]:
        sys.exit("error")
    if columnName == '*':
        sys.exit("error")
    fileData = []
    colList = []
    tName = tableName + '.csv'
    readFile(tName,fileData)
    for data in fileData:
        colList.append(int(data[dictionary[tableName].index(columnName)]))

    to_print = 'na'

    if func.lower() == 'avg':
        to_print = sum(colList)/len(colList)
    elif func.lower() == 'max':
        to_print = max(colList)
    elif func.lower() == 'sum':
        to_print = sum(colList)
    elif func.lower() == 'min':
        to_print = min(colList)
    elif func.lower() == 'distinct':
        distinct(colList,columnName,tableName,dictionary);
    else :
        to_print =  "ERROR\n+Unknown function : ", '"' + str(func) + '"'

    print to_print

def distinct(colList,columnName,tableName,dictionary):
    to_print = "OUTPUT :\n"
    string = tableName + '.' + columnName
    print to_print + (str(string))
    colList = list(OrderedDict.fromkeys(colList))
    size_colList = len(colList)
    for col in range(size_colList):
        print colList[col]

def distinctMany(columnNames,tableNames,dictionary):
    temp = []
    check = 0
    printHeader(columnNames,tableNames,dictionary)
    for tab in tableNames:
        tName = tab + '.csv'
        with open(tName,'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                for col in columnNames:

                    x_read.append('init')
                    x_read[0] = row[dictionary[tableNames[0]].index(col)]
                    if x_read[0] not in temp:
                        temp.append(x_read[0])
                        check = 1
                        print x_read[0],
                if check == 1 :
                    check = 0
                    print

def printHeader(columnNames,tableNames,dictionary):
    
    to_print = "OUTPUT : \n"
    # Table headers
    string = []
    string.append("")
    for col in columnNames:
        for tab in tableNames:
            if col in dictionary[tab]:
                if not string[0] == "":
                    string[0] += ','
                string[0] += tab + '.' + col
    print to_print+str(string)

def evaluate(a,tableNames,dictionary,data):
    string = []
    string.append("")
    for i in a:
        # print i
        if i == '=':
            string[0] += i*2
        elif i in dictionary[tableNames[0]] :
            string[0] += data[dictionary[tableNames[0]].index(i)]
        elif i.lower() == 'and' or i.lower() == 'or':
            string[0] += ' ' + i.lower() + ' '
        else:
            string[0] += i
        # print string
    return string[0]

def join(columnNames,tableNames,dictionary):
    tableNames.reverse()
    fileData = []
    l1 = []
    l2 = []
    dictionary["sample"] = []
    readFile(tableNames[0] + '.csv',l1)
    readFile(tableNames[1] + '.csv',l2)

    for item1 in l1:
        for item2 in l2:
            fileData.append(item2 + item1)

    for i in dictionary[tableNames[1]]:
        dictionary["sample"].append(tableNames[1] + '.' + i)
    for i in dictionary[tableNames[0]]:
        dictionary["sample"].append(tableNames[0] + '.' + i)

    dictionary["test"] = dictionary[tableNames[1]] + dictionary[tableNames[0]]

    tableNames.remove(tableNames[0])
    tableNames.remove(tableNames[0])
    tableNames.insert(0,"sample")

    if(len(columnNames) == 1 and columnNames[0] == '*'):
        columnNames = dictionary[tableNames[0]]

    # print header
    for i in range(len(columnNames)):
        print columnNames[i],
    print

    for data in fileData:
        for col in columnNames:
            if '.' in col:
                print data[dictionary[tableNames[0]].index(col)],
            else:
                print data[dictionary["test"].index(col)],
        print


if __name__ == "__main__":
    main()
