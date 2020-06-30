"""
@author: neera
"""
import sqlite3
import argparse
import json

#global parameters
normalize_table_names = False   #want to normalise table names or not?
normalize_column_names = False  #want to normalize column names or not
table_col_mapping = {}


def clean():
    print()
    print("######################")
    print()
    
  
def change_column_types(col_types):
    for i  in range(len(col_types)):
        col_type = col_types[i].lower()
        if col_type == "integer":
            col_type = 'number'
        col_types[i] = col_type    
    return col_types        
    
#input : cursor object , empty list
#returns a list of all the tables in the database
def find_all_tables(cursor,tables):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    results = cursor.fetchall()
    for result in results:
        tables.append(str(result[0]))
    print("Found the following tables from the database : ")
    print(tables)    
    clean()
    return tables      
 
    
#input : cursor object, columns =[-1,'*'](this is used by valuenet), list of all the tables in the database
#returns : columns, their datatypes, primary and foreigm keys relationships    
def find_all_columns_types_keys(cursor,columns,tables):
    col_types = []
    col_types.append('TEXT')
    foreign_keys = []
    primary_keys = []
    print("Found the following foreign keys relationships: ")
    for index,table in enumerate(tables):
        rows = cursor.execute("PRAGMA table_info({})".format((table)))
        table_info = rows.fetchall()
        for info in table_info:
            col_name = info[1]
            col_type = info[2]
            col = []
            col.append(index)
            col.append(col_name)
            columns.append(col)
            col_types.append(col_type)
            table_col_mapping[(table,col_name)] = len(columns)-1
           
 
    for table in tables:
        rows = cursor.execute("PRAGMA foreign_key_list({})".format((table)))
        fk_info = rows.fetchall()
        if len(fk_info):
           for fks in fk_info:
               src_table = table
               des_table = fks[2]
               src_col = fks[3]
               des_col = fks[4]
               f_key = []
               f_key.append(table_col_mapping[(src_table,src_col)])
               f_key.append(table_col_mapping[(des_table,des_col)])
               foreign_keys.append(f_key)
               print(src_col, " in ", src_table, " references ", des_col, " in ", des_table)
        rows = cursor.execute("PRAGMA table_info({})".format((table)))   
        pk_info = rows.fetchall()
        for pk in pk_info:
            if pk[len(pk)-1] == 1:
                primary_keys.append(table_col_mapping[(table,pk[1])])
    clean()
    print("Found the following primary keys from the database:")
    for pk in primary_keys:
        table = tables[columns[pk][0]]
        col_name = columns[pk][1]
        print(table ," --->" , col_name)
    clean()    
    cleaned_columns = [[-1,'*']]
    for col in range(1,len(columns)):
        temp_col =  columns[col][1].lower()
        temp_col = temp_col.split('_')
        col_name = " ".join(temp_col)
        temp_col = []
        temp_col.append(columns[col][0])
        temp_col.append(col_name)
        cleaned_columns.append(temp_col)
        
    return columns,cleaned_columns,col_types, foreign_keys, primary_keys
            
   


def normalize():
    return

if __name__ =="__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--sqlite_path', type=str, required=False,default = 'dealPlatform')
    arg_parser.add_argument('--database_id',type = str,default ="dealPlatform")
    args = arg_parser.parse_args()
    try :
        db = sqlite3.connect(args.sqlite_path+".db")
       
        cursor = db.cursor()
        spider_data = []
        
        db_info = {}
        db_info['db_id'] = args.database_id
        tables = find_all_tables(cursor,[])
        db_info['table_names_original'] = tables
        if normalize_table_names:
            db_info['table_names'] = normalize( db_info['table_names_original'] )
        else:
            db_info['table_names'] = db_info['table_names_original']
        
        columns, cleaned_columns, column_types, foreign_keys, primary_keys = find_all_columns_types_keys(cursor,[[-1,"*"]],tables)
        db_info['column_names_original'] = columns
        db_info['column_names'] = cleaned_columns
        # if normalize_column_names:
        #     db_info['column_names'] = normalize(db_info['column_names_original'])
        # else:
        #     db_info['column_names'] = columns
        db_info['column_types'] = change_column_types(column_types) 
        db_info['foreign_keys'] = foreign_keys
        db_info['primary_keys'] = primary_keys
        spider_data.append(db_info)
        with open("tables.json",'w') as f:
            json.dump(spider_data,f)
        
  
    except Exception as e:
            print("Exception: " + str(e))
        
        
        
    
    
    


# def find_all_columns(cursor,columns,tables): 
#     temp_cols = []
#     for index,table in enumerate(tables):
#         cursor.execute("select * from " +table)
#         for description in cursor.description:
#             col = []
#             col.append(index)
#             col.append(description[0])
#             columns.append(col)
#             temp_cols.append(description[0])
#     return columns, temp_cols  
            
            
                           
# def find_col_types(cursor,col_types,cols,tables): 
#     table_col_mapping = {}
#     for table in tables:
#         print(table)
#         rows = db.execute("PRAGMA table_info({})".format((table)))
#         table_col_mapping[table] = rows.fetchall()
     