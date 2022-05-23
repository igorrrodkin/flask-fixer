table_structure = ''
columns = ''
excluded = ''

columns_types = [{"name":"ABC" , "price":123.45},{"name":"DEF" , "price":45.645}]
on_conflict_column = list(columns_types[0].keys())[0]
# print('on conflict:' , on_conflict_column)
for key, value in columns_types[0].items():
    print(key)
    if (isinstance(value , str)):
        table_structure += f'{key}  TEXT |\n'
    if (isinstance(value , (float , int))):
        table_structure += f'{key}  FLOAT |\n'
    columns += f'{key} |'
    excluded += f'{key} = EXCLUDED.{key} |\n'
columns = ','.join(columns.split(' ')[:-1])
excluded = ','.join(excluded.split('|')[:-1])
table_structure = ','.join(table_structure.split('|')[:-1])

print(columns)
# print('#################')
# print(excluded)
# print('################')
# print(table_structure)


# string = 'abc'
# print(isinstance(string , int))
