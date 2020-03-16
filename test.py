import os
path = 'user'
try:  
    os.mkdir(path)  
except OSError as error:  
    pass

path2 = path + "/0"
try:  
    os.mkdir(path2)  
except OSError as error:  
    pass