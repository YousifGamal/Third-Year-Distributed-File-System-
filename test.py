import os
path = 'client'
try:  
    os.mkdir(path)  
except OSError as error:  
    pass