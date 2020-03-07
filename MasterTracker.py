from multiprocessing import *
from main import *


df = pd.DataFrame(columns=['user id','file name ','data node number','file path on that data node'
                               ,'is data node alive'])


print(df)
manager = Manager()
ns = manager.Namespace()
ns.df = df

lock = manager.Lock()
Process(target=all, args=(ns,lock,1)).start()
Process(target=all, args=(ns, lock,0)).start()
time.sleep(10)
print(ns.df)


