import pandas as pd
import numpy as np
from datetime import timedelta
import time 
# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,
                               QueryOptions)
import couchbase.subdocument as SD

endpoint = ""
username = ""
password = ""
bucket_name = "travel-sample"


def connected():
    auth = PasswordAuthenticator(username, password)

    # Connect options - global timeout opts
    timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10))

    # get a reference to our cluster
    cluster = Cluster('couchbases://{}'.format(endpoint),
                  ClusterOptions(auth, timeout_options=timeout_opts))

    # Wait until the cluster is ready for use.
    cluster.wait_until_ready(timedelta(seconds=5))
    cb = cluster.bucket("travel-sample")
    collection = cb.scope("inventory").collection("airport")
    return collection,cb,cluster



def get_all_table(cl):
    sql_all = """SELECT * FROM `travel-sample`.inventory.airport"""
    abba = cl.query(sql_all)
    finish = pd.DataFrame()
    for i in abba:
        copied = i['airport'].copy()
        geo_1 = copied['geo']
        del copied['geo']
        a = pd.DataFrame(copied,index = [0])
        b = pd.DataFrame(geo_1,index = [0])
        c = pd.concat([a,b],axis = 1)
        finish = pd.concat([finish,c],ignore_index=True)
    return finish

if __name__=='__main__':
    start = time.time()
    coll,bucket,clu = connected()
    first_table = get_all_table(clu)
    first_table.to_csv("first.csv")
    
    test_data = np.random.randint(low = 1950, high = 2010,size = first_table.shape[0])

    for i,j in zip(test_data,first_table['id']):
        coll.mutate_in("airport_{}".format(j), [SD.upsert("year",int(i))])
    second_table = get_all_table(clu)
    second_table.to_csv("second_table.csv")

    res = clu.query("""SELECT id,year from `travel-sample`.inventory.airport """).execute()
    res2 = pd.DataFrame(res)
    
    third_table =first_table.merge(res2,on = 'id')
    third_table.to_csv("third_table.csv")
    finish = time.time()
    print("script runs for {} seconds".format(finish-start)) 




