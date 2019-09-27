import pyodbc
import pandas as pd
import time

#for driver in pyodbc.drivers():
#    print(driver)

#First we use the connection string to establish a connection (it works with trusted connection)
try :
    conn=pyodbc.connect(
        "Driver= {ODBC Driver 17 for SQL Server};"
        "Server=DESKTOP-SO86PLC;"
        "Database=S19SQLPlayground_Seb;"
        "Trusted_Connection=yes;"
    )

except pyodbc.Error :
      print("Couldn't connect Please check your connection stream")

#conn1 = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.1.114;PORT=1433;DATABASE=S19SQLPlayground_Seb;UID=sa;PWD=vc5.sophia.05')

else:
    start=time.time()  #start the timer
    # we define a read function that read and loads the querry result set into a pandas dataframe
    def read(conn,table_name):
        print(f"Read {table_name}")
        dataf=pd.read_sql(f"SELECT * FROM dbo.{table_name};",conn)
        return dataf

    querycust="Customer"
    queryprod="Product"
    querypurch="Purchase"

    AllCust=read(conn,querycust)
    AllCust=AllCust.set_index('CustomerId')

    AllProd=read(conn,queryprod)
    AllProd=AllProd.set_index('ProductId')

    AllPurch=read(conn,querypurch)
    AllPurch=AllPurch.set_index(['CustomerId','ProductId'])

    CustPurchCount=pd.DataFrame({'CustomerId':[],'ProductCount':[]})

    #the lenght of the index gives us the number of products
    nb_prod=len(AllProd.index)

    #counting of the number of distinct products purchased by each custumer and
    #storing it into CustPurchCount Dataframe

    for  cust in AllCust.iterrows():
        for  prod in AllProd.iterrows():
            for  purch in AllPurch.iterrows():
                if (cust[0]==purch[0][0]) &  (prod[0]==purch[0][1]):
                    if cust[0] not in list(CustPurchCount['CustomerId']):
                        CustPurchCount=CustPurchCount.append({'CustomerId': cust[0] , 'ProductCount' : 1} , ignore_index=True)
                    else:
                        CustPurchCount.loc[CustPurchCount['CustomerId']==cust[0],['ProductCount']]+=1


    #The custumer id of the division querry result
    divresult_id =CustPurchCount[[nb_prod==i for i in list(CustPurchCount['ProductCount']) ]]['CustomerId']

    end =time.time()
    #printing of the results and timing
    print(f'The result of the division query is : \n  {AllCust.loc[divresult_id,:]}')
    print(f'This took {end-start}  ms')
    conn.close()
      
