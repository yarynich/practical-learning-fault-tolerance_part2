import asyncio, psycopg2
from os import close
import asyncpg
import paramiko, os

def iptables():
    host = 'sql1.ypa.local'
    user = 'root'
    passwd = 'qWe12345'
    port = 22

    customer = paramiko.SSHClient()
    customer.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    customer.connect(hostname=host, username=user, password=dpasswd, port=port)
    stdin, stdout, stderr = customer.exec_command('sh /home/Desktop/lab2/network.sh')
    customer.close()    

def check_ping(hostname):
    response = os.system("ping " + hostname)
    return response

def compare():
    test = 0
    while check_ping("sql1.ypa.local") == 0:
        connect1 = psycopg2.connect(dbname='bd2', user='pham', 
                            password='qwe123', host='sql1.ypa.local')
        cursor1 = connect1.cursor()
        cursor1.execute('SELECT COUNT(*) FROM pushes;')
        out1 = cursor1.fetchone()
        connect2 = psycopg2.connect(dbname='bd2', user='pham', 
                            password='qwe123', host='sql2.ypa.local')
        cursor2 = connect2.cursor()
        cursor2.execute('SELECT COUNT(*) FROM pushes;')
        out2 = cursor2.fetchone()

        print(f"В первой базе данных {out1[0]} записей.\nВо второй базе данных {out2[0]} записей.")

async def run():
    connection = await asyncpg.connect(database='bd2', user='pham', 
                        password='qwe123', host='sql1.ypa.local', command_timeout='2')
    try:
        i = 0
        while i < 500:
            async with connection.transaction():
                await connection.execute(f"insert into pushes(id, time) values ('{i}', 'now()');")
            print (i)
            if i == 250:
                iptables()
            i = i + 1
    except:
        pass

loop = asyncio.get_event_loop()
loop.run_until_complete(run())

compare()
