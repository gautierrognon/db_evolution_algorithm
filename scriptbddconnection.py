
import psycopg2
import argparse 
import datetime
from datetime import timedelta
import matplotlib
import matplotlib.pyplot as plt
import webbrowser
import numpy as np

def show_bdd_evolution(username, passwrd, host_ip, port_number, database_name):
    try:
        connection = psycopg2.connect(user = username,password = passwrd, host = host_ip, port = port_number, database = database_name)

        cursor = connection.cursor()
        # Get the name and the size in human version for each table
        cursor.execute("SELECT table_name,pg_size_pretty(pg_total_relation_size(table_name)),pg_total_relation_size(table_name),\
        	pg_size_pretty(pg_database_size('"+ database_name +"')) \
        	FROM information_schema.tables \
        	WHERE table_schema = 'public' \
        	ORDER BY pg_total_relation_size(table_name) DESC  LIMIT 15;")
        record = cursor.fetchall()

        #plot
        fig = plt.figure()
        ax = fig.add_subplot(111)
        plt.rc('font', size=9)
        plt.rc('legend', fontsize=15)
        

        #tab that will be save in the txt (names,size and size(pretty))
        tab_save_name = []
        tab_save_size = []
        tab_save_size_pretty = []

        # for each table 
        for row in record:
        	database_size = row[3]
        	#print table name and size just a check to see if it goes through every table
        	print(row[0] , row[1], row[2])
        	#we store tables name for later use
        	table_name = str(row[0])
        	tab_save_name.append(str(row[0]))
        	tab_save_size_pretty.append(str(row[1]))
        	tab_save_size.append(row[2])
        	#for non_history table
        	if '__history' not in table_name:
        		query = "SELECT create_date::date, count(id) FROM %s \
        		group by create_date::date ORDER BY create_date::date" % (table_name)
        	

        	else:
        		query = "SELECT COALESCE(write_date, create_date)::date, count(id) \
        		FROM %s GROUP BY COALESCE(write_date, create_date)::date \
        		ORDER BY COALESCE(write_date,create_date)::date" % (table_name)

        	cursor.execute(query)
        	create_dates = cursor.fetchall()
        	#tabs to store values
        	tab_date = []
        	tab_size = []
        	count = 0
        	count_tot = 0
        	nb_tot_bytes = row[2]
        	count_bytes = 0

        	#count the maximum of line in the table
        	count_tot = sum([x[1] for x in create_dates])

        	#count the number of bytes added each day and store them in a tab
        	for row in create_dates:
        		tab_date.append(row[0])
        		count = count + row[1]
        		count_bytes = (count*nb_tot_bytes)/count_tot
        		tab_size.append(count_bytes)

        	ax.plot(tab_date, tab_size, label= table_name)

        #define color,legend and label for the plot
        colormap = plt.cm.gist_ncar   
        colors = [colormap(i) for i in np.linspace(0, 1,len(ax.lines))]
        for i,j in enumerate(ax.lines):
        	j.set_color(colors[i])
        #set label and title
        ax.set(ylabel='size', title = database_name + ": "+database_size)
        #legend
        ax.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

        #save plot with the table name as png
        fig.savefig(database_name +".png", bbox_inches='tight')

        
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")



if __name__ == '__main__':

 #    #test
	# user = "postgres"
	# password= "postgres"S
	# host = "127.0.0.1"
	# port = "5432"
	# base = "restored_database"
	# show_bdd_increase(user,password,host,port,base)


    
    parser = argparse.ArgumentParser()
    parser.add_argument("user")
    parser.add_argument("password")
    parser.add_argument("host")
    parser.add_argument("port")
    parser.add_argument("base")
    args = parser.parse_args()
    show_bdd_evolution(args.user,args.password,args.host,args.port,args.base)