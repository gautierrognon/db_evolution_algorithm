import argparse
import psycopg2
import csv
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from datetime import timedelta, date
#we want the N biggest table in the data base
N = 15
ratio_table_name = 'contract'
ratio_table_size = 0
#connect to postgres and call all others function
def bdd_connection(username, passwrd, host_ip, port_number, database_name):
    try:
        connection = psycopg2.connect(user=username, password=passwrd,
            host=host_ip, port=port_number,
            database=database_name)
        cursor = connection.cursor()
        print("connected")

        #query the database to get the tables
        (record, ratio) = bdd_query_tables(cursor, database_name)
        print("query ok")

        (fig1, ax1) = init_plot()
        (fig2, ax2) = init_plot()
        (fig3, ax3) = init_plot()
        print("init ok")

        #process data for ratio biggest tables and contract table, plot data
        (date,table_size,table_name,table_size_tot,database_size) = data_processing(cursor, ratio, record, database_name,ax1,ax2,ax3)
        print("process ok")

        #save data in csv file
        save_in_csv(database_name,table_name,table_size_tot,date,table_size)
        print("data saved")

        label_text = "This graphic shows the evolution (in number of bytes of each table).".format(database_name)
        custom_my_plot(ax1, label_text, graphic_title = "Evolution of the size of {0}'s tables. Database_size: {1}".format(database_name, database_size))
        label_text = "This graphic shows the evolution of the table {0} (in number of bytes).".format(ratio_table_name)
        custom_my_plot(ax2, label_text, graphic_title= "graphic fo the table {0}. Table size: {1}".format(ratio_table_name,ratio_table_size))
        label_text = "This graphic shows the ratio (divide each table size by the {0} size) \n between {1} tables and the {0} table ".format(ratio_table_name, database_name)
        custom_my_plot(ax3, label_text, graphic_title = "ratio between {0} tables and {1}".format(database_name, ratio_table_name))
        print("custom plot ok")

        save_plot(fig1,database_name)
        save_plot(fig2,ratio_table_name)
        save_plot(fig3,'weighted')
        print("plot saved")

        #create html file
        make_html_file(database_name,ratio_table_name,'weighted')
        print("html file created")

    except (psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL",error)
    finally:
        #closing connection
        if(connection):
            cursor.close()
            connection.close()
            print("connection closed")

#query to get N biggest tables and contract table
def bdd_query_tables(cursor,database_name):
    #get the table name, the table size in "human readable" (ex: 250 MB), the table size in "classic version" (ex: 870490112L) and the database total size for the 15 biggest tables
    cursor.execute("SELECT table_name,pg_size_pretty(pg_total_relation_size(table_name)),pg_total_relation_size(table_name),pg_size_pretty(pg_database_size('{0}')),pg_database_size('{0}') \
        FROM information_schema.tables \
        WHERE table_schema = 'public' \
        ORDER BY pg_total_relation_size(table_name) DESC  LIMIT {1};".format(database_name,N))
    record = cursor.fetchall()

    #get the table name, the table size in "human readable" (ex: 250 MB), the table size in "classic version" (ex: 870490112L) and the database total size for the contract table
    cursor.execute("SELECT table_name,pg_size_pretty(pg_total_relation_size(table_name)),pg_total_relation_size(table_name),pg_size_pretty(pg_database_size('{0}')),pg_database_size('{0}') \
        FROM information_schema.tables \
        WHERE table_schema = 'public' and table_name = '{1}'\
        ORDER BY pg_total_relation_size(table_name) DESC  LIMIT 1;".format(database_name,ratio_table_name))
    ratio = cursor.fetchall()
    #get the size of the table we want to compare to the database
    for row in ratio:
    	global ratio_table_size
    	ratio_table_size = row[1]

    return record,ratio

#process data from cursor we send
def data_processing(cursor, data1, data2,database_name,ax1,ax2, ax3):
    #tab to store date,size,name,total size in bytes and number of row for each table
    tab_date=[]
    tab_size=[]
    table_name=[]
    table_size_tot=[]
    for row in data1 + data2:

    	tab_size_plot = []
    	tab_date_plot = []
        name = str(row[0])
        #size total of each table "human readable"
        size_tot = row[1]
        nb_total_bytes = row[2]
        database_size = row[3]
        database_nb_bytes = row[4]

        #check if it's a history table, we query for each table all the dates (create or write dates) and the number of element for each date
        if '__history' not in name:
            query = "SELECT create_date::date, count(id) \
            FROM %s \
            group by create_date::date \
            ORDER BY create_date::date" % (name)
        else:
            query = "SELECT COALESCE(write_date, create_date)::date, count(id) \
            FROM %s \
            GROUP BY COALESCE(write_date, create_date)::date \
            ORDER BY COALESCE(write_date,create_date)::date" % (name)

        cursor.execute(query)
        create_dates = cursor.fetchall()
        #number of element at specific date
        count_elem = 0
        #number of bytes at specific date
        count_bytes = 0
        nb_tot_elem = sum([x[1] for x in create_dates])

        tab_percentage = get_percent(nb_total_bytes,database_nb_bytes)

        for row in create_dates:
            count_elem = count_elem + row[1]
            count_bytes = (count_elem*nb_total_bytes)/nb_tot_elem
            tab_date.append(row[0])
            tab_size.append(count_bytes)
            table_name.append(name)
            table_size_tot.append(size_tot)
            tab_size_plot.append(count_bytes)
            tab_date_plot.append(row[0])

        #label for the plot legend
        label = "{0} : {1}%".format(name,tab_percentage)

        if name != ratio_table_name :
            
            my_plotter(ax1,tab_date_plot,tab_size_plot,label)
            tab_ratio = make_ratio(tab_date_plot,tab_size_plot,number_by_dates)
            my_plotter(ax3,tab_date_plot,tab_ratio,label)

        else :
            my_plotter(ax2,tab_date_plot,tab_size_plot,label)
            number_by_dates = create_all_dates_index(tab_date_plot,tab_size_plot)

    return tab_date, tab_size, table_name, table_size_tot, database_size

#save date un csv file
def save_in_csv(database_name, table_name, table_size_tot, date, table_size):
    with open('{0}.csv'.format(database_name), mode='a') as save_file:
    	save_writer = csv.writer(save_file, delimiter=',', quotechar='"',quoting=csv.QUOTE_MINIMAL)
    	for i in range (0,len(table_name)-1):
    		save_writer.writerow([table_name[i], table_size_tot[i], date[i], table_size[i]])

def daterange(start_date, end_date):
	for n in range(int ((end_date - start_date).days)):
		yield start_date + timedelta(n)

def create_all_dates_index(tableau_date,tableau_size):
    number_by_dates = dict(zip(tableau_date, tableau_size))
    start_date = tableau_date[0]
    end_date = tableau_date[-1]
    last = 1
    for single_date in daterange(start_date,end_date):
        number = number_by_dates.get(single_date)
        if not number:
            number = last
        number_by_dates[single_date] = number
        last = number
    return number_by_dates

#make ratio between one table and the N biggest tables
def make_ratio(num_date, num_size, denomi_dic):
    ratio_size = []
    last=0
    for i in range (0,len(num_date)):
        if num_date[i] in denomi_dic:
            numerator = num_size[i]
            denominator = denomi_dic[num_date[i]]
            ratio = numerator/denominator
            ratio_size.append(ratio)
            last = ratio
        else:
            ratio_size.append(last)
    return ratio_size

#create a figure for the plot
def init_plot():
    fig, ax = plt.subplots(1,1)
    plt.rc('font', size=9)
    plt.rc('legend', fontsize=15)
    return fig, ax

#plot data
def my_plotter(ax,data1,data2, table_name):
    ax.plot(data1, data2, label=table_name)

#custom lines color, label and title, legend
def custom_my_plot(ax, text, graphic_title):
    #color plot lines
    colormap = plt.cm.gist_ncar
    colors=[colormap(i) for i in np.linspace(0, 1, len(ax.lines))]
    for i,j in enumerate(ax.lines):
            j.set_color(colors[i])
    ax.set_xlabel(text)
    #set graphic's label and title
    ax.set(ylabel='size', title= graphic_title)
    #graphic legend
    ax.legend(bbox_to_anchor=(1.05,1),loc=2, borderaxespad=0.)

#save the plot
def save_plot(fig,file_name):
	fig.savefig(file_name , bbox_inches='tight')

#find percentage of a value 
def get_percent(size,size_tot):
    percent = (size * 100)/size_tot
    return percent

#make a html index to visualize data.
def make_html_file(name_image1,name_image2,name_image3):
    html_str = """
    <img src="{0}.png">
    <img src="{1}.png">
    <img src="{2}.png">
    """.format(name_image1,name_image2,name_image3)
    Html_file= open("index.html","w")
    Html_file.write(html_str)
    Html_file.close()



if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("user")
    parser.add_argument("password")
    parser.add_argument("host")
    parser.add_argument("port")
    parser.add_argument("base")
    args = parser.parse_args()
    bdd_connection(args.user,args.password,args.host,args.port,args.base)
