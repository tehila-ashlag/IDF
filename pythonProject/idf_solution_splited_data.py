import pandas as pd
import matplotlib.pyplot as plt
import dateutil.parser
import threading

lock=threading.Lock()
printing_info = []

def load_and_pre_process_data():
    global daily_data_windows
    data=pd.read_csv("time_series.csv")
    sorted_data=data.sort_values(by='timestamp')
    sorted_data.drop_duplicates(subset=['timestamp'],keep='last', inplace=True)
    sorted_data['timestamp']=pd.to_datetime(sorted_data['timestamp'],dayfirst=True)
    sorted_data.dropna(subset=['timestamp'], inplace=True)
    sorted_data.dropna(subset=['value'], inplace=True)
    sorted_data=sorted_data[sorted_data['value'].str.isnumeric()]
    sorted_data['group_by']=sorted_data['timestamp'].dt.date
    daily_data_windows=sorted_data.groupby(['group_by'])
def process_data_in_parallel():
    global sorted_printing_info
    threads=[]
    for daily_window in daily_data_windows:
        thread=threading.Thread(target=process_data(daily_window))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

    sorted_printing_info=sorted(printing_info, key=lambda tup: dateutil.parser.parse(tup[1],dayfirst=True),reverse=False)

def process_data(daily_window):
    sum=0
    cnt=0
    window_day=None
    current_row_hour=None
    for index,row in daily_window[1].iterrows():
        # assuming all data is about june month...
        row_date = row['timestamp']
        row_hour=row_date.hour
        if(window_day==None): #is init..
            window_day = row_date.day
            current_row_hour = row_hour
        else:
            if(current_row_hour!=row_hour):
                time = f"{window_day:02d}.06.2025 {current_row_hour}:00"
                avg=sum/cnt
                with lock:
                    printing_info.append((avg,time))
                current_row_hour=row_hour
                sum=0
                cnt=0
        sum += float(row['value'])
        cnt += 1
    time = f"{window_day:02d}.06.2025 {current_row_hour}:00"
    avg = sum / cnt
    with lock:
        printing_info.append((avg, time))



def print_results():
    fig,ax = plt.subplots()
    fig.patch.set_visible(False)
    fig.set_figheight(375)
    ax.axis('off')
    collabel = ("עצוממ", "הלחתה ןמז")
    the_table = ax.table(cellText=sorted_printing_info,
                         colLabels=collabel,loc='center')
    # plt.savefig('avgs_1.pdf')
    plt.show()

load_and_pre_process_data()
process_data_in_parallel()
print_results()