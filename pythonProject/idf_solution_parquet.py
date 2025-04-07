import pandas as pd
import matplotlib.pyplot as plt
import dateutil.parser

printing_info = []

def load_and_pre_process_data():
    global sorted_data
    data=pd.read_parquet('time_series.parquet', engine='pyarrow')
    # מעלות: ביצועים טובים יותר לשליפות מוקדות- מתפקס על הדאטה שצריך ולא סורק שורה שורה
    # open source משתפר ומתוחזק כל הזמן עי הקהילה
    # מכיל גם מטה דאטה כמו סכמות ובכך מכיל על השירותים המשתמשים בזה
    # תומך ביכולות כיווץ קובץ פר data type ועוד יכולות כיווץ מתקדמות
    # תומך באיחוד קבצים והסכמות שלהם
    sorted_data=data.sort_values(by='timestamp')
    sorted_data.drop_duplicates(subset=['timestamp'],keep='last', inplace=True)
    # TODO recheck if date assign is ok or not needed
    sorted_data['timestamp']=pd.to_datetime(sorted_data['timestamp'],dayfirst=True)
    sorted_data.dropna(subset=['timestamp'], inplace=True)
    sorted_data.dropna(subset=['value'], inplace=True)
    sorted_data=sorted_data[sorted_data['value'].str.isnumeric()]

def process_data():
    sum=0
    cnt=0
    current_row_day,current_row_hour=1,0
    for index,row in sorted_data.iterrows():
        # assuming all data is about june month...
        row_date=row['timestamp']
        row_day =row_date.day
        row_hour=row_date.hour
        if(current_row_day==row_day and current_row_hour==row_hour):
            sum+=float(row['value'])
            cnt+=1
        else:
            time = f"{current_row_day:02d}.06.2025 {current_row_hour}:00"
            avg=sum/cnt
            printing_info.append((avg,time))
            current_row_hour=row_hour
            sum=float(row['value'])
            cnt=1
            if(current_row_day!=row_day):
                current_row_day = row_day

def print_results():
    fig,ax = plt.subplots()
    fig.patch.set_visible(False)
    fig.set_figheight(375)
    ax.axis('off')
    collabel = ("עצוממ", "הלחתה ןמז")
    the_table = ax.table(cellText=printing_info,
                         colLabels=collabel,loc='center')
    # plt.savefig('avgs_1.pdf')
    plt.show()

load_and_pre_process_data()
process_data()
print_results()

