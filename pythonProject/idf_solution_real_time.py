import pandas as pd
import matplotlib.pyplot as plt
import dateutil.parser
import numpy as np
import math

printing_info = {}

def process_data_arrived(msg):
    # msg >timestamp, value
    msg_date = dateutil.parser.parse(msg['timestamp'], dayfirst=True)
    msg_hour = msg_date.hour
    msg_day=msg_date.day
    time = f"{msg_day:02d}.06.2025 {msg_hour}:00"
    if time in printing_info:
        prev_cnt=printing_info[time]['cnt']
        prev_avg=printing_info[time]['avg']
        cnt=prev_cnt+1
        sum=prev_cnt*prev_avg+ msg['value']
        avg=sum/cnt
        printing_info[time]['avg']=avg
        printing_info[time]['cnt'] = cnt
    else:
        printing_info[time]={'avg': msg['value'],'cnt':1}


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


process_data_arrived({'timestamp':"02-06-2025 13:00",'value':123})
process_data_arrived({'timestamp':"02-06-2025 14:10",'value':456})
process_data_arrived({'timestamp':"02-06-2025 12:00",'value':123})
process_data_arrived({'timestamp':"02-06-2025 12:10",'value':456})
print(printing_info)
# print_results()