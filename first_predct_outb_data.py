import pandas as pd
import os
df = pd.DataFrame()
os.listdir('./outb_data')
pd.read_excel('./outb_data/2019.xlsx')
for i in os.listdir('./outb_data'):
    df = pd.concat([df, pd.read_excel('./outb_data/' + i)],\
        axis = 0)
 
df.to_csv('./outb_fas.csv', index = False,  encoding='utf_8_sig')

import easyocr
