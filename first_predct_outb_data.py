import pandas as pd
import os
df = pd.DataFrame()
for i in os.listdir('./outb_data'):
    df = df.append(pd.read_csv('./outb_data/' + i, encoding='utf_8_sig'))
df.to_csv('./outb_fas.csv', index = False,  encoding='utf_8_sig')