import pandas as pd
import holidays
import numpy as np
import data


def train_test_split(data_epias, data_m):
    
    df_epias = data_epias.copy()

    data_m = data_m.loc[:,~data_m.columns.duplicated()].copy()
    df_m = data_m[['Timestamp start (Asia/Istanbul)',
                                    'Hydro generation Run of River forecast',
                                    'Hydro generation Conventional forecast',
                                    'Wind power forecast',
                                    'Photovoltaic',
                                    'Photovoltaic',
                                    'Power demand average forecast']]
    # df_m = df_m.drop(df_m.columns[[1,2,3]], axis = 1)
    
    exos = ['ds','river', 'dam', 'wind', 'sun1', 'sun2', 'alis']
    df_m = df_m.rename(columns=dict(zip(df_m.columns,exos)))
    df_m = df_m.iloc[:168]
    df_m['ds'] = pd.to_datetime(df_m['ds'])
    df_m['sun'] = df_m['sun1'] + df_m['sun2']
    df_m = df_m.drop(['sun1', 'sun2'], axis = 1)
    df_m.loc[:,["river", "dam", "wind", "alis", "sun"]] = df_m.loc[:,["river", "dam", "wind", "alis", "sun"]].multiply(1000, axis="index")
    df_m = df_m.reindex(columns = ['ds', 'river', 'dam', 'wind', 'sun', 'alis'])
    
    holiday = pd.DataFrame(columns=['tarih', 'holiday'])
    for date, name in sorted(holidays.Turkey(years=[2022, 2023,2024]).items()):
        holiday.loc[len(holiday)] = [date, name]
    holiday['tarih'] = pd.to_datetime(holiday['tarih'], format='%Y-%m-%d', errors='ignore')

    # df_epias_start = str(df_epias['ds'].iloc[0])
    # df_epias_end = str(df_epias['ds'].iloc[-1])
    df_epias_ds = df_epias['ds']
    df_epias['ds'] = pd.to_datetime(df_epias['ds']).dt.date
    df_epias = df_epias.set_index('ds').join(holiday.set_index('tarih')).reset_index(names = 'ds')
    # df_epias['ds'] = pd.date_range(start = df_epias_start, end = df_epias_end, freq = 'H')
    df_epias['ds'] = df_epias_ds

    # df_m_start = str(df_m['ds'].iloc[0])
    # df_m_end = str(df_m['ds'].iloc[-1])
    df_m_ds = df_m['ds']
    df_m['ds'] = pd.to_datetime(df_m['ds']).dt.date
    df_m = df_m.set_index('ds').join(holiday.set_index('tarih')).reset_index(names = 'ds')
    # df_m['ds'] = pd.date_range(start = df_m_start, end = df_m_end, freq = 'H')
    df_m['ds'] = df_m_ds

    df_epias['holiday'] = df_epias['holiday'].astype(str)
    df_epias.loc[df_epias['holiday'] != 'nan', 'holiday'] = 1
    df_epias.loc[df_epias['holiday'] == 'nan', 'holiday'] = 0
    n2 = df_epias[df_epias['ds'].isin(pd.to_datetime(df_epias['ds'] + pd.DateOffset(day=2)))].index[0]
    df_epias['is_holiday_lead_2'] = df_epias['holiday'].shift(-n2)
    df_epias.loc[df_epias['is_holiday_lead_2'].isnull(), 'is_holiday_lead_2' ] = 0
    df_epias['holiday'] = df_epias['holiday'].astype(int)
    df_epias['is_holiday_lead_2'] = df_epias['is_holiday_lead_2'].astype(int)

    df_m['holiday'] = df_m['holiday'].astype(str)
    df_m.loc[df_m['holiday'] != 'nan', 'holiday'] = 1
    df_m.loc[df_m['holiday'] == 'nan', 'holiday'] = 0
    # n2 = df_m[df_m['ds'].isin(pd.to_datetime(df_m['ds'] + pd.DateOffset(day=2)))].index[0]
    df_m['is_holiday_lead_2'] = df_m['holiday'].shift(-n2)
    df_m.loc[df_m['is_holiday_lead_2'].isnull(), 'is_holiday_lead_2' ] = 0
    df_m['holiday'] = df_m['holiday'].astype(int)
    df_m['is_holiday_lead_2'] = df_m['is_holiday_lead_2'].astype(int)

    df_epias['dayofmonth'] = df_epias['ds'].dt.day
    df_epias['dayofweek'] = df_epias['ds'].dt.dayofweek
    df_epias['quarter'] = df_epias['ds'].dt.quarter
    df_epias['month'] = df_epias['ds'].dt.month
    df_epias['year'] = df_epias['ds'].dt.year
    df_epias['dayofyear'] = df_epias['ds'].dt.dayofyear
    df_epias['weekofyear'] = df_epias['ds'].dt.isocalendar().week
    df_epias['hour'] =  df_epias['ds'].dt.hour
    df_epias['weekofyear'] = df_epias['weekofyear'].astype(int)

    df_m['dayofmonth'] = df_m['ds'].dt.day
    df_m['dayofweek'] = df_m['ds'].dt.dayofweek
    df_m['quarter'] = df_m['ds'].dt.quarter
    df_m['month'] = df_m['ds'].dt.month
    df_m['year'] = df_m['ds'].dt.year
    df_m['dayofyear'] = df_m['ds'].dt.dayofyear
    df_m['weekofyear'] = df_m['ds'].dt.isocalendar().week
    df_m['hour'] =  df_m['ds'].dt.hour
    df_m['weekofyear'] = df_m['weekofyear'].astype(int)

    df_epias['mondayRise'] = np.where((df_epias['dayofweek'] == 0) & ((df_epias['hour'] == 8) | (df_epias['hour'] == 9) ) | ((df_epias['hour'] >= 17) & (df_epias['hour'] <=21)), 1, 0)
    df_epias['tuesdayRise'] = np.where((df_epias['dayofweek'] == 1) & ((df_epias['hour'] == 8) | (df_epias['hour'] == 9) ) | ((df_epias['hour'] >= 17) & (df_epias['hour'] <=21)), 1, 0)
    df_epias['wednesdayRise'] = np.where((df_epias['dayofweek'] == 2) & ((df_epias['hour'] == 8) | (df_epias['hour'] == 9) ) | ((df_epias['hour'] >= 17) & (df_epias['hour'] <=21)), 1, 0)
    df_epias['thursdayRise'] = np.where((df_epias['dayofweek'] == 3) & ((df_epias['hour'] == 8) | (df_epias['hour'] == 9) ) | ((df_epias['hour'] >= 17) & (df_epias['hour'] <=21)), 1, 0)
    df_epias['fridayRise'] = np.where((df_epias['dayofweek'] == 4) & ((df_epias['hour'] == 8) | (df_epias['hour'] == 9) ) | ((df_epias['hour'] >= 17) & (df_epias['hour'] <=21)), 1, 0)
    df_epias['saturdayRise'] = np.where((df_epias['dayofweek'] == 5) & ((df_epias['hour'] >= 18) & (df_epias['hour'] <=21)), 1, 0)
    df_epias['sundayRise'] = np.where((df_epias['dayofweek'] == 6) & (df_epias['hour'] == 19), 1, 0)
    df_epias['highestDay'] = np.where((df_epias['dayofweek'] == 3),  1, 0)

    df_m['mondayRise'] = np.where((df_m['dayofweek'] == 0) & ((df_m['hour'] == 8) | (df_m['hour'] == 9) ) | ((df_m['hour'] >= 17) & (df_m['hour'] <=21)), 1, 0)
    df_m['tuesdayRise'] = np.where((df_m['dayofweek'] == 1) & ((df_m['hour'] == 8) | (df_m['hour'] == 9) ) | ((df_m['hour'] >= 17) & (df_m['hour'] <=21)), 1, 0)
    df_m['wednesdayRise'] = np.where((df_m['dayofweek'] == 2) & ((df_m['hour'] == 8) | (df_m['hour'] == 9) ) | ((df_m['hour'] >= 17) & (df_m['hour'] <=21)), 1, 0)
    df_m['thursdayRise'] = np.where((df_m['dayofweek'] == 3) & ((df_m['hour'] == 8) | (df_m['hour'] == 9) ) | ((df_m['hour'] >= 17) & (df_m['hour'] <=21)), 1, 0)
    df_m['fridayRise'] = np.where((df_m['dayofweek'] == 4) & ((df_m['hour'] == 8) | (df_m['hour'] == 9) ) | ((df_m['hour'] >= 17) & (df_m['hour'] <=21)), 1, 0)
    df_m['saturdayRise'] = np.where((df_m['dayofweek'] == 5) & ((df_m['hour'] >= 18) & (df_m['hour'] <=21)), 1, 0)
    df_m['sundayRise'] = np.where((df_m['dayofweek'] == 6) & (df_m['hour'] == 19), 1, 0)
    df_m['highestDay'] = np.where((df_m['dayofweek'] == 3),  1, 0)

    df_epias.loc[(df_epias['ds'] >= '2023-01-01 00:00:00') & (df_epias['ds'] <= '2023-01-31 23:00:00'), 'azamiFiyat'] = 4200 
    df_epias.loc[(df_epias['ds'] >= '2023-02-01 00:00:00') & (df_epias['ds'] <= '2023-02-28 23:00:00'), 'azamiFiyat'] = 3650 
    df_epias.loc[(df_epias['ds'] >= '2023-03-01 00:00:00') & (df_epias['ds'] <= '2023-03-31 23:00:00'), 'azamiFiyat'] = 3050 
    df_epias.loc[(df_epias['ds'] >= '2023-04-01 00:00:00') & (df_epias['ds'] <= '2023-06-30 23:00:00'), 'azamiFiyat'] = 2600
    df_epias.loc[(df_epias['ds'] >= '2023-07-01 00:00:00') & (df_epias['ds'] <= '2024-06-30 23:00:00'), 'azamiFiyat'] = 2700 
    df_epias.loc[df_epias['ds'] >= '2024-07-01 00:00:00', 'azamiFiyat'] = 3000 

    df_m['azamiFiyat'] = 3000 



    return df_epias, df_m










