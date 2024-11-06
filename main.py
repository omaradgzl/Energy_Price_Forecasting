#         _               _ _     _           __                                     _ _   
#   _ __ | | __ _ _   _  (_) |_  | |__   ___ / _| ___  _ __ ___   _ __ _   _ _ __   (_) |_ 
#  | '_ \| |/ _` | | | | | | __| | '_ \ / _ \ |_ / _ \| '__/ _ \ | '__| | | | '_ \  | | __|
#  | |_) | | (_| | |_| | | | |_  | |_) |  __/  _| (_) | | |  __/ | |  | |_| | | | | | | |_ 
#  | .__/|_|\__,_|\__, | |_|\__| |_.__/ \___|_|  \___/|_|  \___| |_|   \__,_|_| |_| |_|\__|
#  |_|            |___/            Ezhel&Aga B-Yoruldum                                                  



try:

    import dataBringer
    import dataPrepareator
    import os
    import pandas as pd
    import numpy as np
    import datetime

    import data
    import json
    # from prophet import Prophet
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense
    from sklearn.preprocessing import MinMaxScaler
    from sklearn.metrics import mean_absolute_percentage_error
    import optuna
    from optuna.integration import TFKerasPruningCallback
    from sklearn.metrics import mean_absolute_percentage_error
    from dbConn import DbConnection
    import warnings
    warnings.filterwarnings('ignore')

    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication
    import smtplib

    log_str = ''

    df_static = pd.read_excel('data_static.xlsx')

    def getCSVfileList():
        files = os.listdir()
        files = [x for x in files if x.endswith('csv')]
        return files

    day_dict = {
                0 : 'MON',
                1 : 'TUE',
                2 : 'WED',
                3 : 'THU',
                4 : 'FRI',
                5 : 'SAT',
                6 : 'SUN'
                }

    day = day_dict[datetime.datetime.today().weekday()]
    table_name = 'PTF_FCST_{}'.format(day)
    log_str += 'Today is {}.\n'.format(day)

    files = getCSVfileList()
    if len(files) > 0:
        log_str +='Found csv in folder. Deleting.\n'
        for x in files:
            os.remove(x) 

    conn = DbConnection(username= data.db_username, password= data.db_password, host = data.db_host, port= data.db_port, name = data.db_name)

    if day == 'MON' :
        len_table_before = len(conn.getDataFrame('SELECT * FROM {}'.format(table_name)))
        # len_table_main = len(conn.getDataFrame('SELECT * FROM PTF_FCST'))
        
        log_str += 'Table MON length before run {} \n'.format(len_table_before)
        # log_str += 'Table main length before run {} \n'.format(len_table_main)
    else : 
        len_table_before = len(conn.getDataFrame('SELECT * FROM {}'.format(table_name)))
        log_str += 'Table {} length before run {} \n'.format(day,len_table_before)


    dataBringer.downloadDataForecast()
    files = getCSVfileList()
    log_str +='CSV s downloaded.\n'

    data_epias = dataBringer.getDataTrainFromEpias()
    log_str +='EPIAS data is present.\n'

    data_m = pd.DataFrame()
    for file in files:

        df_sub = pd.read_csv(file)
        data_m = pd.concat([data_m, df_sub], axis = 1)




    df_train, df_test = dataPrepareator.train_test_split(data_epias=data_epias, data_m = data_m)
    df_train = pd.concat([df_static, df_train], ignore_index = True)


    df_train.set_index('ds', inplace=True)
    df_test.set_index('ds', inplace=True)
    X_train = df_train.drop(columns=['y'])
    y_train = df_train['y']
    X_test = df_test
    scaler_X = MinMaxScaler()
    scaler_y = MinMaxScaler()

    X_train_scaled = scaler_X.fit_transform(X_train)
    y_train_scaled = scaler_y.fit_transform(y_train.values.reshape(-1, 1))
    X_test_scaled = scaler_X.transform(X_test)

    TIME_STEPS = 1

    def create_dataset(X, y, time_steps=24):
        Xs, ys = [], []
        for i in range(len(X)):
            end_idx = i + time_steps
            if end_idx > len(X):
                break
            Xs.append(X[i:end_idx])
            ys.append(y[end_idx-1])
        return np.array(Xs), np.array(ys)

    def create_dataset_full(X, time_steps=24):
        Xs = []
        for i in range(len(X)):
            end_idx = i + time_steps
            if end_idx > len(X):
                break
            Xs.append(X[i:end_idx])
        return np.array(Xs)

    X_train_lstm, y_train_lstm = create_dataset(X_train_scaled, y_train_scaled, TIME_STEPS)
    X_test_lstm_full = create_dataset_full(X_test_scaled, TIME_STEPS)
    
    log_str += 'Train & pred ready.\n'
    log_str += 'Training started.\n'
    def create_model(trial):
        model = Sequential()
        
        lstm_units = trial.suggest_int('lstm_units', 50, 200)
        model.add(LSTM(units=lstm_units, return_sequences=True, input_shape=(X_train_lstm.shape[1], X_train_lstm.shape[2]))) # Thi is sibelcim 1:time steps, 2:num_features
        model.add(LSTM(units=lstm_units))

        model.add(Dense(1))

        optimizer = trial.suggest_categorical('optimizer', ['adam', 'sgd'])
        learning_rate = trial.suggest_loguniform('learning_rate', 1e-5, 1e-2)
        
        if optimizer == 'adam':
            optimizer = tf.keras.optimizers.Adam(learning_rate=learning_rate)
        else:
            optimizer = tf.keras.optimizers.SGD(learning_rate=learning_rate)
        
        model.compile(optimizer=optimizer, loss='mse')
        return model

    def objective(trial):
        model = create_model(trial)
        
        model.fit(X_train_lstm, y_train_lstm, 
                epochs=trial.suggest_int('epochs', 10, 50), 
                batch_size=trial.suggest_int('batch_size', 16, 128), 
                verbose=0)
        
        y_pred_train_scaled = model.predict(X_train_lstm)
        y_pred_train = scaler_y.inverse_transform(y_pred_train_scaled)
        
        mape = mean_absolute_percentage_error(scaler_y.inverse_transform(y_train_lstm), y_pred_train)
        
        return mape

    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=100, timeout=600, show_progress_bar=True)

    best_trial = study.best_trial
    model = create_model(best_trial)
    model.fit(X_train_lstm, y_train_lstm, epochs=best_trial.params['epochs'], batch_size=best_trial.params['batch_size'])

    y_pred_scaled_full = model.predict(X_test_lstm_full)
    y_pred_full = scaler_y.inverse_transform(y_pred_scaled_full)

    time_index = df_test.index[-len(y_pred_full):]
    y_pred_df_full = pd.DataFrame(data=y_pred_full, index=time_index, columns=['Predicted_PTF'])

    best_trial = study.best_trial

    pred = y_pred_df_full.rename(columns = {'Predicted_PTF':'yhat'}).reset_index()   

    log_str += 'Correcting max PTF value.\n'
    pred['yhat'].mask(pred['yhat'] > 3000.0, 3000.0 , inplace=True)
    pred['yhat'].mask(pred['yhat'] <= 0.0, 0.0 , inplace=True)

    log_str += 'Predictions inserting into database.\n'

    if day == 'MON' : 
        # conn.dataToDB(df = pred,table = 'PTF_FCST', if_there = 'append', index = False)
        conn.dataToDB(df = pred,table = table_name, if_there = 'append', index = False)
        # len_table_after_main = len(conn.getDataFrame('SELECT * FROM PTF_FCST'))
        len_table_after = len(conn.getDataFrame('SELECT * FROM {}'.format(table_name)))
        # log_str += 'Table MAIN length after run {} \n'.format(len_table_after_main)
        log_str += 'Table MON length after run {} \n'.format(len_table_after)

    else : 
        conn.dataToDB(df = pred,  table = table_name, if_there = 'append', index = False)
        len_table_after = len(conn.getDataFrame('SELECT * FROM {}'.format(table_name)))
        log_str += 'Table length after run {} \n'.format(len_table_after)
        

    log_str += '\n --Finito giocare, resultante importante.--\n'
except Exception as e:
     log_str+= str(e)
finally:
    msg = MIMEMultipart()
    msg['From'] = data.mail_username
    msg['To'] = 'omaradgzl0@gmail.com'
    cc_recipients=["hobalago@gmail.com"]
    msg['Cc'] = ', '.join(cc_recipients)

    msg['Subject'] = 'PTFLog'

    # E-posta metnini ekleyin


    message = """Merhabalar,
    PTF bu loglar ile tamamlandı.\n
    """ + log_str
    msg.attach(MIMEText(message, 'plain'))

    try:
        server = smtplib.SMTP(data.mail_host, data.mail_port)
        server.starttls()
        server.login(data.mail_username, data.mail_password)
        server.send_message(msg)
        server.quit()
        print("E-posta gönderildi ✉️")
    except Exception as e:
        print("E-posta gönderirken bir hata oluştu:", str(e))