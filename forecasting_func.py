
import time



def timeseries_motor():
    import datetime
    import sqlalchemy
    import time
    import numpy as np 
    import pandas as pd 
    from sklearn.metrics import mean_squared_error
    from math import sqrt
    from fbprophet import Prophet
    engine = sqlalchemy.create_engine("postgresql:/username:password@datadbins.cpmhro02yml1.ap-south-1.rds.amazonaws.com/repos")
    engine.connect()
    data=pd.read_sql('select * from dm_reports.v_lob_wise_business_data;',con=engine)
    cutoff=(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d'))
    data_motor=data[data.product_lob=='Motor Lob']
    data_motor['policy_issue_date']=pd.to_datetime(data.policy_issue_date)
    data_motor_train=pd.DataFrame({'ds':data_motor.policy_issue_date,'y':np.log(data_motor.net_premium)})
    data_motor_train['ds']=pd.to_datetime(data_motor_train.ds)
    data_motor_train=data_motor_train[(data_motor_train.ds>"2018-3-31") & (data_motor_train.ds<cutoff)]
    model_p=Prophet(interval_width=0.95,changepoint_prior_scale=0.04,n_changepoints=15)
    model_p.fit(data_motor_train)
    future = model_p.make_future_dataframe(periods=30,include_history=False)
    forecast = model_p.predict(future)
    #model_p.plot(forecast);
    fc=forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    fc[['yhat', 'yhat_lower', 'yhat_upper']]=fc[['yhat', 'yhat_lower', 'yhat_upper']].apply(np.exp)
    fc['yhat']=fc.yhat.astype(int)
    fc['yhat_lower']=fc.yhat_lower.astype(int)
    fc['yhat_upper']=fc.yhat_upper.astype(int)
    #test_set=data_motor[data_motor.policy_issue_date>'2018-6-2']
    #test_set=M[M.ds>'2018-5-27']
    #print(test_set.shape)
    #print(test_set.columns)
    fc['ds']=pd.to_datetime(fc['ds'])
    fc['week']=fc.ds.dt.weekofyear
    fc['month']=fc.ds.dt.month
    fc['date_of_forecasting']=(datetime.datetime.strftime(datetime.datetime.now() , '%Y-%m-%d'))
    fc['lob']='Motor'
    fc['office_code']='NA'
    fc.columns=['forecast_date','predicted_premium','predicted_lower','predicted_upper','forecast_week','forecast_month','run_date','lob','office_code']
    fc=fc[['run_date','lob','forecast_date','forecast_week','forecast_month','predicted_premium', 'predicted_lower', 'predicted_upper','office_code']]

    #output_dict={'date_of_forecasting':[(datetime.datetime.strftime(datetime.datetime.now() , '%Y-%m-%d'))],'forecasted_premium_collection':[fc.yhat.sum()],'lob':['Travel']}
    #output_df=pd.DataFrame(output_dict)
    fc.to_sql(name="premium_forecasting",if_exists="append",con=engine,schema="dm_reports",index=False)
    #output_dict={'date_of_forecasting':[(datetime.datetime.strftime(datetime.datetime.now() , '%Y-%m-%d'))],'forecasted_motor_premium_collection':[fc.yhat.sum()]}
    #output_dict={'date_of_forecasting':[(datetime.datetime.strftime(datetime.datetime.now() , '%Y-%m-%d'))],'forecasted_premium_collection':[fc.yhat.sum()],'lob':['Motor']}
    #output_df=pd.DataFrame(output_dict)
    #output_df.to_sql(name="premium_forecast",if_exists="append",con=engine,schema="dm_reports")
    print('inserted')
    #test_set.index=test_set.policy_issue_date
    #fc.index=fc.ds
    #rms = sqrt(mean_squared_error(fc.yhat.values,test_set.net_premium.values))
    #mape=mean_absolute_percentage_error(test_set.net_premium.values,fc.yhat.values)
    #return (rms,mape/6,sum(test_set.net_premium),sum(fc.yhat),(sum(test_set.net_premium)-sum(fc.yhat))/sum(test_set.net_premium), x ,y)
    #return (output_dict)


def timeseries_travel():
    import datetime
    import sqlalchemy
    import time
    import numpy as np 
    import pandas as pd 
    from sklearn.metrics import mean_squared_error
    from math import sqrt
    from fbprophet import Prophet
    engine = sqlalchemy.create_engine("postgresql://username:password@datadbins.cpmhro02yml1.ap-south-1.rds.amazonaws.com/repos")
    engine.connect()
    data=pd.read_sql('select * from dm_reports.v_lob_wise_business_data;',con=engine)
    cutoff=(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d'))
    data_motor=data[data.product_lob=='Travel Lob']
    data_motor['policy_issue_date']=pd.to_datetime(data.policy_issue_date)
    data_motor_train=pd.DataFrame({'ds':data_motor.policy_issue_date,'y':np.log(data_motor.net_premium)})
    data_motor_train['ds']=pd.to_datetime(data_motor_train.ds)
    data_motor_train=data_motor_train[(data_motor_train.ds>"2018-3-31") & (data_motor_train.ds<cutoff)]
    model_p=Prophet(interval_width=0.95,changepoint_prior_scale=0.04,n_changepoints=15)
    model_p.fit(data_motor_train)
    future = model_p.make_future_dataframe(periods=30,include_history=False)
    forecast = model_p.predict(future)
    #model_p.plot(forecast);
    fc=forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    fc[['yhat', 'yhat_lower', 'yhat_upper']]=fc[['yhat', 'yhat_lower', 'yhat_upper']].apply(np.exp)
    fc['yhat']=fc.yhat.astype(int)
    fc['yhat_lower']=fc.yhat_lower.astype(int)
    fc['yhat_upper']=fc.yhat_upper.astype(int)
    #test_set=data_motor[data_motor.policy_issue_date>'2018-6-2']
    #test_set=M[M.ds>'2018-5-27']
    #print(test_set.shape)
    #print(test_set.columns)
    fc['ds']=pd.to_datetime(fc['ds'])
    fc['week']=fc.ds.dt.weekofyear
    fc['month']=fc.ds.dt.month
    fc['date_of_forecasting']=(datetime.datetime.strftime(datetime.datetime.now() , '%Y-%m-%d'))
    fc['lob']='Travel'
    fc['office_code']='NA'
    fc.columns=['forecast_date','predicted_premium','predicted_lower','predicted_upper','forecast_week','forecast_month','run_date','lob','office_code']
    fc=fc[['run_date','lob','forecast_date','forecast_week','forecast_month','predicted_premium', 'predicted_lower', 'predicted_upper','office_code']]

    #output_dict={'date_of_forecasting':[(datetime.datetime.strftime(datetime.datetime.now() , '%Y-%m-%d'))],'forecasted_premium_collection':[fc.yhat.sum()],'lob':['Travel']}
    #output_df=pd.DataFrame(output_dict)
    fc.to_sql(name="premium_forecasting",if_exists="append",con=engine,schema="dm_reports",index=False)
    #output_dict={'date_of_forecasting':[(datetime.datetime.strftime(datetime.datetime.now() , '%Y-%m-%d'))],'forecasted_premium_collection':[fc.yhat.sum()],'lob':['Travel']}
    #output_df=pd.DataFrame(output_dict)
    #output_df.to_sql(name="premium_forecast",if_exists="append",con=engine,schema="dm_reports")
    print('inserted')


def timeseries_mobile():
    import datetime
    import sqlalchemy
    import time
    import numpy as np 
    import pandas as pd 
    from sklearn.metrics import mean_squared_error
    from math import sqrt
    from fbprophet import Prophet
    engine = sqlalchemy.create_engine("postgresql://username:password@datadbins.cpmhro02yml1.ap-south-1.rds.amazonaws.com/repos")
    engine.connect()
    data=pd.read_sql('select * from dm_reports.v_lob_wise_business_data;',con=engine)
    cutoff=(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d'))
    data_motor=data[data.product_lob=='Mobile Lob']
    data_motor['policy_issue_date']=pd.to_datetime(data.policy_issue_date)
    data_motor_train=pd.DataFrame({'ds':data_motor.policy_issue_date,'y':np.log(data_motor.net_premium)})
    data_motor_train['ds']=pd.to_datetime(data_motor_train.ds)
    #M=data_motor_train[np.abs(data_motor_train.y-data_motor_train.y.mean())<=(1*data_travel_train.y.std())]    
    data_motor_train=data_motor_train[(data_motor_train.ds>"2018-3-31") & (data_motor_train.ds<cutoff)]
    M=data_motor_train[np.abs(data_motor_train.y-data_motor_train.y.mean())<=(1*data_motor_train.y.std())]
    model_p=Prophet(interval_width=0.95)#,changepoint_prior_scale=0.04,n_changepoints=15)
    model_p.fit(M)
    future = model_p.make_future_dataframe(periods=30,include_history=False)
    forecast = model_p.predict(future)
    model_p.plot(forecast);
    fc=forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    fc[['yhat', 'yhat_lower', 'yhat_upper']]=fc[['yhat', 'yhat_lower', 'yhat_upper']].apply(np.exp)
    fc['yhat']=fc.yhat.astype(int)
    fc['yhat_lower']=fc.yhat_lower.astype(int)
    fc['yhat_upper']=fc.yhat_upper.astype(int)
    #test_set=data_motor[data_motor.policy_issue_date>'2018-6-2']
    #test_set=M[M.ds>'2018-5-27']
    #print(test_set.shape)
    #print(test_set.columns)
    fc['ds']=pd.to_datetime(fc['ds'])
    fc['week']=fc.ds.dt.weekofyear
    fc['month']=fc.ds.dt.month
    fc['date_of_forecasting']=(datetime.datetime.strftime(datetime.datetime.now() , '%Y-%m-%d'))
    fc['lob']='Mobile'
    fc['office_code']='NA'
    fc.columns=['forecast_date','predicted_premium','predicted_lower','predicted_upper','forecast_week','forecast_month','run_date','lob','office_code']
    fc=fc[['run_date','lob','forecast_date','forecast_week','forecast_month','predicted_premium', 'predicted_lower', 'predicted_upper','office_code']]

    fc.to_sql(name="premium_forecasting",if_exists="append",con=engine,schema="dm_reports",index=False)

    print('inserted')



def office_wise_forecast():
    import datetime
    import sqlalchemy
    import time
    import numpy as np 
    import pandas as pd 
    from sklearn.metrics import mean_squared_error
    from math import sqrt
    from fbprophet import Prophet
    office=['Ahmedabad', 'Aurangabad', 'Baroda', 'Bengaluru - Corporate Business',
       'Bengaluru Online Website Business', 'Bengaluru Retail Business',
       'Chennai', 'Dehradun', 'Delhi', 'Goa',
       'Hyderabad', 'Jaipur', 'Jalandhar', 'Jammu', 'Kolhapur', 'Kolkatta',
       'Ludhiana', 'Mumbai', 'Nasik', 'Pune', 'Rajkot', 'Surat',
       'Vijayawada', 'Vishakapatnam']
    office_codes=['10701   ', '12401   ', '12403   ', '12404   ', '12702   ',
       '11901   ', '12402   ', '12901   ', '12705   ', '13701   ',
       '13301   ', '13601   ', '12701   ', '10301   ', 
       '10101   ', '12903   ', '13001   ', '10302   ', '13702   ', '10801   ', '10501   '] #12703,12704,12904
    engine = sqlalchemy.create_engine("postgresql://UN:PA@datadbins.cpmhro02yml1.ap-south-1.rds.amazonaws.com/repos")
    engine.connect()
    df=pd.read_sql("select policy_issue_date::Date, office_code, office_name,sum(net_premium) net_premium from dm_marts.m_policy where product_lob ~* 'motor' and policy_status='A'group by policy_issue_date::Date,  office_code ,office_name",con=engine)
    data=df.pivot(index='policy_issue_date',columns='office_code',values='net_premium')
    data.index=pd.to_datetime(data.index)
    cutoff=(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d'))
    for office_code in office_codes:
        buffer=data[office_code]
        buffer_train=pd.DataFrame({'ds':buffer.index,'y':np.log1p(buffer.values)})
        buffer_train=buffer_train[(buffer_train.ds>='2018-4-1')&(buffer_train.ds<cutoff)]
        model=Prophet(interval_width=0.95)
        model.fit(buffer_train)
        future= model.make_future_dataframe(periods=30,include_history=False)
        forecast= model.predict(future)
        fc=forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
        fc[['yhat', 'yhat_lower', 'yhat_upper']]=fc[['yhat', 'yhat_lower', 'yhat_upper']].apply(np.expm1)
        fc['yhat']=fc.yhat.astype(int)
        fc['yhat_lower']=fc.yhat_lower.astype(int)
        fc['yhat_upper']=fc.yhat_upper.astype(int)
        fc['office_code']=office_code.strip(' ')
        fc['ds']=pd.to_datetime(fc['ds'])
        fc['week']=fc.ds.dt.weekofyear
        fc['month']=fc.ds.dt.month
        fc['date_of_forecasting']=(datetime.datetime.strftime(datetime.datetime.now() , '%Y-%m-%d'))
        fc['lob']='office'
        fc.columns=['forecast_date','predicted_premium','predicted_lower','predicted_upper','office_code','forecast_week','forecast_month','run_date','lob']
        fc=fc[['run_date','lob','forecast_date','forecast_week','forecast_month','predicted_premium', 'predicted_lower', 'predicted_upper','office_code']]
        fc.to_sql(name="premium_forecasting",if_exists="append",con=engine,schema="dm_reports",index=False)
        print(office_code)


while True:
    timeseries_motor()
    timeseries_travel()
    timeseries_mobile()
    time.sleep(10)
    #office_wise_forecast()

    print('waiting for next iteration')
    time.sleep(24*60*60)
    
