# Stock-Market-Analysis-and-Prediction-using-LSTM

[ Yahoo Finance API ]
        |
        v
[ Kafka Producer (extract_and_publish) ]
        |
        v
[ Kafka Topic: stock_topic ]
        |
        v
[ Spark Consumer (spark_consumer_HDFS) ]
        |
        v
[ HDFS / CSV Storage ]
        |
        +--> [ LSTM Training (train_lstm_all.py) ] --> [ Model (.h5) & Scaler (.pkl) ]
        |                                                   |
        |                                                   v
        +--------------------> [ Prediction Service (predict.py) ]
                                                        |
                                                        v
                                                [ Dashboard UI ]
