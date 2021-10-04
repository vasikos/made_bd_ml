hdfs dfs -rm -r -skipTrash /nyc_airbnb/average.txt

mapred streaming 
    -mapper 'python3 ./mapper.py' 
    -reducer 'python3 ./reducer.py' 
    -input /nyc_airbnb/AB_NYC_2019.csv 
    -output /nyc_airbnb/average.txt 
    -file ./mapper.py -file ./reducer.py

hdfs dfs -head /nyc_airbnb/average.txt/part-00000
