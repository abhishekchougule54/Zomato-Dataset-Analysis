#import library

from sqlalchemy import create_engine
import pandas as pd


#read json and excel file from local machine
df1=pd.read_json('F:/0_Fun Projects/Project Files/datasets/zomato_dataset/file1.json')
df2=pd.read_json('F:/0_Fun Projects/Project Files/datasets/zomato_dataset/file2.json')
df3=pd.read_json('F:/0_Fun Projects/Project Files/datasets/zomato_dataset/file3.json')
df4=pd.read_json('F:/0_Fun Projects/Project Files/datasets/zomato_dataset/file4.json')
df5=pd.read_json('F:/0_Fun Projects/Project Files/datasets/zomato_dataset/file5.json')
df_country=pd.read_excel('F:/0_Fun Projects/Project Files/datasets/zomato_dataset/Country-Code.xlsx')
df_country.rename(columns = {'Country Code':'restaurant_country_id','Country':'restaurant_country'}, inplace = True)

df1=df1.dropna(subset=['restaurants'])
df2=df2.dropna(subset=['restaurants'])
df3=df3.dropna(subset=['restaurants'])
df4=df4.dropna(subset=['restaurants'])
df5=df5.dropna(subset=['restaurants'])


#concat all the files data
full_df=pd.concat([df1, df2,df3,df4])
full_df=full_df.reset_index()

rest_data=full_df['restaurants']
restaurant_parsed_data=[]

#for loop to extract json dataset and put it into restaurant_parsed_data list
for i in range(0,len(rest_data)):
    for k in range(0,len(rest_data[i])):

        rest_dict=rest_data[i][k]['restaurant']
        restuarant_name=''
        restuarant_has_online_delivery=''
        restuarant_average_cost_for_two=''
        restaurant_cuisines=''
        restaurant_address=''
        resaurant_city=''
        restaurant_country_id=''
        restaurant_aggregate_rating=''
        restaurant_latitude=''
        restaurant_longitude=''
        restaurant_user_votes=''
        for key in rest_dict:
            if key=='name':
                restuarant_name=rest_dict[key]
            if key =='has_online_delivery':
                restuarant_has_online_delivery=rest_dict[key]
            if key =='average_cost_for_two':
                restuarant_average_cost_for_two=rest_dict[key]
            if key =='cuisines':
                restaurant_cuisines=rest_dict[key]
            if key =='location':
                restaurant_address=rest_dict[key]['address']
                restaurant_city=rest_dict[key]['city']
                restaurant_country_id=rest_dict[key]['country_id']
                restaurant_latitude=rest_dict[key]['latitude']
                restaurant_longitude=rest_dict[key]['longitude']
            if key =='user_rating':
                restaurant_aggregate_rating=rest_dict[key]['aggregate_rating']
                restaurant_user_votes=rest_dict[key]['votes']

        restaurant_parsed_data.append([restuarant_name,restuarant_has_online_delivery,restuarant_average_cost_for_two,restaurant_cuisines,restaurant_address,restaurant_city,restaurant_country_id,restaurant_aggregate_rating,restaurant_latitude,restaurant_longitude,restaurant_user_votes])

columns = ['restuarant_name','restuarant_has_online_delivery','restuarant_average_cost_for_two','restaurant_cuisines','restaurant_address','restaurant_city','restaurant_country_id','restaurant_aggregate_rating','restaurant_latitude','restaurant_longitude','restaurant_user_votes']
restaurant_parsed_data_df = pd.DataFrame(restaurant_parsed_data, columns=columns)

#joining extarcted dataframe and country dataframe to get country name
restaurant_parsed_data_df=restaurant_parsed_data_df.merge(df_country, how="inner", on='restaurant_country_id')
restaurant_parsed_data_df=restaurant_parsed_data_df.drop_duplicates()

#insert data into PostgreSQL
engine = create_engine('postgresql://postgres:root@localhost:5432/zomato_analysis')
restaurant_parsed_data_df.to_sql('zomato_restaurant_analysis', engine,if_exists='append', index=False)