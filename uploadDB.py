import pandas as pd
import mysql.connector
import re
import json

# To store DB credentials
cred = json.load(open('DBCredentials.json', 'r'))

def insert_channel_in_DB( dict_, chop_details = False):
    
    # Connect to the database
    cnx = mysql.connector.connect(
                                host=cred['host'],
                                database=cred['name'],
                                user=cred['user'],
                                password=cred['pass'])

    # Create a cursor
    cursor = cnx.cursor()

    # Channel evolution only:
    if chop_details:
        dict_ = { 'channel_evolution': dict_['channel_evolution'] }


    for key_name in dict_.keys():

        if key_name == 'channel_details':
            
            # Fixing data types
            dict_[key_name]['CreatedOn'] = pd.to_datetime( dict_[key_name]['CreatedOn'] ).dt.strftime("%Y-%m-%d")

            #dict_[key_name]['CreatedOn'] = dict_[key_name]['CreatedOn'].dt.strftime("%Y-%m-%d")

            # Insert some data
            data = list(dict_[key_name].to_records())

            insert_query = (
                f"INSERT INTO {key_name} "
                "(ChannelName, CreatedOn, UploadPlaylist, ChannelID) "
                "VALUES (%s, %s, %s, %s)"
            )

        if key_name == 'channel_evolution':
            
            # Fixing data types
            dict_[key_name]['ExtractionDay'] = pd.to_datetime( dict_[key_name]['ExtractionDay'] ).dt.strftime("%Y-%m-%d")

            # Insert some data
            data = list(dict_[key_name].to_records())
            data = [(a,b,int(c),int(d),int(e)) for (a,b,c,d,e) in data] # Data transformation for SQL

            insert_query = (
                f"INSERT INTO {key_name} "
                "(ChannelID, ExtractionDay, views, subscribers, videoNr) "
                "VALUES (%s, %s, %s, %s, %s)"
            )

        cursor.executemany(insert_query, data)

        # Make sure the changes are committed to the database
        cnx.commit()

        print(f"Inserted: {key_name}")

    # Close the cursor and connection
    cursor.close()
    cnx.close()

def remove_emojis(data):
    emoj = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+", re.UNICODE)
    return re.sub(emoj, '', data)

def remove_links_and_line_jumps(text):
    # Remove links
    text = re.sub(r'http\S+', 'LINK', text)
    text = re.sub(r'www\S+', 'LINK', text)
    #text = re.sub(r'http|www\S+', 'LINK', text)

    # Remove line jumps
    text = text.replace('\n', ' ')
    
    return text

def remove_accents(text):
    # Replace accented characters with their unaccented counterparts
    text = text.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
    text = text.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')

    return text

def clean_video_statics(df):

    """ cleans the table video_statics, with the intention to only leave videos missing on the DB (newest videos) """

    # Connect to the database
    cnx = mysql.connector.connect(
                                host=cred['host'],
                                database=cred['name'],
                                user=cred['user'],
                                password=cred['pass'])

    video_ids_in_db = pd.read_sql("SELECT video_id FROM video_statics", cnx)['video_id'].to_list()

    # Clean first character (-)
    #video_ids_in_db = [x[1:] for x in video_ids_in_db]

    cnx.close()

    # Remove the list of videos already in the DB
    df_ = df[~df['video_id'].isin( video_ids_in_db )].copy()

    return df_
    

def video_tables_db_upload( tables, chop_details = False ):
    
    # Connect to the database
    cnx = mysql.connector.connect(
                                host=cred['host'],
                                database=cred['name'],
                                user=cred['user'],
                                password=cred['pass'])

    # Create a cursor
    cursor = cnx.cursor()

    # Channel evolution only:
    if chop_details:
        tables = { 'video_variables': tables['video_variables'] }
    else:
        tables = { 'video_statics': tables['video_statics'], 'video_variables': tables['video_variables'] }


    for table_name in tables.keys():

        if table_name == 'video_statics':
            
            # Fixing data types
            tables[table_name]['CreatedOn'] = pd.to_datetime( tables[table_name]['CreatedOn'] ).dt.strftime("%Y-%m-%d")

            # Ensuring no duplicates:
            tables['video_statics'] = clean_video_statics(tables['video_statics']).copy()
            
            # Removing any emojis from the text
            #tables[table_name]['title'] = tables[table_name]['title'].apply(remove_emojis).apply(remove_links_and_line_jumps).apply(remove_accents)
            #tables[table_name]['descr'] = tables[table_name]['descr'].astype(str).apply(remove_emojis).apply(remove_links_and_line_jumps).apply(remove_accents)

            # Insert some data
            data = list(tables[table_name].to_records())
            #print(len(data[0]))
            data = [(a,b,c,d,e,int(f)) for (index,a,b,c,d,e,f) in data] # Data transformation for SQL

            insert_query = (
                f"INSERT INTO {table_name} "
                "(ChannelID, video_id, CreatedOn, title, descr, duration) "
                "VALUES (%s, %s, %s, %s, %s, %s)"
            )

        if table_name == 'video_variables':
            
            # Fixing data types
            tables[table_name]['ExtractionDay'] = pd.to_datetime( tables[table_name]['ExtractionDay'] ).dt.strftime("%Y-%m-%d")

            # Insert some data
            data = list(tables[table_name].to_records())
            data = [(a,b,int(c),int(d),int(e)) for (index,a,b,c,d,e) in data] # Data transformation for SQL

            insert_query = (
                f"INSERT INTO {table_name} "
                "(video_id, ExtractionDay, viewCount, likeCount, commentCount) "
                "VALUES (%s, %s, %s, %s, %s)"
            )

        print(f"Inserting: {table_name}...")
        cursor.executemany(insert_query, data)

        # Make sure the changes are committed to the database
        cnx.commit()

        print(f"Inserted: {table_name}")

    # Close the cursor and connection
    cursor.close()
    cnx.close()
