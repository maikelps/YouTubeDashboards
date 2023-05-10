import os
from scripts.email_script import send_email_with_data

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dateutil import parser
import json

#import google_auth_oauthlib.flow
#import googleapiclient.discovery
import googleapiclient.errors
from googleapiclient.discovery import build

import json

import mysql.connector

import re

# Reading All Credentials
cred = json.load(open('DBCredentials.json', 'r'))

""" Functions for channel level """
def get_info_to_DF( chan_list_info ):
    # chan_list_info: List of channels
    names, views, subs, vids, creation, uploads_id, chanID = [], [], [], [], [], [], []

    for chan in chan_list_info:
        # Channel Name
        name = chan['snippet']['title']
        print(f"Extracting info for YouTube Channel ----> {name}")
        names.append( name )

        # Getting creation date
        creation.append(chan['snippet']['publishedAt'])

        # Channel Stats
        stats = chan['statistics']
        # Appending stats for DF
        views.append( stats['viewCount'] )
        subs.append( stats['subscriberCount'] )
        vids.append( stats['videoCount'] )
        
        # Getting upload list id to then extract video info
        uploads_id.append( chan['contentDetails']['relatedPlaylists']['uploads'] )

        # Channel ID for future reference:
        chanID.append( chan['id'] )

    frame = pd.DataFrame({
                            'CreatedOn': creation,
                            'views': views,
                            'subscribers': subs,
                            'videoNr': vids,
                            'UploadPlaylist': uploads_id,
                            'ChannelID': chanID
                        }, index=names
                        )

    # Cast to numeric certain features
    for num_col in ['views', 'subscribers', 'videoNr']:
        frame[num_col] = pd.to_numeric(frame[num_col])
    
    # Fix Dates:
    frame['CreatedOn'] = pd.to_datetime(pd.to_datetime(frame['CreatedOn']).dt.strftime('%Y-%m-%d'))
    #frame['CreatedOn'] = pd.to_datetime(frame['CreatedOn'], format='%Y-%m-%d')
    
    # Add tag for ExtractionDay
    frame.insert(0, 'ExtractionDay', np.datetime64('today'))

    # Index Name:
    frame.index.names = ['ChannelName']
    
    return frame

def slice_for_DB( frame ):

    fr = frame.copy()
    fr.reset_index(inplace=True)

    out = {
        'channel_details' : fr[['ChannelName', 'CreatedOn', 'UploadPlaylist', 'ChannelID']].set_index('ChannelName'),
        'channel_evolution' : fr[['ChannelID', 'ExtractionDay', 'views', 'subscribers', 'videoNr']].set_index('ChannelID')
    }

    return out

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

""" Functions for Video Level """

def get_videos_ids( **kargs ):

    # Get all the videos / get the details of each video

    # Create a YouTube API service object
    youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=API_KEY)

    # Set the maximum number of results per page
    #kargs['MAX_RESULTS_PER_PAGE'] = 50

    # Initialize the list of video IDs and the page token
    video_ids = []
    full_dict = []
    page_token = None

    # Keep calling the API until all results have been retrieved
    while True:

        # Call the playlistItems.list method to retrieve the list of videos in the playlist
        request = youtube.playlistItems().list(
            part='snippet',
            playlistId = kargs['PLAYLIST_ID'],
            maxResults = kargs['MAX_RESULTS_PER_PAGE'],
            pageToken = page_token
        )
        response = request.execute()

        # Keep Dictionary with results
        full_dict.append( response )

        # Loop through the list of videos and add the video ID to the list
        for item in response['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])

        # Update the page token
        page_token = response.get('nextPageToken')

        # If there are no more results, break the loop
        if not page_token:
            break

    return {'VidIDs' : video_ids, 'API_Results' : full_dict}

def get_video_details( **kargs ):

    # Build the YouTube API service
    service = build('youtube', 'v3', developerKey=API_KEY)

    # Set the channel URL
    channel_url = f"https://www.youtube.com/channel/{kargs['CHANNEL_ID']}"

    # Call the API to get Info on the video
    request = service.videos().list(
        part="snippet,statistics,contentDetails",
        id=kargs['VIDEO_ID']
    )

    response = request.execute()

    return response

def video_details_for_db( video_details_dictionary, log=False):
    """
    Gets the video details in a tabular format to be uploaded into the DB
    """

    # Iterating on each video and sumarizing the required info in tabular format
    videos_info_sum = {'CreatedOn': [], 'viewCount' : [], 'likeCount' : [], 'commentCount' : [], 'duration' : [], 'title' : [], 'descr': [], 'video_id' : [] }

    for video_id in video_details_dictionary.keys():

        if log:
            print(video_id)

        # Dictionary basic level, contains all the necessary info for easy code explainability below.
        video_info = video_details_dictionary[video_id]['items'][0]

        # Publishing Date
        vid_creation = video_info['snippet']['publishedAt']

        # Stats
        vid_stats = video_info['statistics']

        # Duration
        vid_duration = video_info['contentDetails']['duration']

        # Video Title and Descr
        vid_title = video_info['snippet']['title']
        vid_descr = video_info['snippet']['description']        

        # --- Appending all the info ---

        # Creation
        videos_info_sum['CreatedOn'].append( vid_creation )

        # Stats
        videos_info_sum['viewCount'].append( vid_stats['viewCount'] )

        try:
            videos_info_sum['likeCount'].append( vid_stats['likeCount'] )
        except:
            videos_info_sum['likeCount'].append( 0 )

        try:
            videos_info_sum['commentCount'].append( vid_stats['commentCount'] )
        except:
            videos_info_sum['commentCount'].append( 0 )
        

        # Duration
        vid_duration = vid_duration[2:] # Removing "PT" from the string to leave into the format ISO 8601
        try:
            vid_duration = parser.parse(vid_duration) # Parsing ISO 8601 to Datetime
            vid_duration = (vid_duration.hour * 3600) + (vid_duration.minute * 60) + (vid_duration.second) # Leaving duration in seconds
        except:
            vid_duration = 0
        videos_info_sum['duration'].append( vid_duration )

        # Title
        videos_info_sum['title'].append( vid_title )
        # Descr
        videos_info_sum['descr'].append( vid_descr )

        # ID
        videos_info_sum['video_id'].append( video_id )

    # Creating full DF with info
    df_vid = pd.DataFrame(videos_info_sum)

    # Adding Extra Features
    df_vid.insert( 0, 'ChannelID', video_info['snippet']['channelId'] )
    df_vid.insert( 0, 'ExtractionDay', str(np.datetime64('today')) )

    df_vid['CreatedOn'] = pd.to_datetime(pd.to_datetime(df_vid['CreatedOn']).dt.strftime('%Y-%m-%d'))

    # Creating output tabular configuration:

    tables = {
        'video_statics' : df_vid[['ChannelID', 'video_id', 'CreatedOn', 'title', 'descr', 'duration']],
        'video_variables' : df_vid[['video_id', 'ExtractionDay', 'viewCount', 'likeCount', 'commentCount']],
        'all' : df_vid.copy()
    }

    return tables

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
            tables['video_statics'] = clean_video_statics(tables['video_statics'])
            
            # Removing any emojis from the text
            tables[table_name]['title'] = tables[table_name]['title'].apply(remove_emojis).apply(remove_links_and_line_jumps).apply(remove_accents)
            tables[table_name]['descr'] = tables[table_name]['descr'].apply(remove_emojis).apply(remove_links_and_line_jumps).apply(remove_accents)

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

def pandas_to_excel_sheets(df_list, sheet_list, file_name):
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe.to_excel(writer, sheet_name=sheet, startrow=0 , startcol=0)   
    writer.save()

# --> CHANNEL LEVEL

# Dictionary with the Youtube Channel ID that is intended to extract
with open("scripts/ExtractionKeysWatched.json") as f:
    YtChans = json.load(f)

# Uniting the ID's with commas (API handles it like this)
ids_united = ''
for channel in YtChans.keys():
    print(f"united for: {channel}")
    ids_united = ids_united  + ',' + YtChans[channel]

# Removing comma added on the first character
ids_united = ids_united[1::]

# Set the API key 
API_KEY = cred['APIKEY']

# Build the YouTube API service
service = build('youtube', 'v3', developerKey=API_KEY)

# Call the API to get the channel details
response = service.channels().list(
    part="snippet,contentDetails,statistics",
    id=ids_united
).execute()

# Getting in DF format
df = get_info_to_DF( response['items'] )

# Sclicing
dict_insert = slice_for_DB( df )

print( 'CHANNEL LEVEL WAS A SUCCESS' )

#insert_channel_in_DB( dict_insert, chop_details = True )


# --> VIDEOS LEVEL

list_of_channels = df.index.tolist()
#list_of_channels = list_of_channels[::-1]

print( '---> STARTING VIDEO EXTRACTION FOR:' , list_of_channels)

# Tables for excel sheet
extr = {
        'video_statics'     : pd.DataFrame(columns=['ChannelID', 'video_id', 'CreatedOn', 'title', 'descr', 'duration']),
        'video_variables'   : pd.DataFrame(columns=['video_id', 'ExtractionDay', 'viewCount', 'likeCount', 'commentCount'])
    }
        

for channel_name in list_of_channels:
    print(f"Extracting for: {channel_name}")

    # Get necessary details 
    main_info = {
                    'PlayID' : df.at[ channel_name, 'UploadPlaylist' ],
                    'ChannelID' : df.at[ channel_name, 'ChannelID' ],
                    'VidNR' : df.at[ channel_name, 'videoNr' ],
                }

    # GET VIDEOS basic info
    video_basics = get_videos_ids( PLAYLIST_ID = main_info['PlayID'], MAX_RESULTS_PER_PAGE = 50 )
    print("---> Video Basics")

    # Getting all the details on the videos
    videos_info = { v_id : get_video_details( CHANNEL_ID = main_info['ChannelID'], VIDEO_ID = v_id ) for v_id in video_basics['VidIDs']}

    print("---> Videos Info")

    # Slicing 
    video_tables = video_details_for_db( videos_info )
    print('Video Tables sliced')

    # Appending all into a single DF
    for table_name in extr.keys():
        extr[ table_name ] = extr[ table_name ].append( video_tables[ table_name ] )

    #video_tables_db_upload( video_tables, chop_details = False )

# Creating Excel file:
file_name = 'Extraction_' + str( np.datetime64('today') ) + '.xlsx'
FRAMES = [ dict_insert['channel_details'], dict_insert['channel_evolution'], extr['video_statics'], extr['video_variables'] ]

pandas_to_excel_sheets(FRAMES, ['channel_details', 'channel_evolution', 'video_statics', 'video_variables'], file_name)

# Creating email parameters | Watch out for directory!!
subject = 'Extraction at ' + str(np.datetime64('now')).replace('T', ' ')
send_email_with_data(subject, file_name)
