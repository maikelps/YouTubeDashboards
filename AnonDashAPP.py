import pandas as pd
import numpy as np

from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px

from sklearn.feature_extraction.text import CountVectorizer
from wordcloud import WordCloud
from nltk.corpus import stopwords

#import nltk
#nltk.download('stopwords')

# Load stop words for Spanish
stop_words = set(stopwords.words('spanish'))

# Channel Colors
ch_color_dict = {
    'CH_ID_0' : '#819972',
    'CH_ID_1' : '#29A4BC',
    'CH_ID_2' : '#FFA500',
    'CH_ID_3' : '#FFC0CB',
    'CH_ID_4' : '#c9af80',
    'CH_ID_5' : '#d8d800',
    'CH_ID_6' : '#026440',
    'CH_ID_7' : '#40E0D0',
}

# Selected tab color
tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#02270771',
    'color': 'white',
    'padding': '6px',
    'font-size': '25px',
    'font-weight': '60'
}

# To add as column names
emoji_names = {
    "commentCount": "💬",
    "viewCount": "👀",
    "views":"👀", 
    "subscribers": "📈",
    "videoNr":"🎥",
    "likeCount": "👍",
    "ChannelName": "📺",
    'VideoAge': 'Days⌛'
    }

def read_channel_info():
    """
    Reads Channel Level Information and prepares data types
    """
    # Reading
    ch_id = pd.read_csv('Data/ChannelIDs.csv') # Channel Info
    df = pd.read_csv('Data/ChannelLevel_OverTime.csv') # Channel Level Evolution

    return df\
            .merge( ch_id, left_on='ChannelID', right_on='ChannelID', how='left' )\
            .assign( Age = ( df['ExtractionDay'].astype(np.datetime64) - df['CreatedOn'].astype(np.datetime64) )/np.timedelta64(1, 'D') )\
            .astype({'ExtractionDay': np.datetime64, 'CreatedOn': np.datetime64, 'ChannelName':'category', 'Attribute':'category', 'AnalysisType':'category', 'Age':'int64'})\
            .drop( columns=['ChannelID'] ) \
            .rename(columns=emoji_names)

def preprocess_video_data():
    """
    Preprocesses video level information for DashApp
    """
    # Load raw data and filter by AnalysisType == 'Actuals'
    video_level_data = pd.read_csv('Data/VideoLevel_OverTime.csv') \
                          .query("AnalysisType == 'Actuals'") \
                          .drop(columns='AnalysisType')

    # Merge data and select relevant columns
    merged_data = video_level_data.merge(pd.read_csv('Data/ChannelIDs.csv'), on='ChannelID') \
                                   .merge(pd.read_csv('Data/VideoIDs.csv', sep='\t'), on='video_id')

    # Pivot table and column name transformations
    pivoted_data = pd.pivot_table(merged_data, index=['ExtractionDay', 'ChannelName', 'title', 'descr', 'CreatedOn'], columns=['Attribute'], values=['Values']) \
                     .droplevel(0, axis=1) \
                     .reset_index() \
                     .rename(columns=emoji_names)

    return pivoted_data

def word_cloud_preprocess(fr, series_name):
    # Selecting Title
    text_series = fr[series_name].dropna() \
                                .drop_duplicates() \
                                .str.lower() \
                                .str.replace('link','URL')

    # Extract the text data from the DataFrame and remove stop words
    text_data = text_series.apply(lambda x: ' '.join([word for word in x.split() if word.lower() not in stop_words])).values.astype('U')

    # Build a bag-of-words representation of the text data
    vectorizer = CountVectorizer()
    word_counts = vectorizer.fit_transform(text_data)

    return text_data

# Reading
dfcl = read_channel_info()
dfvd = preprocess_video_data()

#Channel's Options
ALL_ch_names = dfcl[emoji_names['ChannelName']].unique().to_list()

#SLATE | SUPERHERO
ann_app = Dash(external_stylesheets=[dbc.themes.SLATE, "assets/stylesheet.css"], suppress_callback_exceptions=True)

# Dropdowns
analyze_dropdown = dcc.Dropdown(options=['Actuals', 'Evolution'], value='Actuals', clearable=False, className='button')
metric_dropdown = dcc.Dropdown(options=['subscribers', 'videoNr', 'views'], value='subscribers', clearable=False, className='button')

channels_dropdown = dcc.Dropdown(
                                options = ALL_ch_names + ['All'], 
                                value = ['All'], 
                                multi=True, 
                                clearable=False,
                                className='button',
                                style={'display': 'block', 'overflow-y': 'visible'},
                                id='channels-dropdown')

ALL_dates = dfvd['ExtractionDay'].sort_values(ascending=False).unique()
dates_dropdown = dcc.Dropdown(options = ALL_dates, value=ALL_dates[0], className='button')

# CHANNEL DROPDOWN MANAGEMENT
@ann_app.callback(
    Output('channels-dropdown', 'value'),
    Input('channels-dropdown', 'value')
)
def reset_channel_dropdown_value(selected_channels):
    if 'All' in selected_channels and len(selected_channels) > 1:
        return ['All']
    else:
        return selected_channels

chlvl_tab =  html.Div([
        dbc.Card(
                dbc.CardBody([
                        dbc.Row([
                                dbc.Col([ html.H5(children='Analyze by'), analyze_dropdown ], width=3),
                                dbc.Col([ html.H5(children='Metric selection'), metric_dropdown ], width=3),
                                dbc.Col([ html.H5(children='Channel Selection'), channels_dropdown ], width=6),
                        ], align='center'), 
                        html.Br(),
                        dbc.Row([
                                dbc.Col([ 
                                        dcc.Loading( dcc.Graph( id='chlvl_line' ) )
                                ], width=6),
                                dbc.Col([
                                        dcc.Loading( dcc.Graph( id='chlvl_buble' ) )
                                ], width=6),
                        ], align='center'), 
                        html.Br(),      
                ]), 
        color = 'dark')
])

vdlvl_tab =  html.Div([
        dbc.Card(
                dbc.CardBody([
                        dbc.Row([
                                dbc.Col([ html.H5(children='Select Day'), dates_dropdown ], width=3),
                                dbc.Col([ html.H5(children='Channel Selection'), channels_dropdown ], width=3),
                        ], align='center'), 
                        html.Br(),
                        dbc.Row([
                                dbc.Col([ 
                                        dcc.Loading( dcc.Graph( id='vdlvl_bubble' ) )
                                ], width=10),
                        ], align='center'),
                        html.Br(),
                        dbc.Row([
                                dbc.Col([ html.H5('Most common words in video titles') ], width=6),
                                dbc.Col([ html.H5('Most common words in video descriptions') ], width=6)
                        ], align='center'),
                        dbc.Row([
                                dbc.Col([ 
                                    dcc.Loading(
                                        html.Img(id='title_wc')
                                    )
                                ], width=6),
                                dbc.Col([
                                    dcc.Loading(
                                        html.Img(id='descr_wc')
                                    )
                                ], width=6),
                        ], align='center')
                ]), 
        color = 'dark')
])

ann_app.layout = html.Div([
    dbc.Row([
        dbc.Col([ html.H1(children='Some Venecan Content', className='banner') ], width=11),
        dbc.Col([ dbc.CardImg(src="assets/boleta.jpeg", top=True) ], width=1, style={"width": "75px"}),  
    ], align='center'),
    dcc.Tabs(id="tabs", value='Channel Level',
             parent_className='custom-tabs',
             className='custom-tabs-container', 
             children=[
                        dcc.Tab(label='Channel Level', value='Channel Level', className='custom-tab', selected_style=tab_selected_style),
                        dcc.Tab(label='Video Level', value='Video Level', className='custom-tab', selected_style=tab_selected_style),
                    ]),
    html.Div(id='tabs-content')
])

@ann_app.callback(Output('tabs-content', 'children'),
              Input('tabs', 'value'))
def render_content(tab):
    if tab == 'Channel Level':
        return chlvl_tab
    elif tab == 'Video Level':
        return vdlvl_tab

# CHANNEL LINE
@ann_app.callback(
        Output(component_id = 'chlvl_line', component_property='figure'),
        Input(component_id = analyze_dropdown, component_property='value'),
        Input(component_id = metric_dropdown , component_property='value'),
        Input(component_id = channels_dropdown , component_property='value')
)
def update_chlvl_line_graph(analyze, metric, channels_selected):

    # Check if "All" is selected
    if 'All' in channels_selected:
        channels_selected = ALL_ch_names.copy()

    # Filtering
    fr = dfcl[(dfcl['Attribute'] == metric) & (dfcl['AnalysisType'] == analyze) & (dfcl[emoji_names['ChannelName']].isin(channels_selected))].copy()
    fr[emoji_names['ChannelName']] = fr[emoji_names['ChannelName']].astype('string') # To avoid plotting key error due the categorical dtype

    # Channel level Line Plot
    chlvl_line_fig = px.line(fr, 
                            x='ExtractionDay', 
                            y='Values', 
                            color=emoji_names['ChannelName'], 
                            color_discrete_map=ch_color_dict,
                            template='plotly_dark',
                            height=600,
                            title=f"YouTube Channels compared by {metric} | Analysis type: {analyze} ",
                            markers=False,
                            )
    
    chlvl_line_fig.update_xaxes(title='')
    chlvl_line_fig.update_yaxes(title=f"{metric} {emoji_names[metric]}")
    
    #Makes the plot transparent
    chlvl_line_fig.update_layout({
        'plot_bgcolor' : 'rgba(0, 0, 0, 0)',
        'paper_bgcolor': 'rgba(0, 0, 0, 0)',
        'hovermode':'x',
        })
    #fig.update_layout()
    
    return chlvl_line_fig

# CHANNEL BUBBLE
@ann_app.callback(
        Output(component_id = 'chlvl_buble', component_property='figure'),
        #Input(component_id = dates_dropdown  , component_property='value'),
        Input(component_id = channels_dropdown , component_property='value')
)
def update_chlvl_bubble(channels_selected):

    # Check if "All" is selected
    if 'All' in channels_selected:
        channels_selected = ALL_ch_names.copy()

    # Filter and pivot
    fr = dfcl[(dfcl['AnalysisType'] == 'Actuals') & (dfcl[ emoji_names['ChannelName'] ].isin(channels_selected))]
    fr = pd.pivot_table(fr, index=['ExtractionDay', emoji_names['ChannelName'] ], columns=['Attribute'], values=['Values'])\
        .droplevel(0, axis=1)\
        .reset_index()\
        .rename(columns={'ExtractionDay' : 'Status at'})\
        .rename(columns=emoji_names)
    
    fr['Status at'] = fr['Status at'].astype(str)

    chlvl_buble_fig = px.scatter(fr, 
            x=emoji_names['views'], 
            y=emoji_names['subscribers'], 
            size=emoji_names['videoNr'], 
            animation_frame='Status at',
            color=emoji_names['ChannelName'], 
            template='plotly_dark', 
            size_max=40,
            height=600,
            title="Subscribers, Views and #Videos per channel", 
            color_discrete_map=ch_color_dict)

    chlvl_buble_fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 100

    # Preventing values out of the grid, getting parameters
    fixed_offset = 50000
    x_axis_max = fr[ emoji_names['views'] ].max()
    y_axis_max = fr[ emoji_names['subscribers'] ].max()

    # Setting grid parameters
    chlvl_buble_fig.update_xaxes(range=[0, x_axis_max+(fixed_offset*100)]) # Millions scale
    chlvl_buble_fig.update_yaxes(range=[0, y_axis_max+fixed_offset])

    #Makes the plot transparent
    chlvl_buble_fig.update_layout({
        'plot_bgcolor' : 'rgba(0, 0, 0, 0)',
        'paper_bgcolor': 'rgba(0, 0, 0, 0)',
    })

    return chlvl_buble_fig

# VIDEO BUBBLE
@ann_app.callback(
        Output(component_id = 'vdlvl_bubble', component_property='figure'),
        Input(component_id = dates_dropdown  , component_property='value'),
        Input(component_id = channels_dropdown , component_property='value')
)
def video_bubble(day_selected, channels_selected):

    # Check if "All" is selected
    if 'All' in channels_selected:
        channels_selected = ALL_ch_names.copy()

    # Filtering
    fr = dfvd.copy()
    fr = fr[ (fr['ExtractionDay'] == day_selected) & (fr[emoji_names['ChannelName']].isin(channels_selected)) ]
    #fr = fr[ (fr['ChannelName'].isin(channels_selected)) ]

    # Getting the nr of videos
    nr_vids = fr.title.nunique()

    video_buble_fig = px.scatter(fr, 
                                x=emoji_names['commentCount'], 
                                y=emoji_names['viewCount'], 
                                size=emoji_names['likeCount'],
                                color=emoji_names['VideoAge'],
                                hover_name='title',
                                hover_data=[ emoji_names['ChannelName'], 'CreatedOn' ],# list!!
                                template='plotly_dark', 
                                size_max=40,
                                height=600,
                                title=f"Views, Comments, and Likes per video | #Videos: {nr_vids}",
                                color_continuous_scale='RdYlGn_r')#Spectral_r
    
    video_buble_fig.update_xaxes(title='#Comments')
    video_buble_fig.update_yaxes(title='#Views')

    return video_buble_fig

# VIDEO WORDCLOUD
@ann_app.callback(
    Output('title_wc', 'src'),
    Output('descr_wc', 'src'),
    Input(component_id = dates_dropdown, component_property='value'),
    Input(component_id = channels_dropdown, component_property='value')
)
def wordcloud_gen(day_selected, channels_selected):

    # Check if "All" is selected
    if 'All' in channels_selected:
        channels_selected = ALL_ch_names.copy()

    # Filtering
    fr = dfvd.copy()
    fr = fr[ (fr['ExtractionDay'] == day_selected) & (fr[emoji_names['ChannelName']].isin(channels_selected)) ]
    
    # Creating data for wordcloud creation
    title_data = word_cloud_preprocess(fr, series_name='title')
    descr_data = word_cloud_preprocess(fr, series_name='descr')
    
    # Build a word cloud for title
    title_wc = WordCloud(background_color="black",
                         max_words=200,
                         contour_width=5,
                         contour_color='white',
                         collocations=False,
                         width=800,
                         height=400)
    title_wc.generate(' '.join(title_data))
    title_wc_image = title_wc.to_image()

    # Build a word cloud for description
    descr_wc = WordCloud(background_color="black",
                         max_words=200,
                         contour_width=5,
                         contour_color='white',
                         collocations=False,
                         width=800,
                         height=400)
    descr_wc.generate(' '.join(descr_data))
    descr_wc_image = descr_wc.to_image()

    return title_wc_image, descr_wc_image

# Run local server
if __name__ == '__main__':
    ann_app.run_server(debug=True)