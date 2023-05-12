# YouTube Data Scrapping and Dashboard

This Project contains the end-to-end pipeline to extract information on a list of YouTube Channels.

To comply with YouTube' API terms the information shared here has been confined to the framework, any data has been ommited to avoid any comparison or gaming between creators.

A description of each file in the Directory:

* _extraction_script.py_: contains all the routines for extraction. It uses as input a json file with all the channels you intend to explore.
* _SQL_Script_DBCreation_: has all the details for the database creation.
* _reads_extractions_uploadsDB.ipynb_: contains all the processes to mass upload daily extractions into the database.
* _DashboardQuery.ipynb_: Creates the modeled sources for the Dashboards to use.
* _AnonDashAPP.py_: script to create the dash app.
* _Dashboards Preview_: This folder contails a preview on the dashboards, later I will share a video with the capabilities of these solutions and the link will be made available on this document and in my [twitter](https://twitter.com/maikimaiki23).

For more details on how this repository came to be, and some interesting insights, I've written articles that you can find in my [substack](https://amathematicianthinks.substack.com/).