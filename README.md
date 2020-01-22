# multiranking

The folder pyvotecore is a fork of the GPLv3 project https://github.com/bradbeattie/python-vote-core; read LICENSE.txt for more informations

Creates a joined ranking using the Schulze PR methode
Package ncludes a simple Crawler for gathering data from online resources; 

The test example file src/crawler_main.py is an example for joining ranks that is being used by the CVPR Robust Vision Challenge 2018 http://www.robustvision.net/
Use this example file to understand the common functionality of the web-crawler and the multi-ranking joining system. Please do not execute the example file on its own, as this creates a lot of unnecessary network traffic for the participating servers.

Dependencies (can be installed via pip):
python-graph-core >= 1.8.0
beautifulsoup4 >= 4.4.0
requests >= 2.18.0