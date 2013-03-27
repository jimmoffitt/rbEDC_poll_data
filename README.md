Introduction
============

This is a Ruby script written to retrieve data from a Enterprise Data Collector (EDC).  It illustrates the ability to poll a EDC instance with HTTP GET requests.  This script can navigate EDC data streams and retrieve what data has been collected as individual activity files.  (Soon it will also be able to write those activities to a database.)

EDC data streams can be configured in a script configuration file.  Otherwise, there is a script method that can "discover" what streams are hosted on your EDC.  If you do not configure streams explicitly in your configuration file, the "discovery" method will be automatically triggered.  If you do not specify data streams in the configuration file, this discovery process gets triggered every time the script is launched.  

Once the script is started, it enters an endless "while true" loop, retrieving data on a user-specified interval (with a default of 60 seconds).   

* Important note: this script is designed to process normalized Activity Streams (atom) data.  
 
{mention refreshURL/since_date mechanism}


Usage
=====

One file is passed in at the command-line if you are running this code as a script (and not building some wrapper
around the EDC_client class):

1) A configuration file with account/username/password details and processing options (see below, or the sample project
file, for details):  -c "./EDC_Config.yaml"

The EDC configuration file needs to have an "account" section and a "edc" section.  If you specify that
you are using database (ddc --> storage: database) you will need to have a "database" section as well.

So, if you were running from a directory with this source file in it, with the configuration file in that folder too,
the command-line would look like this:

        $ruby ./EDC_client.rb -c "./EDC_Config.yaml"


Configuration
=============

See the sample EDC_config.yaml file for an example of a EDC client configuration file.  

Here are some important points:
+ In the "account" section, you specify the "machine name" used in the URL for your EDC.  EDCs have the following URL pattern:
	https://machine_name.gnip.com

+ In the "edc" section, you can specify the following processing options:
	+ poll_interval: interval in seconds between EDC polls for new data.  Default is every 60 seconds.
	+ poll_max: The maximum number of activities to return from each request.
	+ storage: "files" or "database".  How do you plan on storing the data? In flat files or in a database.
		If you are storing as files, the filename is based on the native activity "id" and the extension indicates the 
		markup format (xml or json, although only xml is currently supported). 
	+ out_box: If storing data in files, where do you want them written to?

+ In the "streams" section you have the option to explicitly list the EDC streams you want to collect data from. For each stream 
	you need to specify its "ID" and provide a stream name:
	
	
	ID: the numeric ID assigned to the stream.  This ID can be referenced by navigating to the data stream with the EDC dashboard and noting the numeric ID in the URL, as in "https://myEDC.gnip.com/data_collectors/5.  Note that these stream IDs are not always consecutive, and there will be gaps in the ID sequence of you have deleted any streams during the life of your EDC. 
		
	Name: a label given to the stream to help you identify the stream in the configuration file.  This name is echoed in standard output as the script runs.


	streams:	
	  - ID 	  : 1
	    Name  : Facebook Keyword Search  
	  - ID    : 3
    	    Name  : Google Plus Keyword Search

