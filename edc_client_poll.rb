
'''
A simple script to retrieve data from a Enterprise Data Collector (EDC).

This script can "self-discover" the current set of active EDC streams, or they can be statically
configured in the EDC configuration file.

Currently, this script only handles normalized Activity Stream (ATOM) data formatted in XML.

TODO:
    [] Add in refreshURL logic/storage.
    [] Add support for original format (JSON).
    [] Keep password configuration?

'''
require "base64"    #Used for basic password encryption.
require "yaml"      #Used for configuration file management.
require "nokogiri"  #Used for parsing activity XML.
require "cgi"       #Used only for retrieving HTTP GET parameters and converting to a hash.
#require "nori"     #I tried to use this gem to transform XML to a Hash, but the IDE kept blowing up!


class EDC_Client

    attr_accessor :http, :machine_name, :user_name, :password_encoded, :url,
                  :streams, :storage, :out_box, :poll_interval, :poll_max

    STREAM_SEARCH_LIMIT = 20  #Used to limit how high we go searching for active streams.

    def initialize(config_file = nil)

        if not config_file.nil? then
            getCollectorConfig(config_file)
        end

        if not @machine_name.nil? then
            @url = "https://" + @machine_name + ".gnip.com/data_collectors"
        end

        #Set up a HTTP object.
        @http = PtREST.new  #Historical API is REST based (currently).
        @http.url = @url  #Pass the URL to the HTTP object.
        @http.user_name = @user_name  #Set the info needed for authentication.
        @http.password_encoded = @password_encoded  #HTTP class can decrypt password.

        @streams = Array.new #of stream (ID,name) definitions.
        if not config_file.nil? then
            getStreamConfig(config_file)
        end
    end

    def getPassword
        #You may want to implement a more secure password handler.  Or not.
        @password = Base64.decode64(@password_encoded)  #Decrypt password.
    end

    def getCollectorConfig(config_file)

        config = YAML.load_file(config_file)

        #Account details.
        @machine_name = config["account"]["machine_name"]
        @user_name  = config["account"]["user_name"]
        @password_encoded = config["account"]["password_encoded"]

        if @password_encoded.nil? then  #User is passing in plain-text password...
            @password = config["account"]["password"]
            @password_encoded = Base64.encode64(@password)
        end

        #EDC configuration details.
        @storage = config["edc"]["storage"]
        @out_box = config["edc"]["out_box"]
        @poll_interval = config["edc"]["poll_interval"]
        @poll_max = config["edc"]["poll_max"]
    end

    def getStreamConfig(config_file)

        config = YAML.load_file(config_file)

        #Load any configured streams.
        streams = config["streams"]

        if not streams.nil? then  #Load from configuration file.
            streams.each do |stream|
                #p stream
                @streams << stream
            end

            @streams.each do |stream|
                stream["refresh_url"] = ""
            end

        else #Nothing in configuration file?  Then go discover what is there.
            @streams = discoverDataCollectors
        end
    end


    '''
    Tours potential end-points and determines whether they are found or not.
    This method is called if there are no streams defined in the configuration file.
    If there are ANY streams defined, this method is not called.
    This method currently hits the "api_help" end-point to determine whether a stream exists or not.
    Uses the STREAM_SEARCH_LIMIT constant to limit how "high" it looks with stream IDs.
    For streams that are found, it populates a stream array with:
        ID: the numeric ID assigned to the stream.
        Name: based on the HTML "title", with punctuation characters dropped.
    '''
    def discoverDataCollectors

        print "Pinging end-points, looking for active streams..."

        1.upto(STREAM_SEARCH_LIMIT) do |i|

            print "."

            url = "https://" + @machine_name + ".gnip.com/data_collectors/<stream_num>/api_help"
            url_test = url
            url_test["<stream_num>"] = i.to_s
            #p url_test

            #Make HTTP Get request.
            @http.url = url_test
            response = @http.GET

            #Parse response either "Not found" or an active data stream page
            if response.body.downcase.include?("not found") then #move on to the next test URL.
                next
            end

            #If active, parse for <title> and load @streams

            doc = Nokogiri::HTML(response.body)
            #Look for title
            stream_title = doc.css("title")[0].text
            #Drop punctuation
            stream_title.gsub!(/\W+/," ")

            stream = Hash.new
            stream["ID"] = i
            stream["name"] = stream_title

            p "Found " + stream_title + " @ ID: " + i.to_s

            #Add to stream array.
            @streams << stream
        end

        @stream.each do |stream|
            stream["refresh_url"] = ""
        end

        @streams
    end

    '''
    Parses normalized Activity Stream XML.
    Parsing details here are driven by the current database schema used to store activities.
    If writing files, then just write out the entire activity payload to the file.
    '''
    def processResponseXML(docXML)

        #Grab Publisher and End Point from response
        publisher = docXML.xpath("//results").attr("publisher")
        end_point = docXML.xpath("//results").attr("endpoint")

        content = docXML.to_s #entire XML payload
        id = ""
        posted_time = ""
        body = ""
        tag = Array.new
        value = Array.new
        activities = 0

        docXML.root.children.each do |node|
            if node.name == 'entry' then
                activities += 1
                if (activities % 50) == 0 then puts "." else print "." end

            end

            #Storing as a file?  Then we are writing the entire activity payload with no need to parse out details.
            if @storage == "files" then #Write to the file.
                #Create file name
                filename = id + ".xml"
                File.open(@out_box + "/" + filename, "w") do |new_file|
                    new_file.write(content)
                end
            else #Storing in database, so do more parsing for payload elements that have been promoted to db fields.
                node.children.each do |sub_node|
                    id = sub_node.inner_text if sub_node.name == "id"
                    posted_time = sub_node.inner_text if sub_node.name == "created"


                    if sub_node.name == "object" then
                        sub_node.children.each do |content_node|
                            body = content_node.inner_text if content_node.name == "content"
                        end
                    end

                    if sub_node.name == "matching_rules" then
                        tag = Array.new
                        value = Array.new
                        sub_node.children.each do |rules_node|
                            #p rules_node.name

                            value << rules_node.inner_text if rules_node.name == "matching_rule"
                            tags = docXML.xpath("//*[tag]")
                            #p tags
                        end
                    end
                end
            end
        end

        p "Retrieved #{activities} activities..."
    end

    '''
    The driver of the process, complete with a "while true" loop!
    Loops for each stream, builds the Activities API URL.
    Applies the @poll_max attribute and manages the "since_date" as GET parameters.




    #max: The maximum number of activities to return capped at 10000 (default: 100).
    #since_date: Only return activities since the given date, in UTC, in the format "YYYYmmddHHMMSS".
    #to_date: return only activities before the given date, in UTC, in the format "YYYYmmddHHMMSS".
    '''
    def retrieveData

        while true do
            @streams.each do |stream|

                url = ""

                #Build URL for retrieving data.
                if stream["refresh_url"] == "" then  #We just started script, first request.
                    url = "https://#{@machine_name}.gnip.com/data_collectors/#{stream["ID"]}/activities.xml"
                else #Not the first request, so make use of the "refreshURL" information returned from the Activitites API.
                    url = stream["refresh_url"]
                end

                #Set up request parameters.
                params = Hash.new

                #Add since_date parameter if it is available.
                if url.include?("since_date") then #then we need to parse off the timestamp, and add explicitly to the params.
                    params["since_date"] = CGI.parse(URI.parse(url).query)["since_date"].first #and only!
                end

                #Add maximum amount of data configuration setting.
                if @poll_max > 0 then
                    params["max"] = @poll_max
                end

                @http.url = url

                response = @http.GET(params) #Ask for data!

                #Load the response into an XML document.
                docXML = Nokogiri::XML.parse(response.body)  {|config| config.noblanks}

                #Grab the "refreshURL" from the Activities API response, which is used in subsequent requests.
                begin
                    stream["refresh_url"] = (docXML.xpath("//results").attr("refreshURL")).to_s
                rescue
                   p "Error occurred parsing 'refreshURL'."
                end

                p "Processing data from #{stream["Name"]}..."
                processResponseXML(docXML)
            end

            #Sleep before asking for more fresh data.
            p "Sleeping for #{@poll_interval} seconds..."
            sleep(@poll_interval)
        end
    end
end

#=======================================================================================================================
#A simple RESTful HTTP class for interacting with the EDC end-point.
#Future versions will most likely use an external PtREST object, common to all PowerTrack ruby clients.
class PtREST
    require "net/https"     #HTTP gem.
    require "uri"

    attr_accessor :url, :uri, :user_name, :password_encoded, :headers, :data, :data_agent, :account_name, :publisher

    def initialize(url=nil, user_name=nil, password_encoded=nil, headers=nil)
        if not url.nil?
            @url = url
        end

        if not user_name.nil?
            @user_name = user_name
        end

        if not password_encoded.nil?
            @password_encoded = password_encoded
            @password = Base64.decode64(@password_encoded)
        end

        if not headers.nil?
            @headers = headers
        end
    end

    def url=(value)
        @url = value
        if not @url.nil?
            @uri = URI.parse(@url)
        end
    end

    def password_encoded=(value)
        @password_encoded=value
        if not @password_encoded.nil? then
            @password = Base64.decode64(@password_encoded)
        end
    end

    #Fundamental REST API methods:
    def POST(data=nil)

        if not data.nil? #if request data passed in, use it.
            @data = data
        end

        uri = URI(@url)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true
        request = Net::HTTP::Post.new(uri.path)
        request.body = @data
        request.basic_auth(@user_name, @password)
        response = http.request(request)
        return response
    end

    def PUT(data=nil)

        if not data.nil? #if request data passed in, use it.
            @data = data
        end

        uri = URI(@url)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true
        request = Net::HTTP::Put.new(uri.path)
        request.body = @data
        request.basic_auth(@user_name, @password)
        response = http.request(request)
        return response
    end

    def GET(params=nil)
        uri = URI(@url)

        #params are passed in as a hash.
        #Example: params["max"] = 100, params["since_date"] = 20130321000000
        if not params.nil?
            uri.query = URI.encode_www_form(params)
        end

        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true
        request = Net::HTTP::Get.new(uri.request_uri)
        request.basic_auth(@user_name, @password)

        response = http.request(request)
        return response
    end

    def DELETE(data=nil)
        if not data.nil?
            @data = data
        end

        uri = URI(@url)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true
        request = Net::HTTP::Delete.new(uri.path)
        request.body = @data
        request.basic_auth(@user_name, @password)
        response = http.request(request)
        return response
    end
end #PtREST class.



#=======================================================================================================================
#Database class.

'''
This class is meant to demonstrate basic code for building a "database" class for use with the
PowerTrack set of example code.  It is written in Ruby, but in its present form hopefully will
read like pseudo-code for other languages.

One option would be to use (Rails) ActiveRecord for data management, but it seems that may abstract away more than
desired.

Having said that, the database was created (and maintained/migrated) with Rails ActiveRecord.
It is just a great way to create databases.

ActiveRecord::Schema.define(:version => 20130306234839) do

  create_table "activities", :force => true do |t|
      t.integer  "native_id",   :limit => 8
      t.text     "content"
      t.text     "body"
      t.string   "rule_value"
      t.string   "rule_tag"
      t.string   "publisher"
      t.string   "job_uuid"
      t.datetime "created_at",               :null => false
      t.datetime "updated_at",               :null => false
      t.float    "latitude"
      t.float    "longitude"
      t.datetime "posted_time"
  end

end

The above table fields are a bit arbitrary.  I cherry picked some Tweet details and promoted them to be table fields.
Meanwhile the entire tweet is stored, in case other parsing is needed downstream.
'''
class PtDatabase
    require "mysql2"
    require "time"
    require "json"
    require "base64"

    attr_accessor :client, :host, :port, :user_name, :password, :database, :sql

    def initialize(host=nil, port=nil, database=nil, user_name=nil, password=nil)
        #local database for storing activity data...

        if host.nil? then
            @host = "127.0.0.1" #Local host is default.
        else
            @host = host
        end

        if port.nil? then
            @port = 3306 #MySQL post is default.
        else
            @port = port
        end

        if not user_name.nil?  #No default for this setting.
            @user_name = user_name
        end

        if not password.nil? #No default for this setting.
            @password = password
        end

        if not database.nil? #No default for this setting.
            @database = database
        end
    end

    #You can pass in a PowerTrack configuration file and load details from that.
    def config=(config_file)
        @config = config_file
        getSystemConfig(@config)
    end


    #Load in the configuration file details, setting many object attributes.
    def getSystemConfig(config)

        config = YAML.load_file(config_file)

        #Config details.
        @host = config["database"]["host"]
        @port = config["database"]["port"]

        @user_name = config["database"]["user_name"]
        @password_encoded = config["database"]["password_encoded"]

        if @password_encoded.nil? then  #User is passing in plain-text password...
            @password = config["database"]["password"]
            @password_encoded = Base64.encode64(@password)
        end

        @database = config["database"]["schema"]
    end


    def to_s
        "PowerTrack object => " + @host + ":" + @port.to_s + "@" + @user_name + " schema:" + @database
    end

    def connect
        #TODO: need support for password!
        @client = Mysql2::Client.new(:host => @host, :port => @port, :username => @user_name, :database => @database )
    end

    def disconnect
        @client.close
    end

    def SELECT(sql = nil)

        if sql.nil? then
            sql = @sql
        end

        result = @client.query(sql)

        result

    end

    def UPDATE(sql)
    end

    def REPLACE(sql)
        begin
            result = @client.query(sql)
            true
        rescue
            false
        end
    end

    #NativeID is defined as an integer.  This works for Twitter, but not for other publishers who use alphanumerics.
    #Tweet "id" field has this form: "tag:search.twitter.com,2005:198308769506136064"
    #This function parses out the numeric ID at end.
    def getNativeID(id)
        native_id = Integer(id.split(":")[-1])
    end

    #Twitter uses UTC.
    def getPostedTime(time_stamp)
        time_stamp = Time.parse(time_stamp).strftime("%Y-%m-%d %H:%M:%S")
    end

    #With Rehydration, there are no rules, just requested IDs.
    def getMatchingRules(matching_rules)
        return "rehydration", "rehydration"
    end

    '''
    Parse the activity payload and get the lat/long coordinates.
    ORDER MATTERS: Latitude, Longitude.

    #An example here we have POINT coordinates.
    "location":{
        "objectType":"place",
        "displayName":"Jefferson Southwest, KY",
        "name":"Jefferson Southwest",
        "country_code":"United States",
        "twitter_country_code":"US",
        "link":"http://api.twitter.com/1/geo/id/7a46e5213d3a1af2.json",
        "geo":{
            "type":"Polygon",
            "coordinates":[[[-85.951854,37.997244],[-85.700857,37.997244],[-85.700857,38.233633],[-85.951854,38.233633]]]}
    },
    "geo":{"type":"Point","coordinates":[38.1341,-85.8953]},
    '''

    def getGeoCoordinates(activity)

        geo = activity["geo"]
        latitude = 0
        longitude = 0

        if not geo.nil? then #We have a "root" geo entry, so go there to get Point location.
            if geo["type"] == "Point" then
                latitude = geo["coordinates"][0]
                longitude = geo["coordinates"][1]

                #We are done here, so return
                return latitude, longitude

            end
        end

        #p activity["location"]
        #p activity["location"]["geo"]
        #p activity["geo"]

        return latitude, longitude
    end

    #Replace some special characters with an _.
    #(Or, for Ruby, use ActiveRecord for all db interaction!)
    def handleSpecialCharacters(text)

        if text.include?("'") then
            text.gsub!("'","_")
        end
        if text.include?("\\") then
            text.gsub!("\\","_")
        end

        text
    end


    '''
    storeActivity
    Receives an Activity Stream data point formatted in JSON.
    Does some (hopefully) quick parsing of payload.
    Writes to an Activities table.

    t.integer  "native_id",   :limit => 8
    t.text     "content"
    t.text     "body"
    t.string   "rule_value"
    t.string   "rule_tag"
    t.string   "publisher"
    t.string   "job_uuid"  #Used for Historical PowerTrack.
    t.float    "latitude"
    t.float    "longitude"
    t.datetime "posted_time"
    '''

    def storeActivity(activity, uuid = nil)

        data = JSON.parse(activity)

        #Handle uuid if there is not one (tweet not returned by Historical API)
        if uuid == nil then
            uuid = ""
        end

        #Parse from the activity the "atomic" elements we are inserting into db fields.

        post_time = getPostedTime(data["postedTime"])

        native_id = getNativeID(data["id"])

        body = handleSpecialCharacters(data["body"])

        content = handleSpecialCharacters(activity)

        #Parse gnip:matching_rules and extract one or more rule values/tags
        rule_values, rule_tags  = "rehydration", "rehydration" #getMatchingRules(data["gnip"]["matching_rules"])

        #Parse the activity and extract any geo available data.
        latitude, longitude = getGeoCoordinates(data)

        #Build SQL.
        sql = "REPLACE INTO activities (native_id, posted_time, content, body, rule_value, rule_tag, publisher, job_uuid, latitude, longitude, created_at, updated_at ) " +
            "VALUES (#{native_id}, '#{post_time}', '#{content}', '#{body}', '#{rule_values}','#{rule_tags}','Twitter', '#{uuid}', #{latitude}, #{longitude}, UTC_TIMESTAMP(), UTC_TIMESTAMP());"

        if not REPLACE(sql) then
            p "Activity not written to database: " + activity.to_s
        end
    end
end #PtDB class.





#=======================================================================================================================
if __FILE__ == $0  #This script code is executed when running this file.

    OptionParser.new do |o|
        o.on('-c CONFIG') { |config| $config = config}
        o.parse!
    end

    if $config.nil? then
        $config = "./EDC_Config_private.yaml"  #Default
    end

    p "Creating EDC Client object with config file: " + $config
    edc = EDC_Client.new($config)

    #Need to set up a HTTP details.
    p edc.retrieveData
    p "Exiting"

end

