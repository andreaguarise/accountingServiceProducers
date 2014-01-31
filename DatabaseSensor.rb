#!/usr/bin/ruby -w

require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'
require 'dbi'

class GenericResource < ActiveResource::Base
  self.format = :xml
end

class DatabaseTable < GenericResource
end

class DatabaseRecord < GenericResource
end 

class LocalDatabaseRecord
  def initialize(time,table,schema)
    @time = time
    @table = table
    @schema = schema
  end
end

class DatabaseRecordText < LocalDatabaseRecord
  
  def post
    "name:#{@table.name}\nrows:#{@table.rows}\navg_row_length:#{@table.avg_row_length}\ndata_length:#{@table.data_length}\nindex_length:#{@table.index_length}"
  end
end

class DatabaseRecordJSON < LocalDatabaseRecord
  
  def post
    record = @table.to_hash
    record["time"]= @time
    record.to_json
  end
  
end

class DatabaseRecordXML < LocalDatabaseRecord
  
end

class DatabaseRecordActiveResource < LocalDatabaseRecord
  
  def to_hash
    rh = {}
    rh['schema'] = @schema
    rh['table'] = @table.name
    rh['indexsize'] = @table.index_length
    rh['rows'] = @table.rows
    rh['tablesize'] = @table.data_length
    rh['time'] = @time
    rh 
  end
  
  def post
    r = DatabaseRecord.new(self.to_hash)
    r.save
  end
  
end

class DatabaseResource < GenericResource
  
end

class Table
  def initialize(name)
    @name = name
    @rows = 0 #number of rows
    @avg_row_length = 0 #set in bytes, avg bytes per row
    @data_length = 0 #set in bytes, data file dimensions
    @index_length = 0 #set in bytes, index file dimensons
  end
  
  #def to_s
  #  "name:#{@name}|rows:#{@rows}|avg_row_length:#{@avg_row_length}|data_length:#{@data_length}|index_length:#{@index_length}"
  #end
  
  def to_hash
    Hash[instance_variables.map { |var| [var[1..-1].to_sym, instance_variable_get(var)] }]
  end
  
  def rows=(rows)
    @rows = rows
  end
  
  def rows; @rows; end
  def name; @name; end
  def avg_row_length; @avg_row_length; end
  def data_length; @data_length; end
  def index_length; @index_length; end
  
  def avg_row_length=(a)
    @avg_row_length=a
  end
  
  def data_length=(d)
    @data_length=d
  end
  
  def index_length=(i)
    @index_length=i
  end
    
end

class Database
  def initialize(dbContactString,user,password)
    @tables = Array.new
    @dbContactString = dbContactString
    pattern = /^(.*):(.*):(.*)$/
    pattern =~ dbContactString
    data = Regexp.last_match
    @dbName = data[2]
    @user = user
    @password = password
    @dbh = nil
  end
  
  def to_s
    "dbContactString: #{@dbContactString}\n"
  end
  
  def connect
    begin
      @dbh = DBI.connect("DBI:#{@dbContactString}", 
                      @user, @password)
         #  
    rescue DBI::DatabaseError => e
      puts "An error occurred"
      puts "Error code:    #{e.err}"
      puts "Error message: #{e.errstr}"
    end
  end
  
  def disconnect 
    @dbh.disconnect if @dbh
  end
  
  def dbName
    @dbName
  end
  
  def getTables
    @dbh.tables
  end
  
  def tables
    @tables
  end
  
end

class MySQL < Database
  
  def getVersion
     #get server version string and display it
     row = @dbh.select_one("SELECT VERSION()")
     row[0]
  end
  
  def getTableStatus
     result = @dbh.select_all("SHOW TABLE STATUS IN #{@dbName}")
     result.each do |row|
       t = Table.new(row[0])
       t.rows=row[4]
       t.avg_row_length=row[5]
       t.data_length=row[6]
       t.index_length=row[8]
       @tables << t
     end
     @tables
  end
  
  
end

class DatabaseSensor
  def initialize
    @options = {}
  end
  
  def getLineParameters
    
    opt_parser = OptionParser.new do |opt|
      opt.banner = "Usage: record_post [OPTIONS] field=value ..."

      @options[:verbose] = false
      opt.on( '-v', '--verbose', 'Output more information') do
        @options[:verbose] = true
      end
  
      @options[:dryrun] = false
        opt.on( '-d', '--dryrun', 'Do not talk to server') do
        @options[:dryrun] = true
      end
  
      @options[:user] = nil
      opt.on( '-u', '--user user', 'Database user') do |user|
        @options[:user] = user
      end
  
      @options[:password] = nil
      opt.on( '-p', '--password password', 'Database password') do |password|
        @options[:password] = password
      end
  
      @options[:uri] = nil
      opt.on( '-U', '--URI uri', 'URI to contact') do |uri|
        @options[:uri] = uri
      end
      
      @options[:uri] = nil
      opt.on( '-P', '--Publisher type', 'Publisher type') do |type|
        @options[:publisher_type] = type  
      end
      
      @options[:db] = nil
      opt.on( '-D', '--Database database', 'Database to contact') do |db|
        @options[:db] = db
      end

      @options[:token] = nil
      opt.on( '-t', '--token token', 'Authorization token that must be obtained from the service administrator') do |token|
       @options[:token] = token
      end

      opt.on( '-h', '--help', 'Print this screen') do
        puts opt
        exit
      end
   
    end

    opt_parser.parse!
  end
  
  def getDbType
    "MySQL"
  end
  
  def newPublisher(time,table,dbName)
    r = case
    when @options[:publisher_type] == "JSON" then
      DatabaseRecordJSON.new(time.to_i,table,dbName)
    when @options[:publisher_type] == "text" then
      DatabaseRecordText.new(time.to_i,table,dbName)
    when @options[:publisher_type] == "ActiveResource" then
      DatabaseRecord.site = @options[:uri]
      DatabaseRecord.headers['Authorization'] = "Token token=\"#{@options[:token]}\""
      DatabaseRecord.timeout = 5
      DatabaseRecord.proxy = ""
      DatabaseRecordActiveResource.new(time.to_i,table,dbName)
    else
        nil
    end
  end
  
  def main
    self.getLineParameters
    db = case
      when  self.getDbType == "MySQL" then 
         MySQL.new(@options[:db],@options[:user],@options[:password])
      when  self.getDbType == "sqlite" then 
        nil
      else 
        nil
    end     
    db.connect
    puts db.getVersion
    puts db.dbName
    tables = db.getTableStatus
    currentTime = Time.now
    tables.each do |table|
      r = newPublisher(currentTime,table,db.dbName)
      puts r.post
    end
    db.disconnect
  end
end