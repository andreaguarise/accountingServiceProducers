#!/usr/bin/ruby -w

require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'
require 'dbi'

class DatabaseRecord
  
end

class Table
  def initialize(name)
    @name = name
    @rows = 0 #number of rows
    @avg_row_length = 0 #set in bytes, avg bytes per row
    @data_length = 0 #set in bytes, data file dimensions
  end
  
  def to_s
    "name:#{@name}|rows:#{@rows}|avg_row_length:#{@avg_row_length}|data_length:#{@data_length}"
  end
  
  def rows=(rows)
    @rows = rows
  end
  
  def rows
    @rows
  end
  
  def avg_row_length=(a)
    @avg_row_length=a
  end
  
  def data_length=(d)
    @data_length=d
  end
    
end

class Database
  def initialize(dbContactString,user,password)
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
  
  def getTables
    @dbh.tables
  end
end

class MySQL < Database
  
  def getVersion
     #get server version string and display it
     row = @dbh.select_one("SELECT VERSION()")
     row[0]
  end
  
  def getTableInfo
     result = @dbh.select_all("SHOW TABLE STATUS IN #{@dbName}")
     result.each do |row|
       t = Table.new(row[0])
       t.rows=row[4]
       t.avg_row_length=row[5]
       t.data_length=row[6]
       puts t.to_s
     end
     #table = new Table(t)
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
    db.getTables.each do |table|
      puts table
    end
    db.getTableInfo
    db.disconnect
    puts db.to_s
  end
end