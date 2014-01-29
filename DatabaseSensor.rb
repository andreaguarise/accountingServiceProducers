#!/usr/bin/ruby -w

require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'
require 'dbi'

class DatabaseRecord
  
end

class Database
  def initialize(dbContactString)
    @dbContactString = dbContactString
  end
  
  def to_s
    "dbContactString: #{@dbContactString}\n"
  end
  
  def connect
    begin
      dbh = DBI.connect("DBI:#{@dbContactString}", 
                      "root", "z1b1bb0")
         # get server version string and display it
      row = dbh.select_one("SELECT VERSION()")
      puts "Server version: " + row[0]
    rescue DBI::DatabaseError => e
      puts "An error occurred"
      puts "Error code:    #{e.err}"
      puts "Error message: #{e.errstr}"
    ensure
      # disconnect from server
      dbh.disconnect if dbh
    end
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
  
      @options[:publishers] = false
      opt.on( '-p', '--publishers', 'Output retrieved publishers') do
        @options[:publishers] = true
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
  
  def main
    puts "Hello my dear!"
    self.getLineParameters
    db = Database.new(@options[:db])
    db.connect
    puts db.to_s
  end
end