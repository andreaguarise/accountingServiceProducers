#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'
require 'sqlite3'

options = {}

class GenericResource < ActiveResource::Base
  self.format = :xml
end


class Record
  def initialize(row)
      @row = row
  end
   
  def to_hash
    rh = {}
    
  end 
end

opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: record_post [OPTIONS] field=value ..."

  options[:verbose] = false
  opt.on( '-v', '--verbose', 'Output more information') do
    options[:verbose] = true
  end

  options[:uri] = nil
  opt.on( '-U', '--URI uri', 'URI to contact') do |uri|
    options[:uri] = uri
  end

  options[:token] = nil
  opt.on( '-t', '--token token', 'Authorization token that must be obtained from the service administrator') do |token|
    options[:token] = token
  end

  options[:database] = nil
  opt.on( '-d', '--database file', 'file containing the sqlite record db') do |token|
    options[:database] = token
  end

  options[:number] = nil
  opt.on( '-n', '--number num', 'number of record treated per each iteration') do |token|
    options[:number] = token
  end

  opt.on( '-h', '--help', 'Print this screen') do
    puts opt
    exit
  end
end

opt_parser.parse!

#open the database connection
db = SQLite3::Database.new options[:database]
db.results_as_hash = true;

startstop = db.get_first_row("SELECT min(key) as start, max(key) as stop FROM records")
start = startstop['start'].to_i
stop = startstop['stop'].to_i

while ( start < stop )
  rs = db.execute( "SELECT * FROM records WHERE key >= #{start} AND key < #{stop} ORDER by key LIMIT #{options[:number]}" )
  rs.each do |row|
    record = JSON.parse(row['record'])
    p "KEY: #{row["key"]},#{record["lrmsId"]}"
  end
  start = start.to_i + options[:number].to_i;


end
