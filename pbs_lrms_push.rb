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

class TorqueExecuteRecord < GenericResource
end

class Record
  def initialize(row)
      @row = row
  end

  def to_hash
    rh = {}
    rh['recordDate'] = @row['recordDate']
    rh['user'] = @row['user']
    #rh[''] = @row['server']
    rh['lrmsId'] = @row['lrmsId']
    rh['queue'] = @row['queue']
    rh['resourceUsed_cput'] = @row['cput']
    rh['resourceUsed_walltime'] = @row['walltime']
    rh['resourceUsed_vmem'] = @row['vmem']
    rh['resourceUsed_mem'] = @row['mem']
    #rh[''] = @row['processors']
    rh['group'] = @row['group']
    rh['jobName'] = @row['jobName']
    rh['ctime'] = @row['ctime']
    rh['qtime'] = @row['qtime']
    rh['etime'] = @row['etime']
    rh['start'] = @row['start']
    rh['end'] = @row['end']
    rh['execHost'] = @row['execHost']
    rh['exitStatus'] = @row['exitStatus']
    rh
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

TorqueExecuteRecord.site = options[:uri]
TorqueExecuteRecord.headers['Authorization'] = "Token token=\"#{options[:token]}\""
TorqueExecuteRecord.timeout = 5
TorqueExecuteRecord.proxy = ""

#open the database connection
db = SQLite3::Database.new options[:database]
db.results_as_hash = true;

startstop = db.get_first_row("SELECT min(key) as start, max(key) as stop FROM records")
start = startstop['start'].to_i
stop = startstop['stop'].to_i

while ( start < stop )
  begin_ = Time.now
  numRecords = 0
  recordsDeletable = []
  rs = db.execute( "SELECT * FROM records WHERE key >= #{start} AND key < #{stop} ORDER by key LIMIT #{options[:number]}" )
  rs.each do |row|
    numRecords += 1
    jsonRecord = JSON.parse(row['record'])
    p "KEY: #{row["key"]},#{jsonRecord["lrmsId"]}"
    recordBuff = Record.new(jsonRecord)
    r = TorqueExecuteRecord.new(recordBuff.to_hash)
    tries = 0
    begin
      tries += 1
      r.save
      if not r.valid?
        puts r.errors.full_messages if options[:verbose]
        oldRecord = TorqueExecuteRecord.get(:search, :lrmsId => r.lrmsId, :start =>r.start )
        newRecord = TorqueExecuteRecord.find(oldRecord["id"])
        newRecord.load(r.attributes)
        newRecord.save
      end
      recordsDeletable << row["key"] 
    rescue Exception => e
      puts "Error sending  #{r.lrmsId}:#{e.to_s}. Retrying" if options[:verbose]
      if ( tries < 2)
        sleep(2**tries)
        retry
      else
        puts "Could not send record #{r.lrmsId}."
      end
    end
  end
  #DELETE the records which have been sent succesfully.
  db.transaction
  recordsDeletable.each do |key|
    p "DELETE #{key}"
    begin
      db.execute( "DELETE FROM records WHERE key = #{key}" )
    rescue
      p "ERROR DELETING #{key}"
    end
  end
  db.commit
  #update the start variable for next iteration.
  start = start.to_i + options[:number].to_i;
  time = Time.now - begin_
  recordsPerMin = (numRecords/Float(time))*60
  printf "Records/min: %0.1f\n", recordsPerMin
end

