#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'
require 'sqlite3'

options = {}
fakeUsers = [] #demo
fakeQueues = [] #demo
class GenericResource < ActiveResource::Base
  self.format = :xml
end

class BatchExecuteRecord < GenericResource
end

class Record
  def initialize(row,fakelocal,fakeusers,fakequeues)
    @row = row
    @fakelocal = fakelocal
    @fakeusers = fakeusers
    @fakequeues = fakequeues
  end

  def to_hash
    #demo
    if @fakelocal
      user_group = @fakeusers.choice
      user_group =~/(.*):(.*):(.*)/
      data = Regexp.last_match
      @row['user'] = data[1]
      @row['group'] = data[2]
      mult = data[3]
      @row['queue'] = @fakequeues.choice
      @row['lrmsId'] = "#{@row['lrmsId']}.fk"
      @row['cput'] = @row['cput']*mult
      @row['walltime']= @row['walltime']*mult
      puts "#{@row['user']} - #{@row['group']} - #{@row['queue']}"
    end
    #demo end
    rh = {}
    rh['recordDate'] = @row['recordDate']
    rh['localUser'] = @row['user']
    #rh[''] = @row['server']
    rh['lrmsId'] = @row['lrmsId']
    rh['queue'] = @row['queue']
    rh['resourceUsed_cput'] = @row['cput']
    rh['resourceUsed_walltime'] = @row['walltime']
    rh['resourceUsed_vmem'] = @row['vmem']
    rh['resourceUsed_mem'] = @row['mem']
    #rh[''] = @row['processors']
    rh['localGroup'] = @row['group']
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
  ###demo
  options[:fakelocal] = false
  opt.on( '-f', '--fakelocal', 'mangle record record of a grid farm to simulate a local one. Just for DEMO') do
    options[:fakelocal] = true
  end

  options[:fakeQueues] = nil
  opt.on( '-q', '--fakeQueues file', 'file containing the fake queues one queue per line') do |token|
    options[:fakeQueues] = token
  end

  options[:fakeUsers] = nil
  opt.on( '-u', '--fakeUsers file', 'file containing the fake queues user:group one per line') do |token|
    options[:fakeUsers] = token
  end

  ###demo end
  options[:verbose] = false
  opt.on( '-v', '--verbose', 'Output more information') do
    options[:verbose] = true
  end

  options[:dryrun] = false
  opt.on( '-D', '--dryrun', 'Do not actually send the records.') do
    options[:dryrun] = true
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

BatchExecuteRecord.site = options[:uri]
BatchExecuteRecord.headers['Authorization'] = "Token token=\"#{options[:token]}\""
BatchExecuteRecord.timeout = 5
BatchExecuteRecord.proxy = ""

if options[:fakelocal]
  File.open(options[:fakeUsers], "r").each_line do |line|
    line.chomp!
    fakeUsers << "#{line}:#{rand}"
  end

  File.open(options[:fakeQueues], "r").each_line do |line|
    line.chomp!
    fakeQueues << line
  end
end

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
    recordBuff = Record.new(jsonRecord,options[:fakelocal],fakeUsers,fakeQueues)
    r = BatchExecuteRecord.new(recordBuff.to_hash)
    tries = 0
    begin
      tries += 1
      if not options[:dryrun]
        r.save
        if not r.valid?
          puts r.errors.full_messages if options[:verbose]
          oldRecord = BatchExecuteRecord.get(:search, :lrmsId => r.lrmsId, :start =>r.start )
          newRecord = BatchExecuteRecord.find(oldRecord["id"])
        newRecord.load(r.attributes)
        newRecord.save
        end
        recordsDeletable << row["key"]
      end
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

