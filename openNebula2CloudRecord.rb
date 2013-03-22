#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'

options = {}
values = {}
class GenericResource < ActiveResource::Base
  self.format = :json
end

class CloudRecord < GenericResource
end

class OpenNebulaJsonRecord
  def initialize(jsonRecord)
    @jsonRecord = jsonRecord
  end

  def recordVector
    rv = {}
    #rv['FQAN'] = @jsonRecord['a']
    rv['VMUUID'] = @jsonRecord["VM"]["ID"]
    rv['cloudType'] = "OpenNebula"
    rv['cpuCount'] = @jsonRecord["VM"]["TEMPLATE"]["CPU"]
    #rv['cpuDuration'] = @jsonRecord["VM"]
    #rv['Disk'] = @jsonRecord['e']
    rv['diskImage'] = @jsonRecord["VM"]["TEMPLATE"]["DISK"]["IMAGE"]
    rv['endTime'] = @jsonRecord["ETIME"]
    #rv['globaluserName'] = @jsonRecord['h']
    rv['localVMID'] = @jsonRecord["VM"]["ID"]
    rv['local_group'] = @jsonRecord["VM"]["GNAME"]
    rv['local_user'] = @jsonRecord["VM"]["UNAME"]
    rv['memory'] = @jsonRecord["VM"]["TEMPLATE"]["MEMORY"]
    rv['networkInbound'] = @jsonRecord["VM"]["NET_RX"]
    rv['networkOutBound'] = @jsonRecord["VM"]["NET_TX"]
    #rv['networkType'] = @jsonRecord['q']
    #rv['resource_name'] = @resourceName
    rv['startTime'] = @jsonRecord["STIME"]
    #rv['status'] = @jsonRecord['t']
    #rv['storageRecordId'] = @jsonRecord['u']
    #rv['suspendDuration'] = @jsonRecord['v']
    #rv['wallDuration'] = @jsonRecord['z']
    rv
  end
  
  def to_s
    stringVector = "VMUUID = " + self.recordVector['VMUUID'] + "\n"
    stringVector += "cloudType = " + self.recordVector['cloudType'] + "\n"
    stringVector += "cpuCount = " + self.recordVector['cpuCount'] + "\n"
  end

  #def resourceName=(resourceName)
#	@resourceName = resourceName
 # end

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

  options[:file] = nil
  opt.on( '-f', '--file file', 'file containing the output of the oneacct --json command') do |token|
    options[:file] = token
  end

  opt.on( '-h', '--help', 'Print this screen') do
    puts opt
    exit
  end
end

opt_parser.parse!

ARGV.each do |f|
  f =~/(.*)=(.*)/
  data = Regexp.last_match
  values[data[1]] = data[2]
end

CloudRecord.site = options[:uri]
CloudRecord.headers['Authorization'] = "Token token=\"#{options[:token]}\""
CloudRecord.timeout = 5
#EmiComputeAccountingRecord.proxy = ""

  threads = values['threads'].to_i + 1
  thread_counter = 0
  $stdout.sync = true
  i = 0
  parsed = JSON.parse IO.read(options[:file])
  parsed["HISTORY_RECORDS"]["HISTORY"].each do |jsonRecord|
    i += 1
    if i==10 then
      print "."
    i = 0
    end
    Thread.new {
      puts JSON.pretty_generate(jsonRecord)
      record = OpenNebulaJsonRecord.new(jsonRecord)
      puts record
      r = CloudRecord.new(record.recordVector)
      r.resource_name="hdesk-dev-21.to.infn.it"
      tries = 0
      begin
        tries += 1
	r.save
      rescue Exception => e
        puts "Error sending  #{r.VMUUID}:#{e.to_s}. Retrying"
        if ( tries < 3)
          sleep(2**tries)
        retry
        end
      end
    }
    if ( Thread.list.size >= threads ) then
      main = Thread.main
      current = Thread.current
      all = Thread.list
      all.each { |t| t.join unless t == main }
    thread_counter=0
    end
  end
  main = Thread.main
  current = Thread.current
  all = Thread.list
  all.each { |t| t.join unless t == main }


puts

