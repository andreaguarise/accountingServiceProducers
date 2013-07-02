#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'
require 'date'
require 'uuidtools'

options = {}
values = {}

class GenericResource < ActiveResource::Base
  self.format = :xml
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
    rv['cloudType'] = "OpenNebula"
    if @jsonRecord["VM"]["TEMPLATE"]["CPU"] then
      #Number of physical CPU was assigned in the template. Use this
      rv['cpuCount'] = @jsonRecord["VM"]["TEMPLATE"]["CPU"]
    else
      #Number of physical CPU was not assigned in the template, just Virtual CPUS
      #Where requested. This causes possible overbooking. Use this if physical is
      #not specified
      rv['cpuCount'] = @jsonRecord["VM"]["TEMPLATE"]["VCPU"]
    end
    #rv['cpuDuration'] = @jsonRecord["VM"]
    #rv['Disk'] = @jsonRecord['e']
    if @jsonRecord["VM"]["TEMPLATE"]["DISK"]
      if @jsonRecord["VM"]["TEMPLATE"]["DISK"].kind_of?(Array)
        rv['diskImage'] = ""
        @jsonRecord["VM"]["TEMPLATE"]["DISK"].each do |disk|
          rv['diskImage'] += disk["IMAGE"] if disk["IMAGE"]
        end
      else
        rv['diskImage'] = @jsonRecord["VM"]["TEMPLATE"]["DISK"]["IMAGE"] if @jsonRecord["VM"]["TEMPLATE"]["DISK"]["IMAGE"]
      end
    end
    rv['endTime'] = Time.at(@jsonRecord["ETIME"].to_i).to_datetime
    #rv['globaluserName'] = @jsonRecord["e"]
    rv['localVMID'] = @jsonRecord["VM"]["ID"]
    rv['local_group'] = @jsonRecord["VM"]["GNAME"]
    rv['local_user'] = @jsonRecord["VM"]["UNAME"]
    rv['memory'] = @jsonRecord["VM"]["TEMPLATE"]["MEMORY"]
    rv['networkInbound'] = @jsonRecord["VM"]["NET_RX"]
    rv['networkOutBound'] = @jsonRecord["VM"]["NET_TX"]
    #rv['networkType'] = @jsonRecord['q']
    #rv['resource_name'] = @resourceName
    rv['status'] = @jsonRecord['VM']['STATE'] + ":" + @jsonRecord['VM']['LCM_STATE']
    #rv['storageRecordId'] = @jsonRecord['u']
    #rv['suspendDuration'] = @jsonRecord['v']

    ## Compute endTime from the available information. use current date if none applies
    endTimeBuff = Time.new.to_time.to_i
    endTimeBuff = @jsonRecord["RETIME"] if @jsonRecord["RETIME"] != "0"
    endTimeBuff = @jsonRecord["EETIME"] if @jsonRecord["EETIME"] != "0"
    endTimeBuff = @jsonRecord["ETIME"] if @jsonRecord["ETIME"] != "0"
    rv['endTime'] = Time.at(endTimeBuff.to_i).to_datetime

    ## Compute startTime from the available information. use endTime if none applies
    startTimeBuff = endTimeBuff
    startTimeBuff = @jsonRecord["RSTIME"] if @jsonRecord["RSTIME"] != "0"
    startTimeBuff = @jsonRecord["PSTIME"] if @jsonRecord["PSTIME"] != "0"
    startTimeBuff = @jsonRecord["STIME"] if @jsonRecord["STIME"] != "0"
    rv['startTime'] = Time.at(startTimeBuff.to_i).to_datetime

    ## wallDuration is by definition endTime - startTime
    rv['wallDuration'] = rv['endTime'].to_i - rv['startTime'].to_i

    ## VMUUID must be assured unique.
    buffer = @resourceName  + "/" + @jsonRecord["STIME"] + "/" +@jsonRecord["VM"]["ID"]
    rv['VMUUID'] = UUIDTools::UUID.md5_create(UUIDTools::UUID_DNS_NAMESPACE,buffer)
    rv
  end

  def to_s
    stringVector = "VMUUID = " + self.recordVector['VMUUID'] + "\n"
    stringVector += "startTime = " + self.recordVector['startTime'].to_s + "\n"
    stringVector += "endTime = " + self.recordVector['endTime'].to_s + "\n"
  end

  def resourceName=(resourceName)
    @resourceName = resourceName
  end

  def resourceName
    @resourceName
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
CloudRecord.proxy = ""

if values['resource_name'] then
  ResourceName=values['resource_name']
else
  ResourceName=ENV["HOSTNAME"]
end
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
    puts JSON.pretty_generate(jsonRecord) if options[:verbose]
    record = OpenNebulaJsonRecord.new(jsonRecord)
    record.resourceName = ResourceName
    puts record.to_s
    r = CloudRecord.new(record.recordVector)
    r.resource_name=ResourceName
    tries = 0
    begin
      tries += 1
      r.save
      if not r.valid?
        puts r.errors.full_messages if options[:verbose]
        recordBuff = CloudRecord.get(:search, :VMUUID => r.VMUUID )
        newRecord = CloudRecord.find(recordBuff["id"])
        newRecord.load(r.attributes)
        newRecord.save
      end

    rescue Exception => e
      puts "Error sending  #{r.VMUUID}:#{e.to_s}. Retrying" if options[:verbose]
      if ( tries < 2)
        sleep(2**tries)
        retry
      else
        puts "Could not send record #{r.VMUUID}."
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

