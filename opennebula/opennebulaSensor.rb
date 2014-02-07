#!/usr/bin/ruby -w

require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'
require 'dbi'
require 'uuidtools'

class GenericResource < ActiveResource::Base
  self.format = :xml
end


class OneacctFile
  def initialize(file)
    @file = file
  end
  
  def parse
    records = []
    parsed = JSON.parse IO.read(@file)
    parsed["HISTORY_RECORDS"]["HISTORY"].each do |jsonRecord|
      record = OpenNebulaJsonRecord.new(jsonRecord)
      record.resourceName = "Test"
      records << record.recordVector
    end
    records
  end
  
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

class OpennebulaSensor
  def initialize
    @options = {}
  end
  
  def getLineParameters
    
    opt_parser = OptionParser.new do |opt|
      opt.banner = "Usage: record_post [OPTIONS]"

      @options[:verbose] = false
      opt.on( '-v', '--verbose', 'Output more information') do
        @options[:verbose] = true
      end
  
      @options[:dryrun] = false
        opt.on( '-d', '--dryrun', 'Do not talk to server') do
        @options[:dryrun] = true
      end
  
      @options[:uri] = nil
      opt.on( '-U', '--URI uri', 'URI to contact') do |uri|
        @options[:uri] = uri
      end
      
      @options[:uri] = nil
      opt.on( '-P', '--Publisher type', 'Publisher type') do |type|
        @options[:publisher_type] = type  
      end
      
      @options[:file] = nil
      opt.on( '-F', '--File file', 'File containing the output of oneacct --json command') do |file|
        @options[:file] = file
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
  
  def newPublisher(records)
    r = case
    when @options[:publisher_type] == "JSON" then
      p = OneRecordJSON.new(records)
    when @options[:publisher_type] == "XML" then
      p = OneRecordXML.new(records)
    when @options[:publisher_type] == "ssm" then
      p = OneRecordText.new(records)
    when @options[:publisher_type] == "ssmfile" then
      p = OneRecordTextFile.new(records)
      p.limit = 1000
      p.dir = "/tmp/"
    #when @options[:publisher_type] == "ActiveResource" then
    #  OneRecord.site = @options[:uri]
    #  OneRecord.headers['Authorization'] = "Token token=\"#{@options[:token]}\""
    #  OneRecord.timeout = 5
    #  OneRecord.proxy = ""
    #  p = OneRecordActiveResource.new(time.to_i,tables,dbName)
    else
      p =  nil
    end
    p
  end
  
  
  def main
    self.getLineParameters
    f = OneacctFile.new(@options[:file])
    records = f.parse
    p = newPublisher(records)
    #records.each do |r|
    #  puts "==== #{r} ===="
    #end  
    r = newPublisher(records)
    r.post
  end
end