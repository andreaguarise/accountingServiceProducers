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


class LocalRecord
  def initialize(records)
    @records = records
  end
end

class OneRecordSSM < LocalRecord
  
  def print(record)
    "VMUUID: " + record['VMUUID'] + "\n" +
    #"SiteName: " + + "\n" +
    "MachineName: " + record['localVMID'] + "\n" +
    "LocalUserId: " + record['local_user'] + "\n" +
    "LocalGroupId: " + record['local_group'] + "\n" +
    "GlobaUserName: " + "" + "\n" +
    "FQAN: " + "" + "\n" +
    "Status: " + record['statusLiteral']+ "\n" + ##FIXME Need status table opennebula --> apel
    "StarTime: " + record['startTime'].to_i.to_s + "\n" +
    "EndTime: " + record['endTime'].to_i.to_s + "\n" + #FIXME set in ssm record just if status == completed
    "SuspendDuration: " + "" + "\n" +
    "WallDuration: " + record['wallDuration'].to_i.to_s + "\n" +
    "CpuDuration: " +  record['cpuDuration'].to_i.to_s + "\n" + #Check validity of this number! It is inferred from percentage of CPU consupmption
    "CpuCount: " + record['cpuCount'] + "\n" +
    "NetworkType: " + "" + "\n" +
    "NetworkInbound: " + record['networkInbound'] + "\n" +
    "NetworkOutbound: " + record['networkOutbound'] + "\n" +
    "Memory: " + record['memory'] + "\n" +
    "Disk: " + "" + "\n" +
    "StorageRecordId: " + "" + "\n" +
    "ImageId: " + record['diskImage'] + "\n" +
    "CloudType: " + "OpenNebula" + "\n" + "%%\n"
  end
  
  
  def post
    @records.each do |record|
      puts print(record)
    end
  end
  
end

class OneRecordSSMFile < OneRecordSSM
  
  @@written = 0
  @@files = 0
  def dir=(dir)
    @dir = dir
  end
  
  def limit=(limit)
    @limit = limit
  end
  
  def RandomExa(length, chars = 'abcdef0123456789')
        rnd_str = ''
        length.times { rnd_str << chars[rand(chars.size)] }
        rnd_str
    end
  
  def generateFileName
    time = Time.now.to_i
    timeHex = time.to_s(16)
    random_string = RandomExa(6)
    filename = timeHex + random_string
    filename

  end
  
  def post 
    while not @records.empty?
      @@written = 0
      out = File.new("#{@dir}/#{self.generateFileName}","w")
      if out
        while ( @@written < @limit.to_i)
          break if @records.empty?
          record = @records.pop
          out.syswrite(print(record))
          @@written += 1
        end
      else
        puts "Could not open file!"
        exit
      end 
      @@files +=1
      out.close
    end
  end
  
end

class OneacctFile
  def initialize(file,resourceName)
    @file = file
    @resourceName = resourceName
  end
  
  def parse
    records = []
    parsed = JSON.parse IO.read(@file)
    parsed["HISTORY_RECORDS"]["HISTORY"].each do |jsonRecord|
      record = OpenNebulaJsonRecord.new(jsonRecord)
      record.resourceName = @resourceName
      records << record.recordVector
    end
    records
  end
  
end

class OpenNebulaStatus
  def initialize(state,lcm_state)
    @state = state
    @lcm_state = lcm_state
    @state_ary = ['INIT','PENDING','HOLD','ACTIVE','STOPPED','SUSPENDED','DONE','FAILED','POWEROFF','UNDEFINED1','UNDEFINED2']
    @lcmstate_ary = ['LCM_INIT',
      'PROLOG',
      'BOOT',
      'RUNNING',
      'MIGRATE',
      'SAVE_STOP',
      'SAVE_SUSPEND',
      'SAVE_MIGRATE',
      'PROLOG_MIGRATE',
      'PROLOG_RESUME',
      'EPILOG_STOP',
      'EPILOG',
      'SHUTDOWN',
      'CANCEL',
      'FAILURE',
      'CLEANUP',
      'UNKNOWN',
      'HOTPLUG',
      'SHUTDOWN_POWEROFF',
      'BOOT_UNKNOWN',
      'BOOT_POWEROFF',
      'BOOT_SUSPENDED',
      'BOOT_STOPPED',
      'LCMUNDEFINED1',
      'LCMUNDEFINED2',
      'LCMUNDEFINED3',
      'LCMUNDEFINED4']
  end
  
  def to_s
    if (@state != '3')
      "#{@state_ary[@state.to_i]}"
    else
      "#{@lcmstate_ary[@lcm_state.to_i]}"
    end  
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
    rv['networkOutbound'] = @jsonRecord["VM"]["NET_TX"]
    rv['cpuPercentage'] = @jsonRecord["VM"]["CPU"]#<!-- Percentage of 1 CPU consumed (two fully consumed cpu is 200) -->
    rv['cpuPercentageNormalized'] = rv['cpuPercentage'].to_f/(100.0*rv['cpuCount'].to_f)
    #rv['networkType'] = @jsonRecord['q']
    #rv['resource_name'] = @resourceName
    rv['status'] = @jsonRecord['VM']['STATE'] + ":" + @jsonRecord['VM']['LCM_STATE']
    state = OpenNebulaStatus.new(@jsonRecord['VM']['STATE'],@jsonRecord['VM']['LCM_STATE'])
    rv['statusLiteral'] = state.to_s
    #rv['storageRecordId'] = @jsonRecord['u']
    #rv['suspendDuration'] = @jsonRecord['v']

    ## Compute endTime from the available information. use current date if none applies
    endTimeBuff = Time.new.to_time.to_i
    endTimeBuff = @jsonRecord["RETIME"] if @jsonRecord["RETIME"] != "0" #RUNNING_ENDTIME
    endTimeBuff = @jsonRecord["EETIME"] if @jsonRecord["EETIME"] != "0" #EPILOG_ENDTIME
    endTimeBuff = @jsonRecord["ETIME"] if @jsonRecord["ETIME"] != "0"
    rv['endTime'] = Time.at(endTimeBuff.to_i).to_datetime

    ## Compute startTime from the available information. use endTime if none applies
    startTimeBuff = endTimeBuff
    startTimeBuff = @jsonRecord["RSTIME"] if @jsonRecord["RSTIME"] != "0" #RUNNING_STARTTIME
    startTimeBuff = @jsonRecord["PSTIME"] if @jsonRecord["PSTIME"] != "0" #PROLOG_STARTTIME
    startTimeBuff = @jsonRecord["STIME"] if @jsonRecord["STIME"] != "0"
    rv['startTime'] = Time.at(startTimeBuff.to_i).to_datetime

    ## wallDuration is by definition endTime - startTime
    rv['wallDuration'] = rv['endTime'].to_i - rv['startTime'].to_i
    rv['cpuDuration'] = rv['wallDuration'].to_f*rv['cpuPercentageNormalized']
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
      
      @options[:resourceName] = nil
      opt.on( '-r', '--resourceName resourceName', 'Name of resource, e.g. BDII siteName') do |resourceName|
        @options[:resourceName] = resourceName
      end
      
      @options[:uri] = nil
      opt.on( '-d', '--dir dir', 'outpudDir for ssm files') do |outDir|
        @options[:outputDir] = outDir
      end
      
      @options[:limit] = nil
      opt.on( '-L', '--Limit limit', 'number of record per outpu file') do |limit|
        @options[:limit] = limit
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
      p = OneRecordSSM.new(records)
    when @options[:publisher_type] == "ssmfile" then
      p = OneRecordSSMFile.new(records)
      p.limit = @options[:limit]
      p.dir = @options[:outputDir]
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
    f = OneacctFile.new(@options[:file],@options[:resourceName])
    records = f.parse
    p = newPublisher(records)
    p.post
  end
end