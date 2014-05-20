#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'json'
require 'mysql'

options = {}
values = {}

class SSMrecord
  def initialize(row)
    @row = row
  end
  
  def machineName
    if /^(.*):.*$/.match(@row['gridResource'])
      $1
    end  
  end
  
  def queue
    if /^.*:(.*)$/.match(@row['gridResource'])
      $1
    end
  end
  
  def userFQAN
    @row['userFqan']
  end
  
  def VOGroup
    ""
  end
  
  def VORole
    ""
  end
  
  def processors
    ""
  end
  
  def nodeCount
    ""
  end
  
  def infrastructureDescription
    ""
  end
  
  def infrastructureType
    ""  
  end
   
  def to_s
    out = ""
    out += "Site: " + @row['siteName'] + "\n"
    out += "SubmitHost: " + @row['gridResource'] + "\n"
    out += "MachineName: " + self.machineName + "\n"
    out += "Queue: " + self.queue + "\n"
    out += "LocalJobId: " + @row['lrmsId'] + "\n"
    out += "LocalUserId: " + @row['localUserId'] + "\n"
    out += "GlobalUserName: " + @row['gridUser'] + "\n"
    out += "FQAN: " + self.userFQAN + "\n"
    out += "VO: " + @row['userVo'] + "\n"
    out += "VOGroup: " + self.VOGroup + "\n"
    out += "VORole: " + self.VORole + "\n"
    out += "WallDuration: " + @row['wallTime'] + "\n"
    out += "CpuDuration: " + @row['cpuTime'] + "\n"
    out += "Processors: " + self.processors + "\n"
    out += "NodeCount: " + self.nodeCount + "\n"
    out += "StartTime: " + @row['start'] + "\n"
    out += "EndTime: " + @row['end'] + "\n"
    out += "InfrastructureDescription: " + self.infrastructureDescription + "\n"
    out += "InfrastructureType: " + self.infrastructureType + "\n"
    out += "MemoryReal: " + @row['pmem'] + "\n"
    out += "MemoryVirtual: " + @row['vmem'] + "\n"
    out += "ServiceLevelType: " + @row['iBenchType'] + "\n"
    out += "ServiceLevel: " + @row['iBench'] + "\n"
    out += "%%\n"
  end
end

class SSMmessage
  def initialize(dir,file)
    @dir = dir
    @file = file
    @message = "APEL-individual-job-message: v0.3\n"
  end
  
  def addRecord(r)
    @message += r.to_s
  end
  
  def write
    puts @message
  end
  
end

class HlrDbRow
	def initialize(row)
		@row = row
	end

	def recordHashLRMS
	  rh = {}
    rh['recordDate'] = @row['date']
    rh['user'] = @row['localUserId']
    #rh[''] = @row['server']
    rh['lrmsId'] = @row['lrmsId']
    if /^.*:(.*)$/.match(@row['gridResource'])
      rh['queue'] = $1
    end
    rh['resourceUsed_cput'] = @row['cpuTime']
    rh['resourceUsed_walltime'] = @row['wallTime']
    rh['resourceUsed_vmem'] = @row['vmem']
    rh['resourceUsed_mem'] = @row['pmem']
    #rh[''] = @row['processors']
    rh['group'] = @row['localGroup']
    #rh['jobName'] = @row['jobName']
    rh['ctime'] = @row['start']
    rh['qtime'] = @row['start']
    rh['etime'] = @row['start']
    rh['start'] = @row['start']
    rh['end'] = @row['end']
    rh['execHost'] = @row['executingNodes']
    rh['exitStatus'] = "0"
    rh
	end

  def recordHashBlah
    rh = {}
    rh['ceId'] = @row['gridResource']
    rh['clientId'] = @row['dgJobId']
    rh['jobId'] = @row['dgJobId']
    rh['localUser'] = @row['localUserId']
    rh['lrmsId'] = @row['lrmsId']
    rh['recordDate'] = @row['date']
    rh['timestamp'] = @row['date']
    rh['userDN'] = @row['gridUser']
    rh['userFQAN'] = @row['userFqan']
    rh
  end

end

opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: record_post [OPTIONS] field=value ..."

  options[:verbose] = false
  opt.on( '-v', '--verbose', 'Output more information') do
    options[:verbose] = true
  end
  
  options[:dryrun] = false
  opt.on( '-d', '--dryrun', 'Do not talk to server') do
    options[:dryrun] = true
  end
  
  options[:dir] = nil
  opt.on( '-D', '--Dir dir', 'Output dir to store SSM files') do |dir|
    options[:dir] = dir
  end
  
  options[:num] = 1000
  opt.on( '-n', '--num num', 'num records per message') do |num|
    options[:num] = num
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

$stdout.sync = true

begin
	con = Mysql.new values['dbhost'], values['dbuser'], values['dbpasswd'], values['dbname']
	firstlast = con.query("SELECT min(id) as min_id, max(id) as max_id FROM jobTransSummary")
	r = firstlast.fetch_row
	stop_id = r[1]
  current_id = r[0]
	stop_id = values['stop_id'] if values['stop_id']
	current_id = values['start_id'] if values['start_id']
	puts "Start from #{current_id}, stop at #{stop_id}"
	until current_id.to_i > stop_id.to_i
		rs = con.query("SELECT * FROM jobTransSummary WHERE id > #{current_id} LIMIT #{values['limit']}")
		n_rows = rs.num_rows
		i = 0
		rs.each_hash do |row|
		  ###COMPOSE AND WRITE MESSAGESE HERE
		  record_counter = 0
		  message = SSMmessage.new(options[:dir],"file")
		  while  record_counter < options[:num].to_i do
		     record_counter = record_counter +1
		     record = SSMrecord.new(row)
		     message.addRecord(record)
		     current_id = row['id']
		     puts "current_id:#{current_id}, going to #{stop_id}"
		  end
		  message.write
		end
	end

rescue Mysql::Error => e
	puts e.errno
	puts e.error
ensure
	con.close if con
end
puts

