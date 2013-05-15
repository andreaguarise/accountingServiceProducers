#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'
require 'mysql'

options = {}
values = {}

class GenericResource < ActiveResource::Base
  self.format = :xml
end


class EmiComputeAccountingRecord < GenericResource
end

class TorqueExecuteRecord < GenericResource  
end

class BlahRecord < GenericResource
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
  
  options[:publishers] = false
  opt.on( '-p', '--publishers', 'Output retrieved publishers') do
    options[:publishers] = true
  end
  
  options[:uri] = nil
  opt.on( '-U', '--URI uri', 'URI to contact') do |uri|
    options[:uri] = uri
  end

  options[:token] = nil
  opt.on( '-t', '--token token', 'Authorization token that must be obtained from the service administrator') do |token|
    options[:token] = token
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

tokens = {}
File.open("/tmp/faust_tokens", "r").each_line do |line|
  /(.*):(.*)/.match(line)
  tokens[$1] = $2
end

tokens.each do |token|
  puts token.to_json
end

publishers = {}

TorqueExecuteRecord.site = options[:uri]
TorqueExecuteRecord.headers['Authorization'] = "Token token=\"#{options[:token]}\""
TorqueExecuteRecord.timeout = 5

BlahRecord.site = options[:uri]
BlahRecord.headers['Authorization'] = "Token token=\"#{options[:token]}\""
BlahRecord.timeout = 5
#EmiComputeAccountingRecord.proxy = ""
$stdout.sync = true

begin
	con = Mysql.new values['dbhost'], values['dbuser'], values['dbpasswd'], values['dbname']
	puts con.get_server_info
	firstlast = con.query("SELECT min(id) as min_id, max(id) as max_id FROM jobTransSummary")
	r = firstlast.fetch_row
	stop_id = r[1]
  current_id = r[0]
	stop_id = values['stop_id'] if values['stop_id']
	current_id = values['start_id'] if values['start_id']
	threads = values['threads'].to_i + 1
	thread_counter = 0
	puts "Start from #{current_id}, stop at #{stop_id}"
	until current_id > stop_id
		rs = con.query("SELECT * FROM jobTransSummary WHERE id > #{current_id} LIMIT #{values['limit']}")
		n_rows = rs.num_rows
		i = 0
		rs.each_hash do |row|
			i += 1
			if i==10 then 
				print "."
				i = 0 
			end
			current_id=row['id']
			#puts "Current ID: #{current_id}"
			break if current_id > stop_id
			hlr_row = HlrDbRow.new(row)
			Thread.new {
			  /^(.*):.*$/.match(row['gridResource'])
			  current_publisher = $1
			  if options[:publishers]
			    publishers[current_publisher] = row['siteName'] if not publishers.include?(current_publisher)
			  end	 
			  if not options[:dryrun] 
				  lrmsRecord = TorqueExecuteRecord.new(hlr_row.recordHashLRMS)
				  tries = 0
				  begin 	
					  tries += 1
					  lrmsRecord.save
				  rescue
					  puts "error sending LRMS: #{row['uniqueChecksum']}. Retrying"
					  if ( tries < 3)
						  sleep(2**tries)
						  retry
					  end
				  end
				  blahRecord = BlahRecord.new(hlr_row.recordHashBlah)
          tries = 0
          begin   
            tries += 1
            blahRecord.save
          rescue
            puts "error sending BLAH: #{row['uniqueChecksum']}. Retrying"
            if ( tries < 3)
              sleep(2**tries)
              retry
            end
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
		
	end
	main = Thread.main
	current = Thread.current
	all = Thread.list
	all.each { |t| t.join unless t == main }

  publishers.each do |publisher,value|
          puts "#{publisher}:#{value}"
  end

rescue Mysql::Error => e
	puts e.errno
	puts e.error
ensure
	con.close if con
end
puts

