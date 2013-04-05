#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'
require 'mysql'

options = {}
values = {}

class GenericResource < ActiveResource::Base
  self.format = :json
end


class EmiComputeAccountingRecord < GenericResource
end


class HlrDbRow
	def initialize(row)
		@row = row
	end

	def recordVector
		time = Time.new
		rv = {}
		rv['recordId'] = @row['uniqueChecksum']
		rv['createTime'] = time.strftime("%Y-%m-%d %H:%M:%S") 
		rv['globalJobId'] = @row['dgJobId']
		rv['localJobId'] = @row['lrmsId']
		rv['localUserId'] = @row['localUserId']
		rv['globalUserName'] = @row['gridUser']
		rv['charge'] = @row['cost']
		rv['queue'] = @row['gridResource']
		rv['group'] = @row['userVo']
		rv['ceCertificateSubject'] = @row['acl']
		rv['startTime'] = @row['date']
		rv['endTime'] = @row['endDate']
		rv['cpuDuration'] = @row['cpuTime']
		rv['wallDuration'] = @row['wallTime']
		rv['machineName'] = @row['gridResource']##get host.domain
		rv['projectName'] = @row['']
		rv['execHost'] = @row['executingNodes']
		rv['physicalMemory'] = @row['pmem']
		rv['virtualMemory'] = @row['vmem']
		rv['serviceLevelIntBench'] = @row['iBench']
		rv['serviceLevelIntBenchType'] = @row['iBenchType'] 
		rv['serviceLevelFloatBench'] = @row['fBench']
		rv['serviceLevelFloatBenchType'] = @row['fBenchType']
		rv['timeInstantETime'] = @row['date']
		rv['voOrigin'] = @row['voOrigin']
		rv['dgasAccountingProcedure'] = @row['accountingProcedure']
		rv['vomsFQAN'] = @row['userFqan']
		rv
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

EmiComputeAccountingRecord.site = options[:uri]
EmiComputeAccountingRecord.headers['Authorization'] = "Token token=\"#{options[:token]}\""
EmiComputeAccountingRecord.timeout = 5
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
				r = EmiComputeAccountingRecord.new(hlr_row.recordVector)
				tries = 0
				begin 	
					tries += 1
					r.save
				rescue
					puts "error sending + #{row['uniqueChecksum']}. Retrying"
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
		
	end
	main = Thread.main
	current = Thread.current
	all = Thread.list
	all.each { |t| t.join unless t == main }

rescue Mysql::Error => e
	puts e.errno
	puts e.error
ensure
	con.close if con
end
puts

