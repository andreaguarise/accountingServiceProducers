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

  opt.on( '-h', '--help', 'Print this screen') do
    puts opt
    exit
  end
end

opt_parser.parse!

db = SQLite3::Database.new options[:database]

db.execute( "select * from records" ) do |row|
  p row
end