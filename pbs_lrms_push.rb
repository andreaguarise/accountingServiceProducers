#!/usr/bin/ruby -w
require 'rubygems'
require 'optparse'
require 'active_resource'
require 'json'
require 'date'

options = {}
  
class GenericResource < ActiveResource::Base
  self.format = :xml
end

