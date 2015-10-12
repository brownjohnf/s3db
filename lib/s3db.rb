require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)

require_relative 's3db/utils'
require_relative 's3db/collection'
require_relative 's3db/database'
require_relative 's3db/record'
require_relative 's3db/backend'
require_relative 's3db/file_backend'

module S3DB
  class << self
    attr_accessor :backend
  end
end
