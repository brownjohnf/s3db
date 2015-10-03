require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)

require_relative 'collection'
require_relative 'database'
require_relative 'file_backend'

module S3DB
  class << self
    attr_accessor :backend
  end

  class Utils
    class << self
      def sanitize(input)
        raise ArgumentError, 'invalid input!' unless input =~ /\w/i

        input.to_s
      end
    end
  end
end
