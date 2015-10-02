require 'json'

require_relative 'record'
require_relative 'collection'
require_relative 'database'
require_relative 'file_backend'

module S3DB
  class << self
    attr_accessor :backend
  end
end

# sample usage
S3DB.backend = S3DB::FileBackend.new('/tmp')

S3DB::Database.drop('sample_s3db')
S3DB::Database.create('sample_s3db')


db = S3DB::Database.new('sample_s3db')
puts db.show_collections

coll = db.create_collection('sample_collection')

record = S3DB::Record.create(db, coll, {"id"=>1, "name"=>"jack"})

puts `tree /tmp/sample_s3db`
