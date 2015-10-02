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

# Set the backend to be local file storage
S3DB.backend = S3DB::FileBackend.new('/tmp')

# Drop any existing test db
S3DB::Database.drop('sample_s3db')
# Create a new, empty db
S3DB::Database.create('sample_s3db')

# Load an existing db
db = S3DB::Database.new('sample_s3db')
# List the available collections in the db
puts 'Available collections in ' + db.name
puts db.show_collections.inspect

# Create a new collection in the database
coll = db.create_collection('sample_collection', {'id' => 'Fixnum', 'name' => 'String'})
db.create_collection('other_collection')
# List the available collections in the db (should be one now)
puts 'Available collections in ' + db.name
puts db.show_collections.inspect

# Insert a record into one of our dbs
record = coll.insert({"id"=>1, "name"=>"jack"})
puts coll.all.inspect

puts `tree /tmp/sample_s3db`
