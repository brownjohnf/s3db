require 'json'
require 'uuidtools'
require 'slop'

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


# sample usage #################################################################

module DummyApp

  # initialize
  # Set the backend to be local file storage
  S3DB.backend = S3DB::FileBackend.new('/tmp')
  # Drop any existing test db
  S3DB::Database.drop('sample_s3db')
  # Create/ensure presence of a new, empty db
  S3DB::Database.create('sample_s3db')

  DB = S3DB::Database.new('sample_s3db')

  # start building your app models
  class Word < S3DB::Collection
    schema({
      'id' => 'String',
      'word' => 'String',
    })

    database DB

    collection 'words'
  end
  Word.write

  words = `cat /usr/share/dict/cracklib-small | head -n 100`.chomp.split("\n")

  words[0..4].each do |word|
    Word.create({
      'word' => word,
    })
  end

  words[5..9].each do |word|
    Word.new({
      'word' => word,
    }).save
  end

  puts Word.all

  class Person < S3DB::Collection
    schema({
      'name' => 'String',
      'friends' => 'Array',
    })

    database DB
    collection :people
    id_generator lambda { |name| name.downcase }
    id_field 'name'
  end
  Person.write

  Person.create({
    'name' => 'Jack',
    friends: %w{ Hayley Brett Emily }
  })

  Person.all
end

=begin
# Create a new collection in the database
coll = db.create_collection('sample_collection', {'id' => 'String', 'name' => 'String'})
db.create_collection('words')
coll.insert({"id"=>'2', "name"=>"Emily"})

# List the available collections in the db (should be one now)
puts '==> Available collections in ' + db.name
puts db.show_collections

# switch collections
coll = db.use('words')

# Set/Update the schema
coll.schema = {'id' => 'String', 'word' => 'String' }
coll.save


puts '==> All records in ' + coll.name
puts coll.all
=end

puts `tree /tmp/sample_s3db`
