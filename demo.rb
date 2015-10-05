require_relative 'lib/s3db'

module DummyApp

  # initialize
  # Set the backend to be local file storage
  S3DB.backend = S3DB::FileBackend.new('/tmp')
  # Fore drop any existing test db
  S3DB::FileBackend.delete('/tmp/sample_s3db')
  # Create/ensure presence of a new, empty db
  S3DB::Database.create('sample_s3db')
  # load the db
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

  words = [
    'abreaction',
    'abreactions',
    'abreast',
    'abridge',
    'abridged',
    'abridges',
    'abridging',
    'abridgment',
    'abroad',
    'abrogate',
  ]

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

  puts Person.all
end
