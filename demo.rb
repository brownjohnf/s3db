require_relative 'lib/s3db'

# Clean up so we get a clean slate
S3DB::FileBackend.delete('/tmp/sample_s3db')

module SampleApp
  class << self
    attr_accessor :backend, :database
  end

  self.backend = S3DB::FileBackend.new('/tmp/sample_s3db')
  self.database = S3DB::Database.create(self.backend, 'sample_app')

  class Person
    include S3DB::Record

    database SampleApp.database
    collection_name :people

    string :name
  end

  class Word
    include S3DB::Record

    database SampleApp.database
    collection_name :words
    id_generator lambda { |name| name.gsub(' ', '').downcase }
    id_field :word

    string :word
  end

  class App
    include S3DB::Record

    database SampleApp.database
    collection_name :apps
    id_generator lambda { |source_id| 'source-id-' + source_id.to_s }
    id_field :source_id

    string :source_id
    string :name
  end

  App.create(name: 'Ibotta', source_id: 'com.ibotta.android')

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

  words.first(1).each do |word|
    puts Word.create({
      'word' => word,
    }).inspect
  end

  words.last(1).each do |word|
    puts Word.new({
      'word' => word,
    }).save
  end


  Person.create({
    'name' => 'Jack Brown',
  })

  puts Person.all
  puts Word.all
  puts App.all

  puts App.find('source-id-com.ibotta.android').inspect
end

puts `(which tree && tree -a /tmp/sample_s3db) || true`
