module S3DB
  class << self
    attr_accessor :backend
  end

  class FileBackend
    attr_reader :path

    def initialize(path)
      @path = path
    end

    def write_db(db_name)
      Dir.mkdir(File.join(@path, db_name))
    end
  end

  class Database
    class << self
      def show
      end

      def create(db_name)
        begin
          S3DB.backend.write_db(db_name)
        rescue Errno::EEXIST
          STDERR.puts 'database exists!'
          exit 1
        end
      end

      def drop
      end
    end

    def show_collections
    end

    def create_collection(collection)
      Collection.create(self, collection)
    end

    def drop_collection(collection)
      Collection.load(self, collection).drop
    end
  end

  class Collection
    class << self
      def create(database, name, schema)
        collection = new(database, name, schema)
        collection.save

        collection
      end

      def load(database, name)
        collection = new(database, name)
        # confirm that the db exists
        collection.exists!

        collection
      end
    end

    def initialize(database, name, schema)
      @database = database
      @name = name
      @schema = schema
      # set up all the s3 bindings, etc.
    end
  end

  class Record
    class << self
      def create(database, collection, data)
        record = new(database, collection, data)
        # validate against schema

        record.save

        record
      end
    end
  end
end

# sample usage
S3DB.backend = S3DB::FileBackend.new('/tmp')

db = S3DB::Database.create('sample_s3db')

puts `ls -l /tmp | grep db`
