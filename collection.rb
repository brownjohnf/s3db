module S3DB
  class Collection
    attr_reader :database, :name, :schema

    class << self
      def create(database, collection_name, schema = {})
        collection = new(database, collection_name)
        collection.schema = schema

        puts 'schema set in create'

        collection.save

        puts 'called save in create'

        collection
      end

      def find(database, name)
        collection = new(database, name)
        # confirm that the db exists
        collection.exists!

        collection
      end
    end

    def initialize(database, name)
      @database = database
      @name = name
      # set up all the s3 bindings, etc.
      puts 'coll init'
    end

    def insert(data)
      record = Record.new(@database, self, data)
      record.save

      record
    end

    def schema=(schema)
      @schema = schema
      puts 'coll schema set'
    end

    def set_schema
      raise ArgumentError, 'missing schema!' unless @schema

      S3DB.backend.write_schema(@database.name, @name, @schema.to_json)
      puts 'schema written'
    end

    def save
      S3DB.backend.write_collection(@database.name, @name)
      S3DB.backend.bootstrap(@database.name, @name)
      set_schema
    end

    def find(record_id)
      res = S3DB.backend.read_record(@database.name, @name, record_id)

      JSON.parse(res)
    end

    def all
      S3DB.backend.list_records(@database.name, @name).map do |f|
        JSON.parse(f)
      end
    end
  end
end
