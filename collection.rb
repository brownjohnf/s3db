module S3DB
  class Collection
    attr_reader :database, :name

    class << self
      def create(database, collection_name, schema = {})
        S3DB.backend.write_collection(database.name, collection_name)

        collection = new(database, collection_name)

        collection.bootstrap
        collection.set_schema(schema)

        collection
      end

      def load(database, name)
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
    end

    def set_schema(schema)
      S3DB.backend.write_schema(@database.name, @name, schema.to_json)
    end

    def bootstrap
      S3DB.backend.bootstrap(@database.name, @name)
    end

    def valid?
      @schema
    end
  end
end
