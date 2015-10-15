module S3DB
  class Collection
    attr_reader :database, :name

    class << self
      # Create a new collection.
      #
      # database  - Database attached to collection. Required.
      # name      - String name of the collection. Required.
      #
      # returns a new Collection.
      def create(database, name)
        collection = new(database, name)
        collection.save

        collection
      end
    end

    # Instantiate a new collection, without writing it to disk.
    #
    # database  - Database attached to collection. Required.
    # name      - String name of the collection. Required.
    #
    # returns a new Collection, validated but unwritten.
    def initialize(database, name)

      # Store the database and collection name
      @database = database
      @name = Utils.sanitize(name)

      # Sanity check the database and collection name
      validate!

      # Yield self for configs, if people want to.
      yield self if block_given?
    end

    # Validate a collection to ensure that it's sane.
    #
    # returns nil on success; raises an error on failure.
    def validate!
      unless @database.is_a?(S3DB::Database)
        raise ArgumentError, 'database must be an S3DB::Database!'
      end

      unless @name.is_a?(String)
        raise ArgumentError, 'name must be a String!'
      end

      nil
    end

    def list_records
      @database.backend.list_records(@database.name, @name).map do |file|
        @database.backend.read_record(@database.name, @name, file)
      end
    end

    # Write the collection skeleton to disk.
    #
    # Returns nil.
    def save
      @database.backend.write_collection(@database.name, @name)
      @database.backend.write_schema(@database.name, @name, @schema.to_json)

      nil
    end
  end
end
