module S3DB
  class Database
    attr_reader :name

    class << self
      # Create a new database.
      #
      # db_name   - String name of the database to create. Required.
      #
      # Returns a new S3DB::Database on success, raises an error on failure.
      def create(db_name)
        begin
          S3DB.backend.write_db(db_name)
        rescue Errno::EEXIST
          raise ArgumentError, 'database exists!'

          exit 1
        end

        new(db_name)
      end

      # Drop a database.
      #
      # db_name   - String name of database to drop. Required.
      #
      # Returns the String database name on success, raises an error on failure.
      def drop(db_name)
        S3DB.backend.delete_db(db_name)
      end
    end

    # Create a new DB instance.
    #
    # db_name   - String name of database. Will be used in the storage path,
    #             so make sure it's something sane. Required.
    #
    # Returns a new Database instance.1
    def initialize(db_name)
      @name = db_name

      raise ArgumentError, 'db does not exist' unless valid?
    end

    # List all available collections in the database.
    #
    # Returns sorted Array of Strings.
    def show_collections
      S3DB.backend.list_collections(@name).sort
    end

    # Create a collection under this database.
    #
    # collection  - String name of collection to create. Will also be used as
    #               its filesystem path, so use something sane. Required.
    # schema      - Hash schema for this collection. Default: {}.
    #
    # Returns true on success.
    def create_collection(collection, schema = {})
      Collection.database self
      Collection.schema schema
      Collection.collection collection

      Collection.write
    end

    # Drop a collection from this database.
    #
    # collection  - String name of collection to drop.
    #
    # Returns the name of the collection on success.
    def drop_collection(collection)
      S3DB.backend.delete_collection(@name, collection)
    end

    # Locate the storage location for the database.
    #
    # Returns a String path of the database path. Will vary by backend adapter.
    def location
      S3DB.backend.storage_location(@name)
    end

    private

    # Check to ensure that the database name is valid, from the perspective of
    # the storage engine.
    #
    # Returns a Bool.
    def valid?
      S3DB.backend.valid_db?(@name)
    end
  end
end
