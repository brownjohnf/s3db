module S3DB
  class Database
    attr_reader :backend, :name

    class << self
      # Create a new database.
      #
      # backend   - S3DB::Backend subclass. Required.
      # db_name   - String name of the database to create. Required.
      #
      # returns a new S3DB::Database on success, raises an error on failure.
      def create(backend, db_name)
        begin
          backend.write_db(db_name)
        rescue Errno::EEXIST
          raise ArgumentError, 'database exists!'
        end

        new(backend, db_name)
      end

      # Drop a database.
      #
      # backend   - S3DB::Backend subclass. Required.
      # db_name   - String name of database to drop. Required.
      #
      # returns the String database name on success, raises an error on failure.
      def drop(backend, db_name)
        backend.delete_db(db_name)
      end
    end

    # Create a new DB instance.
    #
    # backend   - S3DB::Backend subclass. Required.
    # db_name   - String name of database. Will be used in the storage path,
    #             so make sure it's something sane. Required.
    #
    # returns a new Database instance.
    def initialize(backend, db_name)
      @backend = backend
      @name = db_name

      yield self if block_given?
    end

    # Save a DB instance to disk.
    #
    # Returns a Database instance.
    def save
      @backend.write_db(@name)

      self
    end

    # List all available collections in the database.
    #
    # returns sorted Array of Strings.
    def show_collections
      @backend.list_collections(@name).sort
    end

    # Create a collection under this database.
    #
    # collection  - String name of collection to create. Will also be used as
    #               its filesystem path, so use something sane. Required.
    # schema      - Hash schema for this collection. Default: {}.
    #
    # returns true on success.
    def create_collection(collection, schema = {})
      Collection.create(self, collection)
    end

    # Drop a collection from this database.
    #
    # collection  - String name of collection to drop.
    #
    # returns the name of the collection on success.
    def drop_collection(collection)
      @backend.delete_collection(@name, collection)
    end

    # Locate the storage location for the database.
    #
    # returns a String path of the database path. Will vary by backend adapter.
    def path
      @backend.db_path(@name)
    end

    private

    # Check to ensure that the database name is valid, from the perspective of
    # the storage engine.
    #
    # returns a Bool.
    def valid?
      @backend.db_exist?(@name)
    end
  end
end
