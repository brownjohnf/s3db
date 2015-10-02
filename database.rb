module S3DB
  class Database
    attr_reader :name

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

        new(db_name)
      end

      def drop(db_name)
        S3DB.backend.delete_db(db_name)
      end
    end

    def initialize(db_name)
      @name = db_name

      raise ArgumentError, 'db does not exist' unless valid?
    end

    def valid?
      S3DB.backend.valid_db?(@name)
    end

    def show_collections
      S3DB.backend.list_collections(@name).sort
    end

    def create_collection(collection, schema = {})
      Collection.create(self, collection, schema)
    end

    def drop_collection(collection)
      Collection.load(self, collection).drop
    end
  end
end
