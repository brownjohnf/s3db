require 'json'

module S3DB
  class Record
    attr_reader :database, :collection

    class << self
      def create(database, collection, data)
        record = new(database, collection, data)
        # validate against schema

        record.save

        record
      end

      def find(id)
        # WIF
      end
    end

    def initialize(db, coll, data)
      @database = db
      @collection = coll
      @data = data
    end

    def save
      S3DB.backend.save_record(@database.name, @collection.name, @data['id'], @data.to_json)
    end
  end
end
