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
    end

    def initialize(db, coll, data)
      @database = db
      @collection = coll
      @data = data
    end

    def save
      raise ArgumentError, 'data does not match schema' unless valid?

      S3DB.backend.save_record(@database.name, @collection.name, @data['id'], @data.to_json)
    end

    def valid?
      return false unless @schema

      return false unless @data.keys.sort == @collection.schema.keys.sort

      @data.each_pair do |key, value|
        return false unless value.class == Object.const_get(@collection.schema[key])
      end
    end
  end
end
