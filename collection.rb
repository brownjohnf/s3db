module S3DB
  class Collection
    attr_reader :database, :name, :schema

    SCHEMA = {}
    DATABASE = nil

    class << self
      attr_accessor :_database
      attr_accessor :_schema
      attr_accessor :_collection
      attr_accessor :_id_generator
      attr_accessor :_id_field
    end

    def self.database(db)
      self._database = db
    end

    def self.schema(schema)
      self._schema = schema.merge('id' => 'String')
    end

    def self.collection(collection)
      self._collection = Utils.sanitize(collection)
    end

    def self.id_generator(proc)
      self._id_generator = proc
    end

    def self.id_field(field)
      self._id_field = field
    end

    # this actually writes the files necessary to have a functional db/coll
    def self.write
      raise ArgumentError, 'missing database' if self._database.nil?
      raise ArgumentError, 'missing schema' if self._schema.nil?
      raise ArgumentError, 'missing collection name' if self._collection.nil?

      S3DB.backend.write_collection(_database.name, _collection)
      S3DB.backend.write_schema(_database.name, _collection, _schema.to_json)
    end

    def self.all
      S3DB.backend.list_records(_database.name, _collection).map do |f|
        JSON.parse(f)
      end
    end

    def self.create(data)
      new(data).save
    end

    def initialize(data)
      @data = data
    end

    def save
      set_id

      raise ArgumentError, 'data does not match schema' unless _valid?

      S3DB.backend.save_record(self.class._database.name, self.class._collection, @data['id'], @data.to_json)
    end

    def set_id
      if @data['id']
        @id = @data['id']
      else
        @id = self.class._id_generator.call(@data[self.class._id_field]) rescue UUIDTools::UUID.random_create.to_s

        @data['id'] = @id
      end
    end

    def _valid?
      validate

      raise ArgumentError, 'missing schema' unless self.class._schema.is_a?(Hash)

      puts @data.keys.inspect
      puts self.class._schema.keys.inspect
      return false unless @data.keys.map(&:to_s).sort == self.class._schema.keys.map(&:to_s).sort

      @data.each_pair do |key, value|
        puts value.class.to_s
        puts self.class._schema[key.to_s]
        return false unless value.class.to_s == self.class._schema[key.to_s]
      end
    end
    def validate; end

    def exists!
      @database.show_collections.include?(@name)
    end

    def find(id)
      res = S3DB.backend.read_record(self.class._database.name, self.class._collection, id)

      JSON.parse(res)
    end
  end
end
