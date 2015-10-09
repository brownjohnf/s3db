module S3DB
  class Collection
    attr_reader :data

    class << self
      attr_accessor :_database
      attr_accessor :_schema
      attr_accessor :_collection
      attr_accessor :_id_generator
      attr_accessor :_id_field

      # TODO: set some defaults here
    end

    def self.database(db)
      self._database = db
    end

    def self.schema(sch)
      self._schema = sch.merge('id' => 'String')
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
      S3DB.backend.list_records(_database.name, _collection).map do |file|
        new(
          JSON.parse(
            S3DB.backend.read_record(_database.name, _collection, file)
          )
        )
      end
    end

    def self.find(filename)
      res = S3DB.backend.read_record(_database.name, _collection, filename)

      new(JSON.parse(res))
    end

    def self.create(data)
      record = new(data)

      record.save

      record
    end

    def initialize(data)
      @data = data

      set_id
    end

    def save
      # TODO: make this not raise an error, and add a save! method
      raise ArgumentError, 'data does not match schema' unless _valid?

      S3DB.backend.write_record(
        self.class._database.name,
        self.class._collection,
        filename,
        @data.to_json
      )

      self
    end

    def update(data)
      data['id'] = @data['id']
      @data = data

      set_id && save
    end

    def filename
      '%s.json' % [@id]
    end

    def id
      @data['id']
    end

    def set_id
      if @data['id']
        @id = @data['id']
      elsif self.class._id_generator.nil? || self.class._id_field.nil?
        @id = UUIDTools::UUID.random_create.to_s
      else
        @id = self.class._id_generator.call(@data[self.class._id_field])
      end

      @data['id'] = @id

      @id
    end

    def validate; end

    # TODO: implement an missing method method for getter/setters

    private

    def _valid?
      validate

      return false unless @data.keys.map(&:to_s).sort == self.class._schema.keys.map(&:to_s).sort

      @data.each_pair do |key, value|
        return false unless value.class.to_s == self.class._schema[key.to_s]
      end

      true
    end
  end
end
