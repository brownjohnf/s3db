module S3DB
  module Record
    attr_accessor :data

    module ClassMethods
      attr_accessor :_database, :_collection, :_schema, :_id_generator, :_id_field

      # Set the database to use for the record.
      #
      # database  - S3DB::Database instance. Required.
      #
      # Returns the database instance.
      def database(database)
        self._database = database
      end

      # Set the collection to a new collection with the passed name.
      #
      # name  - String name of the collection. Required.
      #
      # returns the newly created S3DB::Collection.
      def collection_name(name)
        self._collection = S3DB::Collection.create(_database, name)
      end

      # Set the id generator for new records.
      #
      # proc  - Proc to call, returning the id field. Required.
      #
      # returns the Proc instance.
      def id_generator(proc)
        self._id_generator = proc
      end

      # Set the field to use for the id. It will be passed to the id_generator
      # proc.
      #
      # id_field  - String name of field. Required.
      #
      # returns the id_field.
      def id_field(key)
        self._id_field = key.to_s
      end

      # Set a field on the record to validate as a string.
      #
      # key   - String name of key. Required.
      #
      # returns the schema.
      def string(key)
        schema = instance_variable_get(:@_schema) || {}
        schema[key.to_s] = 'String'
        instance_variable_set(:@_schema, schema)
      end

      # Locate all records in the collection.
      #
      # returns the output from S3DB::Collection#list_records.
      def all
        instance_variable_get(:@_collection).list_records.map do |rec|
          new(JSON.parse(rec))
        end
      end

      # Locate a single record from the collection by filename.
      #
      # filename  - String filename to return. Required.
      #
      # returns the record on success; raises an error on failure.
      def find(filename)
        record = new(_id: filename)
        res = _database.backend.read_record(_database.name, _collection.name, record.__send__(:_filename))

        raise ArgumentError, 'missing record!' unless res

        new(JSON.parse(res))
      end

      # Create a new record, and write it to disk.
      #
      # data  - Hash data for the record.
      #
      # returns an instance of the record.
      def create(data)
        record = new(data)
        record.__send__(:_set_id)
        record.save
      end
    end

    # Instantiate a new record.
    #
    # data  - Hash of data. Required.
    #
    # returns a new instance of the record.
    def initialize(data)
      hash = {}
      data.each_pair do |k,v|
        hash[k.to_s] = v
      end

      @data = hash
    end

    def new_record?
      _id.nil?
    end

    # Save an instantiated record.
    #
    # returns the record on success or failure.
    def save
      _set_id

      return false if _id.nil?

      self.class._database.backend.write_record(
        self.class._database.name,
        self.class._collection.name,
        _filename,
        @data.to_json
      )

      self
    end

    # Save an instantiated record, raising an error on failure.
    #
    # returns the record on success; raises an error on failure.
    def save!
      save || raise(ArgumentError, 'failed to save!')
    end

    # Update the data for a record and save.
    #
    # data  - Hash of data for the record. Required.
    #
    # returns #save.
    def update(data)
      return false if _id.nil?

      # Copy the existing id to the new data, if it exists.
      data.merge('_id' => _id)

      # Update the dataset
      @data = data

      save
    end

    def _id=(id)
      @data['_id'] = id
    end

    def _id
      @data['_id']
    end

    # TODO: implement a missing method method for getter/setters

    private

    def _filename
      '%s.json' % [_id]
    end

    def _set_id
      return _id if !_id.nil?

      if self.class._id_generator.nil? || self.class._id_field.nil?
        self._id = UUIDTools::UUID.random_create.to_s
      else
        self._id = self.class._id_generator.call(@data[self.class._id_field])
      end

      _id
    end

    def _valid?
      return false unless @data.keys.map(&:to_s).sort == self.class._schema.keys.map(&:to_s).sort

      @data.each_pair do |key, value|
        return false unless value.class.to_s == self.class._schema[key.to_s]
      end

      true
    end

    # When included, extend the class methods of the host class
    def self.included(host_class)
      host_class.extend(ClassMethods)
    end
  end
end

