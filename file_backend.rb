require 'fileutils'

module S3DB
  class FileBackend
    attr_reader :path, :errors

    PATH_BLACKLIST = [
      'bin',
      'boot',
      'cdrom',
      'data',
      'dev',
      'docker',
      'etc',
      'home',
      'lib',
      'lib64',
      'media',
      'mnt',
      'opt',
      'proc',
      'root',
      'run',
      'sbin',
      'srv',
      'sys',
      'usr',
      'var',
      'badpath', #this is just to be VERY sure we don't trash a real dir in tests
    ]

    class << self
      # Create a new base path for a file backend storage location. This method
      # will create the basepath if it doesn't exist, but also use an existing
      # path if it does exest.
      #
      # path  - String base path. Required.
      #
      # returns a new FileBackend.
      def create(path)
        be = new(path)
        be.save

        be
      end

      # Create a new base path for a file backend storage location. This method
      # will throw an error if the path already exists. This is safer.
      #
      # path  - String base path. Required.
      #
      # returns a new FileBackend.
      def create!(path)
        be = new(path)
        be.save!

        be
      end

      # Destroy a base path for data storage. This method will raise an error
      # if the directory is not empty.
      #
      # path  - String base path. Required.
      #
      # returns the String path that was removed.
      def destroy(path)
        be = new(path).destroy

        be
      end

      # Destroy a base path, whether or not it's empty. This is dangerous, and
      # should be used with great care.
      #
      # path  - String path to delete. Required.
      #
      # returns itself on success, and raises an error on failure.
      def delete(path)
        be = new(path)

        if be.valid!
          FileUtils.rm_rf(path)
        end

        self
      end
    end

    # Create a new FileBackend.
    #
    # path  - String path to use as the base storage location.
    #
    # returns a new FileBackend.
    def initialize(path)
      @errors = []

      @path = path.strip
    end

    # Check a path to ensure it does not violate basic sanity rules, such as
    # being a linux system path, or having weird characters in the name.
    #
    # path  - String path to check. Required.
    #
    # returns true if it checks out, false otherwise. It will raise an error
    # for dangerous exceptions.
    def validate_path
      PATH_BLACKLIST.each do |p|
        @errors << "`#{p}` is insane to use as a base path!" if @path =~ /#{p}/i
      end

      if @path !~ /^(\w|\/)+$/
        @errors << "path does not match /^(\w|\/)+$/"
      end
    end

    # Check to see whether the backend is in a valid state.
    #
    # returns Bool.
    def valid?
      @errors = []

      validate_path

      !@errors.any?
    end

    # Confirm that the backend is in a consistent state. Raises an error on
    # failure.
    #
    # returns true on success, raises an error on failure.
    def valid!
      if !valid?
        raise ArgumentError, @errors.join(', ')
      end

      true
    end

    # Save a FileBackend, which means writing its root path directory to the
    # filesystem.
    #
    # returns itself on success, returns false on failure.
    def save
      return false unless valid?

      FileUtils.mkdir_p(@path)

      self
    end

    # Save a FileBackend, which means writing its root path directory to the
    # filesystem.
    #
    # returns itself on success, raises an error on failure.
    def save!
      valid!

      begin
        Dir.mkdir(@path)
      rescue Errno::EEXIST
        raise ArgumentError, 'base path exists!'
      end

      self
    end


    # Destroy a FileBackend basepath. The directory must be empty (which means
    # you must destroy all collections/dbs in the basepath first).
    #
    # returns itself on success, false on failure.
    def destroy
      return false unless valid?

      begin
        Dir.rmdir(@path)
      rescue Errno::ENOTEMPTY, Errno::ENOENT
        return false
      end

      self
    end

    # Destroy a FileBackend basepath. The directory must be empty (which means
    # you must destroy all collections/dbs in the basepath first).
    #
    # returns itself on success, raises an error on failure.
    def destroy!
      valid!

      begin
        Dir.rmdir(@path)
      rescue Errno::ENOENT
        raise ArgumentError, 'basepath does not exist!'
      rescue Errno::ENOTEMPTY
        raise ArgumentError, 'basepath not empty!'
      end

      self
    end

    # Build a full path from the base path and database name.
    #
    # db_name   - String name of database. Required.
    #
    # returns a String path.
    def db_path(db_name)
      File.join(@path, db_name)
    end

    # Build a full path to a collection from the base path, database name and
    # collection name.
    #
    # db_name           - String name of database. Required.
    # collection_name   - String name of collection. Required.
    #
    # returns a String path.
    def collection_path(db_name, collection_name)
      File.join(@path, db_name, collection_name)
    end

    # Build a full path to a scema from the base path, database name and
    # collection name.
    #
    # db_name           - String name of database. Required.
    # collection_name   - String name of collection. Required.
    #
    # returns a String path.
    def schema_path(db_name, collection_name)
      File.join(@path, db_name, collection_name, 'schema.json')
    end

    # Build a full path to the data dir from the base path, database name and
    # collection name.
    #
    # db_name           - String name of database. Required.
    # collection_name   - String name of collection. Required.
    #
    # returns a String path.
    def data_path(db_name, collection_name)
      File.join(@path, db_name, collection_name, 'data')
    end

    # Build a full path to a record from the base path, database name,
    # collection name, and filename.
    #
    # db_name           - String name of database. Required.
    # collection_name   - String name of collection. Required.
    #
    # returns a String path.
    def record_path(db_name, collection_name, filename)
      File.join(@path, db_name, collection_name, 'data', filename)
    end

    def write_db(db_name)
      Dir.mkdir(db_path(db_name))

      db_name
    end

    def write_collection(db_name, collection_name)
      dir = collection_path(db_name, collection_name)

      unless Dir.exist?(dir)
        Dir.mkdir(dir)
      end

      dir = data_path(db_name, collection_name)

      unless Dir.exist?(dir)
        Dir.mkdir(dir)
      end

      collection_name
    end

    def write_schema(db_name, collection_name, json_schema)
      File.open(schema_path(db_name, collection_name), 'w') do |f|
        f.puts json_schema
      end

      true
    end

    def read_schema(db_name, collection_name)
      file = schema_path(db_name, collection_name)

      File.read(file)
    end

    def list_records(db_name, coll_name)
      output = []

      Dir.open(data_path(db_name, coll_name)) do |dir|
        dir.each do |file|
          output << File.read(File.join(dir.path, file)) unless File.directory?(file)
        end
      end

      output
    end

    def save_record(db_name, coll_name, filename, data)
      File.open(record_path(db_name, coll_name, filename), 'w') do |f|
        f.puts data
      end
    end

    def read_record(db_name, coll_name, filename)
      File.read(record_path(db_name, coll_name, filename))
    end

    def delete_db(db_name)
      FileUtils.rm_rf(db_path(db_name))

      db_name
    end

    def delete_collection(db_name, collection_name)
      FileUtils.rm_rf(collection_path(db_name, collection_name))

      collection_name
    end

    def valid_db?(db_name)
      Dir.exist?(db_path(db_name))
    end

    def list_collections(db_name)
      Dir.entries(db_path(db_name)).select do |dir|
        dir != '.' && dir != '..'
      end
    end

    def storage_location(db_name)
      db_path(db_name)
    end
  end
end
