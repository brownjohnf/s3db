require 'fileutils'

module S3DB
  class FileBackend
    attr_reader :path

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
    ]

    class << self
      # Create a new base path for a file backend storage location. This method
      # will create the basepath if it doesn't exist, but also use an existing
      # path if it does exest.
      #
      # path  - String base path. Required.
      #
      # Returns a new FileBackend.
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
      # Returns a new FileBackend.
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
      # Returns the String path that was removed.
      def destroy(path)
        new(path).destroy

        path
      end

      # Check a path to ensure it does not violate basic sanity rules, such as
      # being a linux system path, or having weird characters in the name.
      #
      # path  - String path to check. Required.
      #
      # Returns true if it checks out, false otherwise. It will raise an error
      # for dangerous exceptions.
      def valid_path?(path)
        PATH_BLACKLIST.each do |p|
          raise_error ArgumentError, "#{p} is insane!" if path =~ /#{p}/i
        end

        path =~ /\w/
      end
    end

    def initialize(path)
      @path = path.strip
    end

    def save
      if !self.class.valid_path?(@path)
        return false
      end

      FileUtils.mkdir_p(@path)

      true
    end

    def save!
      if !self.class.valid_path?(path)
        raise ArgumentError, 'invalid path!'
      end

      Dir.mkdir(@path)

      true
    end

    def destroy
      Dir.rmdir(@path)
    end

    def destroy!
      FileUtils.rm_rf(@path)
    end

    def db_path(db_name)
      File.join(@path, db_name)
    end

    def collection_path(db_name, collection_name)
      File.join(@path, db_name, collection_name)
    end

    def schema_path(db_name, collection_name)
      File.join(@path, db_name, collection_name, 'schema.json')
    end

    def data_path(db_name, collection_name)
      File.join(@path, db_name, collection_name, 'data')
    end

    def record_path(db_name, coll_name, filename)
      File.join(@path, db_name, coll_name, 'data', "#{filename}.json")
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
