require 'fileutils'

module S3DB
  class FileBackend
    attr_reader :path

    def initialize(path)
      @path = path
    end

    def build_db_path(db_name)
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
      Dir.mkdir(build_db_path(db_name))
    end

    def write_collection(db_name, collection_name)
      Dir.mkdir(collection_path(db_name, collection_name))
    end

    def write_schema(db_name, collection_name, json_schema)
      File.open(schema_path(db_name, collection_name), 'w') do |f|
        f.puts json_schema
      end
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

    def bootstrap(db_name, collection_name)
      Dir.mkdir(data_path(db_name, collection_name))
    end

    def delete_db(db_name)
      FileUtils.rm_rf(build_db_path(db_name))
    end

    def valid_db?(db_name)
      Dir.exist?(build_db_path(db_name))
    end

    def list_collections(db_name)
      Dir.entries(build_db_path(db_name)).select do |dir|
        dir != '.' && dir != '..'
      end
    end
  end
end
