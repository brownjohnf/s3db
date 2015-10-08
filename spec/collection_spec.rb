require 'spec_helper'

RSpec.describe S3DB::Collection do
  before :all do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
    S3DB.backend = S3DB::FileBackend.create(TEST_DB_BASE_PATH)
    @db = S3DB::Database.create('testdb')
  end

  after :all do
    S3DB::Database.drop('testdb')
    S3DB.backend.delete_db(@db.name)
  end

  describe '::database' do
    it 'sets @_database' do
      S3DB::Collection.database(@db)
      expect(S3DB::Collection._database).to eq @db
    end
  end

  describe '::schema' do
    it 'sets @_schema' do
      S3DB::Collection.schema({})
      expect(S3DB::Collection._schema).to eq({'id' => 'String'})
    end

    it 'inserts id into the schema' do
      S3DB::Collection.schema({'name' => 'String'})
      expect(S3DB::Collection._schema).to eq({'name' => 'String', 'id' => 'String'})
    end
  end

  describe '::collection' do
    it 'sets @_collection' do
      S3DB::Collection.collection('testcol')
      expect(S3DB::Collection._collection).to eq 'testcol'
    end

    it 'sanitizes the collection name' do
      expect do
        S3DB::Collection.collection('test coll')
      end.to raise_error ArgumentError, 'invalid input!'
    end
  end

  describe '::id_generator' do
    it 'sets @_id_generator' do
      proc = Proc.new { |n| 'test' }

      S3DB::Collection.id_generator(proc)
      expect(S3DB::Collection._id_generator).to eq proc
    end
  end

  describe '::id_field' do
    it 'sets @_id_field' do
      S3DB::Collection.id_field('id')
      expect(S3DB::Collection._id_field).to eq 'id'
    end
  end

  describe '::write' do
    it 'writes the collection and schema to disk' do
      S3DB::Collection.database(@db)
      S3DB::Collection.collection('testcoll')
      S3DB::Collection.schema({})

      expect(S3DB.backend).to receive(:write_collection).with('testdb', 'testcoll')
      expect(S3DB.backend).to \
        receive(:write_schema).with('testdb', 'testcoll', S3DB::Collection._schema.to_json)

      S3DB::Collection.write
    end
  end

  describe '::all' do
    before :each do
      S3DB::Collection.database(@db)
      S3DB::Collection.collection('testcoll')
      S3DB::Collection.schema({'test' => 'String'})
      S3DB::Collection.write
      @record = S3DB::Collection.create({'test' => 'true'})
    end

    after :each do
      S3DB.backend.delete_record(@db.name, 'testcoll', @record.filename)
      S3DB.backend.delete_collection(@db.name, 'testcoll')
    end

    it 'lists all records in the collection' do
      expect(S3DB.backend).to \
        receive(:list_records).with(@db.name, 'testcoll').and_call_original

      expect(S3DB::Collection.all.map(&:data)).to eq([@record.data])
    end
  end

  describe '::create' do
    before :each do
      S3DB::Collection.database(@db)
      S3DB::Collection.collection('testcoll')
      S3DB::Collection.schema({'name' => 'String'})
      S3DB::Collection.write
    end

    after :each do
      S3DB.backend.delete_collection(@db.name, 'testcoll')
    end

    let(:data) do
      { 'id' => 'recordid', 'name' => 'Jack' }
    end

    it 'calls ::initialize and ::save' do
      allow_any_instance_of(S3DB::Collection).to receive(:save)
      expect(S3DB::Collection).to receive(:new).with(data).and_call_original
      S3DB::Collection.create(data)
    end
  end
end
