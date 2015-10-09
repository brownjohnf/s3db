require 'spec_helper'

RSpec.describe S3DB::Collection do
  before :all do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
    S3DB.backend = S3DB::FileBackend.create(TEST_DB_BASE_PATH)
    @db = S3DB::Database.create('testdb')
  end

  after :all do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
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

  describe '::find' do
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

    it 'finds an existing record' do
      expect(S3DB::Collection.find(@record.filename).data).to eq(@record.data)
    end

    it 'returns a record' do
      expect(S3DB::Collection.find(@record.filename).class).to eq S3DB::Collection
    end

    it 'raises an error on a missing record' do
      expect do
        S3DB::Collection.find('rando')
      end.to raise_error ArgumentError, 'record does not exist!'
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

  describe '::initialize' do
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

    it 'sets @data' do
      record = S3DB::Collection.new(data)
      expect(record.data).to eq(data)
    end

    it 'sets @id' do
      record = S3DB::Collection.new(data)
      expect(record.instance_variable_get(:@id)).to eq(data['id'])
    end
  end

  describe 'instance methods' do
    subject do
      S3DB::Collection.database(@db)
      S3DB::Collection.collection('testcoll')
      S3DB::Collection.schema({'name' => 'String'})
      S3DB::Collection.write

      S3DB::Collection.new(data)
    end

    let(:data) do
      { 'id' => 'recordid', 'name' => 'Jack' }
    end

    it 'responds to accessors/readers' do
      expect(subject).to respond_to(:data)
    end

    describe '#save' do
      it 'sets the id and writes the record' do
        expect(S3DB.backend).to \
          receive(:write_record).with(@db.name, 'testcoll', subject.filename, subject.data.to_json)
        subject.save
      end

      it 'returns itself' do
        expect(subject.save).to eq subject
      end
    end

    describe '#update' do
      it 'sets the id and writes the record' do
        expect(subject).to receive(:set_id).and_return(true)
        expect(subject).to receive(:save)
        subject.update(data)
      end

      it 'returns nil on failure' do
        expect(subject).to receive(:set_id).and_return(nil)
        expect(subject.update(data)).to be_falsey
      end
    end

    describe '#filename' do
      it 'generates the filename from the id' do
        expect(subject.filename).to eq(subject.id + '.json')
      end
    end

    describe '#id' do
      it 'generates the id from the data hash' do
        expect(subject.id).to eq(subject.data['id'])
      end
    end

    describe '#set_id' do
      it "sets @id to data['id'] if it exists" do
        subject.instance_variable_set(:@data, data.merge('id' => 'blah'))
        expect(subject.set_id).to eq 'blah'
      end

      it 'sets @id to the generator if present' do
        S3DB::Collection.id_generator(Proc.new { |n| n })
        S3DB::Collection.id_field('name')
        subject.instance_variable_set(:@data, data.merge('id' => nil))

        expect(subject.class._id_generator).to receive(:call).with('Jack').and_call_original
        expect(subject.set_id).to eq 'Jack'
      end

      it 'sets @id to an uuid if the generator is not present' do
        S3DB::Collection.id_generator(nil)
        subject.instance_variable_set(:@data, data.merge('id' => nil))

        expect(UUIDTools::UUID).to receive(:random_create).and_return('uuid')
        expect(subject.set_id).to eq 'uuid'
      end

      it 'updates the id in the data hash' do
        subject.instance_variable_set(:@data, data.merge('id' => nil))

        expect(subject.data['id']).to be_nil
        subject.set_id
        expect(subject.data['id']).to_not be_nil
      end

      it 'returns the id' do
        expect(subject.set_id).to eq subject.id
      end
    end

    describe '#validate' do
      it 'exists' do
        expect(subject).to respond_to(:validate)
      end
    end

    describe '#_valid?' do
      it 'calls validate' do
        expect(subject).to receive(:validate)
        subject.__send__(:_valid?)
      end

      it 'returns false if the keys do not match' do
        data.delete('name')
        subject.instance_variable_set(:@data, data)
        expect(subject.__send__(:_valid?)).to be false
      end

      it 'returns false if the values are of the wrong type' do
        data['name'] = 123
        subject.instance_variable_set(:@data, data)
        expect(subject.__send__(:_valid?)).to be false
      end
    end
  end
end
