require 'spec_helper'

RSpec.describe S3DB::Record do
  before :each do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
    @backend = S3DB::FileBackend.create(TEST_DB_BASE_PATH)
    @database = S3DB::Database.create(@backend, 'testdb')
    @collection = S3DB::Collection.create(@database, 'testcoll')
  end

  after :each do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
  end

  class TestRecord
    include S3DB::Record
  end

  describe S3DB::Record::ClassMethods do
    before :each do
      TestRecord.id_generator nil
      TestRecord.id_field nil
      TestRecord._database = nil
      TestRecord._collection = nil
    end

    it 'responds to setters' do
      expect(TestRecord).to respond_to(:database)
      expect(TestRecord).to respond_to(:collection_name)
      expect(TestRecord).to respond_to(:id_generator)
      expect(TestRecord).to respond_to(:id_field)
      expect(TestRecord).to respond_to(:string)
      expect(TestRecord).to respond_to(:all)
      expect(TestRecord).to respond_to(:find)
      expect(TestRecord).to respond_to(:create)
    end

    describe '::database' do
      it 'sets @_database' do
        expect(TestRecord._database).to be_nil
        expect(TestRecord.database(:db)).to eq :db
        expect(TestRecord._database).to eq :db
      end
    end

    describe '::collection_name' do
      it 'sets @_collection to a new collection with the name' do
        expect(TestRecord._collection).to be_nil
        TestRecord.database(@database)
        expect(TestRecord.collection_name(@collection.name)).to \
          be_a(S3DB::Collection)
        expect(TestRecord._collection).to be_a(S3DB::Collection)
        expect(TestRecord._collection.name).to eq @collection.name
      end
    end

    describe '::id_generator' do
      it 'sets @_id_generator' do
        proc = Proc.new {}
        expect(TestRecord._id_generator).to be_nil
        expect(TestRecord.id_generator(proc)).to eq proc
        expect(TestRecord._id_generator).to eq proc
      end
    end

    describe '::id_field' do
      it 'sets @_database' do
        TestRecord._id_field = nil
        expect(TestRecord._id_field).to be_nil
        expect(TestRecord.id_field(:id)).to eq 'id'
        expect(TestRecord._id_field).to eq 'id'
      end
    end

    describe '::string' do
      it 'adds a key to the schema with String value' do
        expect(TestRecord._schema).to be_nil
        expect(TestRecord.string(:name)).to eq({'name' => 'String'})
        expect(TestRecord._schema).to eq({'name' => 'String'})
        expect(TestRecord.string(:word)).to eq({'name' => 'String', 'word' => 'String'})
        expect(TestRecord._schema).to eq({'name' => 'String', 'word' => 'String'})
      end
    end

    describe '::all' do
      it 'lists all available records' do
        TestRecord.database @database
        TestRecord.collection_name @collection.name

        expect(TestRecord._collection).to receive(:list_records) {[]}
        TestRecord.all
      end
    end

    describe '::find' do
      let(:filename) { 'testfile' }

      it 'looks up a record by filename' do
        TestRecord.database @database
        TestRecord.collection_name @collection.name

        expect(TestRecord._database.backend).to \
          receive(:read_record) \
          .with(TestRecord._database.name, TestRecord._collection.name, filename + '.json') \
          .and_return('{}')
        TestRecord.find(filename)
      end

      it 'raises an error with a missing file' do
        TestRecord.database @database
        TestRecord.collection_name @collection.name

        expect(TestRecord._database.backend).to \
          receive(:read_record).with(
            TestRecord._database.name,
            TestRecord._collection.name,
            filename + '.json'
          )

        expect do
          TestRecord.find(filename)
        end.to raise_error ArgumentError, 'missing record!'
      end
    end

    describe 'create' do
      before :each do
        TestRecord.database @database
        TestRecord.collection_name @collection.name
      end

      let(:data) do
        Hash.new
      end

      it 'instantiates a record' do
        expect(TestRecord).to receive(:new).with(data).and_call_original
        allow_any_instance_of(TestRecord).to receive(:save)

        TestRecord.create(data)
      end

      it 'saves a record' do
        expect_any_instance_of(TestRecord).to receive(:save)
        allow(TestRecord).to receive(:new).and_call_original

        TestRecord.create(data)
      end
    end
  end

  describe 'instance methods' do
    subject do
      TestRecord.id_field nil
      TestRecord.id_generator nil
      TestRecord.database @database
      TestRecord.collection_name @collection.name
      TestRecord.new({})
    end

    describe '#initialize' do
      it 'sets data' do
        expect(TestRecord.new({}).instance_variable_get(:@data)).to eq({})
      end
    end

    describe '#new_record?' do
      it 'returns true when the record has no _id' do
        subject.instance_variable_set(:@data, {})
        expect(subject.new_record?).to eq true
      end

      it 'returns false when the record has an _id' do
        subject.instance_variable_set(:@data, {'_id' => 'present'})
        expect(subject.new_record?).to eq false
      end
    end

    describe '#save' do
      it 'sets the id' do
        expect(subject).to receive(:_set_id)

        subject.save
      end

      it 'writes to disk' do
        subject.save
        expect(subject.class._database.backend).to \
          receive(:write_record).with(
            subject.class._database.name,
            subject.class._collection.name,
            subject.__send__(:_filename),
            subject.data.to_json,
          )

        subject.save
      end
    end

    describe '#save!' do
      it 'raises an error on save failure' do
        expect(subject).to receive(:save).and_return(false)

        expect { subject.save! }.to raise_error ArgumentError, 'failed to save!'
      end
    end

    describe '#update' do
      it 'updates the data' do
        subject.save
        expect(subject.data).to eq({'_id' => subject._id})
        expect(subject.update({'name' => 'jack'})).to eq subject
        expect(subject.data).to eq({'name' => 'jack', '_id' => subject._id})
      end
    end

    describe '#_id' do
      it 'returns the _id key from @data' do
        expect(subject._id).to be_nil

        subject.instance_variable_set(:@data, {'_id' => 'myid' })
        expect(subject._id).to eq('myid')
      end
    end

    describe '#_id=' do
      it 'sets the _id key in @data' do
        subject.instance_variable_set(:@data, {})
        expect(subject._id=('otherid')).to eq('otherid')
        expect(subject.instance_variable_get(:@data)).to eq({'_id' => 'otherid'})
      end
    end

    describe 'private #_filename' do
      it 'appends .json to the _id' do
        subject._id = 'fileid'
        expect(subject.__send__(:_filename)).to eq('fileid.json')
      end
    end

    describe 'private #_set_id' do
      it 'returns the _id if set' do
        subject._id = 'newid'
        TestRecord._id_generator = Proc.new {}
        TestRecord._id_field = nil
        expect(subject.class._id_generator).to_not receive(:call)
        expect(UUIDTools::UUID).to_not receive(:random_create)
        expect(subject.__send__(:_set_id)).to eq('newid')
        expect(subject._id).to eq('newid')
      end

      it 'sets a uuid if _id is not set and _id_generator is nil' do
        subject._id = nil
        TestRecord._id_generator = nil
        TestRecord._id_field = 'id'
        expect(UUIDTools::UUID).to receive(:random_create).and_call_original

        expect(subject.__send__(:_set_id)).to match(/\w+\-\w+\-\w+-\w+-\w+/)
      end

      it 'sets a uuid if _id is not set and _id_field is nil' do
        subject._id = nil
        TestRecord._id_generator = Proc.new { |n| 'proc' }
        TestRecord._id_field = nil
        expect(UUIDTools::UUID).to receive(:random_create).and_call_original

        expect(subject.__send__(:_set_id)).to match(/\w+\-\w+\-\w+-\w+-\w+/)
      end

      it 'calls the proc if _id is not set but _id_field and _id_generator are' do
        subject
        TestRecord.id_generator Proc.new { |n| "#{n}proc" }
        TestRecord.id_field 'name'
        subject._id = nil
        subject.instance_variable_set(:@data, {'name' => 'jack' })

        expect(subject.__send__(:_set_id)).to eq('jackproc')
      end
    end

    describe 'private #_valid' do
      it 'checks the schema'
    end
  end
end

