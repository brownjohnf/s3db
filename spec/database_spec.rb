require 'spec_helper'

RSpec.describe S3DB::Database do
  before :all do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
    S3DB.backend = S3DB::FileBackend.create(TEST_DB_BASE_PATH)
  end

  after :all do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
  end

  describe '::create' do
    after :each do
      S3DB::Database.drop('test')
    end

    it 'writes a new db' do
      expect(S3DB.backend).to receive(:write_db).and_call_original

      S3DB::Database.create('test')
    end

    it 'returns a db' do
      expect(S3DB::Database.create('test')).to be_a(S3DB::Database)
    end

    it 'throws an error on an existing db' do
      S3DB::Database.create('test')

      expect do
        S3DB::Database.create('test')
      end.to raise_error(ArgumentError, 'database exists!')
    end
  end

  describe '::drop' do
    before :each do
      S3DB::Database.create('test')
    end

    it 'calls delete_db on the backend' do
      expect(S3DB.backend).to receive(:delete_db).and_call_original

      S3DB::Database.drop('test')
    end

    it 'does not raise an error on a non-existent db' do
      S3DB::Database.drop('test')

      expect do
        S3DB::Database.drop('test')
      end.to_not raise_error
    end
  end

  context 'instance methods' do
    before :all do
      S3DB::Database.create('test')
    end

    let(:dbname) { 'test' }

    subject do
      S3DB::Database.new('test')
    end

    describe '#initialize' do
      it 'sets the name' do
        expect(subject.instance_variable_get(:@name)).to eq(dbname)
      end
    end

    describe '#show_collections' do
      it 'calls backend.list_collections with the db name' do
        expect(S3DB.backend).to receive(:list_collections).with(dbname).and_return([])

        subject.show_collections
      end

      it 'returns an array of strings' do
        subject.create_collection('testcollection')

        expect(subject.show_collections).to eq %w{testcollection}
      end

      it 'sorts the collections' do
        array = %w{a b c}
        allow(S3DB.backend).to receive(:list_collections).and_return(array)

        expect(subject.show_collections).to eq array
      end
    end

    describe '#create_collection' do
      it 'sets the collection attributes and calls write' do
        expect(S3DB::Collection).to receive(:database).with(subject).and_call_original
        expect(S3DB::Collection).to receive(:schema).with({}).and_call_original
        expect(S3DB::Collection).to \
          receive(:collection).with('testcollection').and_call_original

        expect(S3DB::Collection).to receive(:write).and_call_original

        subject.create_collection('testcollection')
      end
    end

    describe '#drop_collection' do
      it 'calls backend.delete_collection' do
        expect(S3DB.backend).to \
          receive(:delete_collection).with(dbname, 'testcollection').and_call_original

        subject.drop_collection('testcollection')
      end
    end

    describe '#path' do
      it 'calls backend.db_path' do
        subject # needed to setup initial calls to db_path

        expect(S3DB.backend).to \
          receive(:db_path).with('test').and_call_original

        expect(subject.path).to eq TEST_DB_BASE_PATH + '/' + dbname
      end
    end

    describe 'private #valid?' do
      it 'returns true if backend.valid_db? is true' do
        allow(S3DB.backend).to receive(:db_exist?).and_return(true)

        expect(subject.__send__(:valid?)).to be true
      end
    end
  end
end
