require 'spec_helper'

RSpec.describe S3DB::Database do
  before :each do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
    @backend = S3DB::FileBackend.create(TEST_DB_BASE_PATH)
  end

  after :each do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
  end

  describe '::create' do
    it 'writes a new db' do
      expect(@backend).to receive(:write_db).and_call_original

      S3DB::Database.create(@backend, 'test')
    end

    it 'returns a db' do
      expect(S3DB::Database.create(@backend, 'test')).to be_a(S3DB::Database)
    end
  end

  describe '::drop' do
    before :each do
      S3DB::Database.create(@backend, 'test')
    end

    it 'calls delete_db on the backend' do
      expect(@backend).to receive(:delete_db).and_call_original

      S3DB::Database.drop(@backend, 'test')
    end

    it 'does not raise an error on a non-existent db' do
      S3DB::Database.drop(@backend, 'test')

      expect do
        S3DB::Database.drop(@backend, 'test')
      end.to_not raise_error
    end
  end

  context 'instance methods' do
    let(:dbname) { 'test' }

    subject do
      S3DB::Database.create(@backend, dbname)
    end

    describe '#initialize' do
      it 'sets the name' do
        expect(subject.instance_variable_get(:@name)).to eq(dbname)
      end

      it 'sets the database' do
        expect(subject.instance_variable_get(:@database)).to eq(@database)
      end

      it 'accepts a block and yields itself' do
        subject

        S3DB::Database.new(subject.backend, subject.name) do |db|
          expect(db.class).to eq S3DB::Database
        end
      end

      it 'accepts an optional block' do
        expect(S3DB::Database.new(subject.backend, subject.name)).to \
          be_a(S3DB::Database)
      end
    end

    describe '#save' do
      it 'writes the db to disk' do
        expect(@backend).to receive(:write_db).with(subject.name)
        expect(subject.save).to eq(subject)
      end
    end

    describe '#show_collections' do
      it 'calls backend.list_collections with the db name' do
        expect(@backend).to \
          receive(:list_collections).with(dbname).and_return([])

        subject.show_collections
      end

      it 'returns an array of strings' do
        subject.create_collection('testcollection')

        expect(subject.show_collections).to eq %w{testcollection}
      end

      it 'sorts the collections' do
        array = %w{a b c}
        allow(@backend).to receive(:list_collections).and_return(array)

        expect(subject.show_collections).to eq array
      end
    end

    describe '#create_collection' do
      it 'calls collection::create' do
        expect(S3DB::Collection).to \
          receive(:create).with(subject, 'testcollection')

        subject.create_collection('testcollection')
      end
    end

    describe '#drop_collection' do
      it 'calls backend.delete_collection' do
        expect(@backend).to \
          receive(:delete_collection).with(dbname, 'testcollection').and_call_original

        subject.drop_collection('testcollection')
      end
    end

    describe '#path' do
      it 'calls backend.db_path' do
        subject # needed to setup initial calls to db_path

        expect(@backend).to \
          receive(:db_path).with('test').and_call_original

        expect(subject.path).to eq TEST_DB_BASE_PATH + '/' + dbname
      end
    end

    describe 'private #valid?' do
      it 'returns true if backend.valid_db? is true' do
        allow(@backend).to receive(:db_exist?).and_return(true)

        expect(subject.__send__(:valid?)).to be true
      end
    end
  end
end
