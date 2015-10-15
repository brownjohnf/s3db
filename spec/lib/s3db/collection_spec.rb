require 'spec_helper'

RSpec.describe S3DB::Collection do
  before :each do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
    @backend = S3DB::FileBackend.create(TEST_DB_BASE_PATH)
    @database = S3DB::Database.create(@backend, 'testdb')
  end

  after :each do
    S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
  end

  describe '::create' do
    it 'calls #initialize and #save' do
      expect(S3DB::Collection).to \
        receive(:new).with(@database, 'testcoll').and_call_original
      expect_any_instance_of(S3DB::Collection).to receive(:save)
      expect(S3DB::Collection.create(@database, 'testcoll')).to \
        be_a(S3DB::Collection)
    end
  end

  describe 'instance methods' do
    let(:coll_name) { 'testcoll' }
    let(:schema) do
      {
        'id' => 'String'
      }
    end

    subject do
      S3DB::Collection.new(@database, coll_name)
    end

    it 'responds to accessors/readers' do
      expect(subject).to respond_to(:name)
      expect(subject).to respond_to(:database)
    end

    describe '#initialize' do
      it 'sets @database' do
        expect(subject.instance_variable_get(:@database)).to eq(@database)
        expect(subject.database).to eq(@database)
      end

      it 'sets @name' do
        expect(subject.instance_variable_get(:@name)).to eq(coll_name)
        expect(subject.name).to eq(coll_name)
      end

      it 'accepts a block and yields itself' do
        subject

        S3DB::Collection.new(@database, coll_name) do |collection|
          expect(collection.class).to eq S3DB::Collection
        end
      end

      it 'accepts an optional block' do
        expect(S3DB::Collection.new(@database, coll_name)).to \
          be_a(S3DB::Collection)
      end

      it 'validates itself' do
        expect_any_instance_of(S3DB::Collection).to receive(:validate!)

        S3DB::Collection.new(@database, coll_name)
      end
    end

    describe '#validate!' do
      it 'checks the db and raises an error' do
        expect do
          S3DB::Collection.new('database', coll_name)
        end.to raise_error ArgumentError, 'database must be an S3DB::Database!'

        expect do
          S3DB::Collection.new(@database, coll_name)
        end.to_not raise_error
      end

      it 'checks the name and raises an error' do
        subject.instance_variable_set(:@name, 1)
        expect do
          subject.validate!
        end.to raise_error ArgumentError, 'name must be a String!'
      end
    end
  end
end
