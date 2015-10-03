require 'spec_helper'

RSpec.describe S3DB::FileBackend do
  before :all do
    S3DB::FileBackend.new(TEST_DB_BASE_PATH).delete_db('testdb')
  end

  after :all do
    S3DB::FileBackend.new(TEST_DB_BASE_PATH).delete_db('testdb')
  end

  context 'class' do
    describe '::initialize' do
      it 'sets the base path' do
        expect(S3DB::FileBackend.new(TEST_DB_BASE_PATH).path).to eq TEST_DB_BASE_PATH
      end

      it 'returns a new Database' do
        expect(S3DB::FileBackend.new(TEST_DB_BASE_PATH)).to be_a S3DB::FileBackend
      end
    end

    describe '::create' do
      it 'writes the directory, if it does not exist' do
        S3DB::FileBackend.destroy(TEST_DB_BASE_PATH)
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be false

        S3DB::FileBackend.create(TEST_DB_BASE_PATH)
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be true
      end

      it 'does not raise an error if the dir does exist' do
        S3DB::FileBackend.create(TEST_DB_BASE_PATH)
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be true

        expect do
          S3DB::FileBackend.create(TEST_DB_BASE_PATH)
        end.to_not raise_error
      end

      it 'refuses to use any system dir'
    end
  end

  context 'instance' do
    subject do
      S3DB::FileBackend.new(TEST_DB_BASE_PATH)
    end
  end
end
