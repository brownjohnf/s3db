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

      it 'sets empty errors' do
        expect(S3DB::FileBackend.new(TEST_DB_BASE_PATH).errors).to eq []
      end

      it 'returns a new Database' do
        expect(S3DB::FileBackend.new(TEST_DB_BASE_PATH)).to be_a S3DB::FileBackend
      end
    end

    describe '::create' do
      it 'builds a FileBackend and calls save' do
        expect_any_instance_of(S3DB::FileBackend).to receive(:save).and_call_original

        S3DB::FileBackend.create(TEST_DB_BASE_PATH)
      end
    end

    describe '::create!' do
      it 'builds a FileBackend and calls save' do
        expect_any_instance_of(S3DB::FileBackend).to receive(:save!)

        S3DB::FileBackend.create!(TEST_DB_BASE_PATH)
      end
    end

    describe '::destroy' do
      it 'builds a backend and calls destroy' do
        expect_any_instance_of(S3DB::FileBackend).to receive(:destroy)

        S3DB::FileBackend.destroy(TEST_DB_BASE_PATH)
      end
    end
  end

  context 'instance' do
    subject do
      S3DB::FileBackend.new(TEST_DB_BASE_PATH)
    end

    describe '#validate_path' do
      before :each do
        subject.instance_variable_set(:@errors, [])
      end

      it 'accepts good paths' do
        %w{/tmp /tmp/test tmp}.each do |path|
          subject.instance_variable_set(:@path, path)

          subject.validate_path

          expect(subject.errors).to eq []
        end
      end

      it 'rejects blacklist paths' do
        %w{/badpath badpath}.each do |path|
          subject.instance_variable_set(:@path, path)
          subject.instance_variable_set(:@errors, [])

          subject.validate_path

          expect(subject.errors.length).to eq 1
          expect(subject.errors.join('')).to match(/insane.*base.*path/)
        end
      end

      it 'rejects malformed paths' do
        ['mal\formed', 'bad path'].each do |path|
          subject.instance_variable_set(:@path, path)
          subject.instance_variable_set(:@errors, [])

          subject.validate_path

          expect(subject.errors.length).to eq 1
          expect(subject.errors.join('')).to match(/path does not match/)
        end
      end
    end

    describe '#valid?' do
      it 'returns true with no errors' do
        expect(subject).to receive(:validate_path)

        expect(subject.valid?).to be true
      end

      it 'returns false with errors' do
        subject.instance_variable_set(:@path, ['badpath'])

        expect(subject.valid?).to be false
      end
    end

    describe '#valid!' do
      it 'raises an error when valid? = false' do
        allow(subject).to receive(:valid?).and_return(false)

        expect do
          subject.valid!
        end.to raise_error ArgumentError, ''
      end

      it 'does not raise an error when valid? = true' do
        allow(subject).to receive(:valid?).and_return(true)

        expect do
          subject.valid!
        end.to_not raise_error
      end
    end

    describe '#save' do
      it 'validates the path' do
        expect(subject).to receive(:valid?).and_call_original

        subject.save
      end

      it 'writes the directory, if it does not exist' do
        S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be false

        subject.save
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be true
      end

      it 'does not raise an error if the dir does exist' do
        S3DB::FileBackend.create(TEST_DB_BASE_PATH)
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be true

        expect do
          subject.save
        end.to_not raise_error
      end
    end

    describe '#save!' do
      it 'validates the path' do
        S3DB::FileBackend.destroy(TEST_DB_BASE_PATH)

        expect(subject).to receive(:valid?).and_call_original

        subject.save!
      end

      it 'writes the path, if it does not exist' do
        S3DB::FileBackend.destroy(TEST_DB_BASE_PATH)
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be false

        subject.save!
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be true
      end

      it 'raises an error if the path exists' do
        S3DB::FileBackend.create(TEST_DB_BASE_PATH)
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be true

        expect do
          subject.save!
        end.to raise_error 'base path exists!'
      end
    end

    describe '#destroy' do
      it 'returns false if invalid' do
        allow(subject).to receive(:valid?).and_return false

        expect(subject.destroy).to be false
      end

      it 'returns false if the directory is not empty' do
        subject.save
        Dir.mkdir(File.join(TEST_DB_BASE_PATH, 'randomdir'))
        expect(subject.destroy).to be false
        S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
        expect(subject.destroy).to be false
      end

      it 'removes the dir and returns itself if valid' do
        allow(subject).to receive(:valid?).and_return true

        subject.save
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be true
        expect(subject.destroy).to be subject
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be false
      end
    end

    describe '#destroy!' do
      it 'raises an error if not empty' do
        subject.save
        Dir.mkdir(File.join(TEST_DB_BASE_PATH, 'randomdir'))
        allow(subject).to receive(:valid!)

        expect do
          subject.destroy!
        end.to raise_error ArgumentError, 'basepath not empty!'
        S3DB::FileBackend.delete(TEST_DB_BASE_PATH)
        expect do
          subject.destroy!
        end.to raise_error ArgumentError, 'basepath does not exist!'
      end

      it 'removes the dir and returns itself if empty' do
        allow(subject).to receive(:valid!).and_return true

        subject.save
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be true
        expect(subject.destroy!).to be subject
        expect(Dir.exist?(TEST_DB_BASE_PATH)).to be false
      end
    end

    describe '#db_path' do
      it 'builds a path from the base path and db_name' do
        expect(subject.db_path('test')).to eq "#{TEST_DB_BASE_PATH}/test"
      end
    end

    describe '#collection_path' do
      it 'builds a path from the base path, db_name and collection name' do
        expect(subject.collection_path('test', 'coll')).to eq "#{TEST_DB_BASE_PATH}/test/coll"
      end
    end

    describe '#schema_path' do
      it 'builds a path from the base path, db_name and coll name' do
        expect(subject.schema_path('test', 'coll')).to \
          eq "#{TEST_DB_BASE_PATH}/test/coll/schema.json"
      end
    end

    describe '#data_path' do
      it 'builds a path from the base path, db_name and collection_name' do
        expect(subject.data_path('test', 'coll')).to \
          eq "#{TEST_DB_BASE_PATH}/test/coll/data"
      end
    end

    describe '#record_path' do
      it 'builds a path from the base path, db name, coll name and filename' do
        expect(subject.record_path('test', 'coll', 'file.json')).to \
          eq "#{TEST_DB_BASE_PATH}/test/coll/data/file.json"
      end
    end
  end
end
