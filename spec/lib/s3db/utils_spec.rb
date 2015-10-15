require 'spec_helper'

RSpec.describe S3DB::Utils do
  describe '::sanitize' do
    it 'only allows word characters' do
      [
        'test',
        '383',
        'HjLoo',
      ].each do |input|
        expect do
          S3DB::Utils.sanitize(input)
        end.to_not raise_error
      end

      [
        'te st',
        383,
        String,
      ].each do |input|
        expect do
          S3DB::Utils.sanitize(input)
        end.to raise_error ArgumentError, 'invalid input!'
      end
    end
  end
end
