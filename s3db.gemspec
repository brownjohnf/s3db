# -*- encoding: utf-8 -*-

files = ['./s3db.gemspec']
files << './lib/s3db.rb'

# Gemfile
files << 'Gemfile'
files << 'Gemfile.lock'


Gem::Specification.new do |gem|
  gem.name          = "s3db"
  gem.version       = '0.0.0'
  gem.authors       = ["Jack Brown"]
  gem.email         = ["jack@brownjohnf.com"]
  gem.description   = "S3-backed storage engine"
  gem.summary       = "Leverage the endless cheap capacity of s3 for high-latency storage"
  gem.homepage      = "https://github.com/brownjohnf/s3db"

  gem.files         = files
  # Library files
  gem.files         << Dir.glob(File.join('lib', '**', '*.rb'))
  # Spec files
  gem.files         << Dir.glob(File.join('spec', '**', '*.rb'))
  # Examples
  gem.files         << Dir.glob(File.join('examples', '**', '*.rb'))
  gem.executables   << 's3db'
  gem.test_files    = gem.files.grep(%r{^(spec)/})
  gem.require_paths = ["lib"]

  gem.add_dependency 'uuidtools', '~> 2.1', '>= 2.1.5'
  gem.add_dependency 'json', '~> 1.8', '>= 1.8.3'

  gem.add_development_dependency 'guard-rspec', '~> 4.6', '>= 4.6.4'
  gem.add_development_dependency 'rspec',       '~> 3.3', '>= 3.3.0'
  gem.add_development_dependency 'simplecov',   '~> 0.10', '>= 0.10.0'

  gem.license = "MIT"
end

