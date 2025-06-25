# frozen_string_literal: true

require_relative 'lib/delta_sharing/version'

Gem::Specification.new do |spec|
  spec.name = 'delta_sharing'
  spec.version = DeltaSharing::VERSION
  spec.authors = ['Samuel Souza']
  spec.email = ['samuel.ssouza95@gmail.com']

  spec.summary = 'Ruby client for Delta Sharing protocol'
  spec.description = 'A Ruby implementation of the Delta Sharing client for reading shared Delta Lake tables'
  spec.homepage = 'https://github.com/samssouza/delta-sharing-ruby'
  spec.license = 'MIT'
  spec.required_ruby_version = '>= 2.6'

  spec.metadata['allowed_push_host'] = 'https://rubygems.org'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = 'https://github.com/samssouza/delta-sharing-ruby'
  spec.metadata['changelog_uri'] = 'https://github.com/samssouza/delta-sharing-ruby/blob/main/CHANGELOG.md'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      (f == __FILE__) || f.match(%r{\A(?:(?:test|spec|features)/|\.(?:git|travis|circleci)|appveyor)})
    end
  end
  spec.bindir = 'bin'
  spec.executables = spec.files.grep(%r{\Abin/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  # Dependencies for Delta Sharing
  spec.add_dependency 'httparty', '~> 0.21.0'
  spec.add_dependency 'red-arrow', '~> 19.0'
  spec.add_dependency 'red-arrow-dataset', '~> 19.0'
  spec.add_dependency 'red-parquet', '~> 19.0'
  spec.add_development_dependency 'minitest', '~> 5.0'
  spec.add_development_dependency 'webmock', '~> 3.18'
end
