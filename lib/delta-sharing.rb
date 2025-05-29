# frozen_string_literal: true

require_relative 'delta-sharing/version'
require_relative 'delta-sharing/errors'
require_relative 'delta-sharing/client'
require_relative 'delta-sharing/reader'
require_relative 'delta-sharing/schema'
require 'arrow'
require 'parquet'
require 'httparty'
require 'tempfile'
require 'json'

module DeltaSharing
end
