# frozen_string_literal: true

require_relative 'delta_sharing/version'
require_relative 'delta_sharing/errors'
require_relative 'delta_sharing/client'
require_relative 'delta_sharing/reader'
require_relative 'delta_sharing/schema'
require 'arrow'
require 'parquet'
require 'httparty'
require 'tempfile'
require 'json'

module DeltaSharing
end
