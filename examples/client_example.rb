#!/usr/bin/env ruby

require 'delta-sharing'

# Initialize client with profile file
# The profile file should contain:
# {
#   "shareCredentialsVersion": 1,
#   "endpoint": "https://your-delta-sharing-server/delta-sharing/",
#   "bearerToken": "your-bearer-token"
# }

client = DeltaSharing::Client.new('config.share')

shares = client.list_shares
puts "Shares: #{shares}"

share_name = shares.first[:name]

schemas = client.list_schemas(share_name)
puts "Schemas in share #{share_name}: #{schemas}"

schema_name = schemas.first[:name]

tables = client.list_tables(share_name, schema_name)
puts "Tables in share #{share_name} and schema #{schema_name}: #{tables}"

table_name = tables.first[:name]

# Get table metadata
metadata = client.get_table_metadata(share_name, schema_name, table_name)
puts "Table #{table_name} metadata: #{metadata}"

# Get table version
version = client.get_table_version(share_name, schema_name, table_name)
puts "Table #{table_name} current version: #{version}"

# Read table data
raw_response = client.read_table_data(share_name, schema_name, table_name, limit: 2)
puts "Table #{table_name} response lines: #{raw_response.length}"
raw_response.each_with_index do |line, i|
  puts "  Line #{i}: #{line[0..100]}#{line.length > 100 ? '...' : ''}"
end
